package logstore

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
)

// A sealed block holds blockLines consecutive lines of one generation as a
// single compressed blob, which is what makes stored history cheap: the
// per-row overhead of SQLite disappears, and a thousand lines of the same
// container compress against each other rather than alone.
//
// Inside a block the fields are grouped rather than interleaved — every
// timestamp, then every level, then every message — because a run of similar
// values compresses far better than the same values scattered between
// unrelated bytes. Timestamps are stored as deltas for the same reason.
//
// The engine's timestamp prefix is lifted out of the raw line and rebuilt on
// read. The store's contract is that a stored line is byte-for-byte what the
// engine sent (see lineFromEntry), so packing verifies the rebuild: a
// timestamp that would not survive the round trip keeps its line verbatim
// instead. fixedNanoLayout, unlike time.RFC3339Nano, keeps trailing zeros in
// the fraction, which is what Docker and Podman emit.
const (
	// blockLines is how many lines seal into one block. Measured across real
	// container and application logs, 1000 sits at the knee: 250 gives up about
	// a fifth of the compression, and 4000 buys under a tenth more while
	// quadrupling how much a point read has to decompress.
	blockLines = 1000
	// fixedNanoLayout is RFC 3339 with the fraction always nine digits.
	fixedNanoLayout = "2006-01-02T15:04:05.000000000Z07:00"
	// blockFormatV1 tags the payload layout so a future format can be told
	// apart without guessing.
	blockFormatV1 = 1
	// dedupFPR is the false-positive rate of a block's line-key filter. A false
	// positive only costs one needless decompression, never a wrong answer, so
	// a loose rate is the right trade: the filter exists to keep backfill from
	// decompressing blocks that obviously cannot hold the line.
	dedupFPR = 0.02
)

// blockCodec compresses and decompresses block payloads. The zstd encoder and
// decoder are safe for concurrent use, so one codec serves the writer and every
// reader.
type blockCodec struct {
	enc *zstd.Encoder
	dec *zstd.Decoder
}

// blockWindow bounds the compressor's match window. A block holds a thousand
// lines — a few hundred KB at most — so a window this size still sees the whole
// block, while the library's default would size itself for streaming input and
// hold far more state than the store ever needs.
const blockWindow = 1 << 20 // 1 MiB

func newBlockCodec() (*blockCodec, error) {
	// Both defaults scale their internal state with GOMAXPROCS, which on a
	// many-core host reserves hundreds of megabytes for a store whose whole
	// point is running on a small VPS. Sealing happens only on the writer
	// goroutine, so one encoder is all there is to use; the decoder is bounded
	// to the database's connection pool, which is what caps concurrent readers.
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderConcurrency(1),
		zstd.WithWindowSize(blockWindow),
	)
	if err != nil {
		return nil, fmt.Errorf("create log block encoder: %w", err)
	}
	dec, err := zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(maxDBConns),
		zstd.WithDecoderMaxMemory(blockWindow*16),
	)
	if err != nil {
		enc.Close()
		return nil, fmt.Errorf("create log block decoder: %w", err)
	}
	return &blockCodec{enc: enc, dec: dec}, nil
}

func (c *blockCodec) close() {
	c.enc.Close()
	c.dec.Close()
}

// sealedLine is one line on its way into or out of a block. seq is the store's
// monotonic line number, which orders lines that share a timestamp.
type sealedLine struct {
	seq    int64
	tsNS   int64
	stream int
	level  int
	raw    string
}

// blockSummary is everything about a block that is kept in columns, so a query
// can rule a block in or out without decompressing it.
type blockSummary struct {
	tsMinNS   int64
	tsMaxNS   int64
	seqMin    int64
	seqMax    int64
	lines     int
	levelMask int64
	rawBytes  int64
}

// pack serializes and compresses one block's lines, which must be ordered by
// (tsNS, seq). It returns the payload, the dedup filter, and the summary.
func (c *blockCodec) pack(lines []sealedLine) (payload, filter []byte, summary blockSummary, err error) {
	if len(lines) == 0 {
		return nil, nil, blockSummary{}, fmt.Errorf("pack empty log block")
	}

	summary = blockSummary{
		tsMinNS: lines[0].tsNS,
		tsMaxNS: lines[len(lines)-1].tsNS,
		seqMin:  lines[0].seq,
		seqMax:  lines[len(lines)-1].seq,
		lines:   len(lines),
	}

	// bodies[i] is what is stored for line i: the message with its timestamp
	// prefix removed, or the whole line when the prefix cannot be rebuilt.
	bodies := make([]string, len(lines))
	verbatim := make([]byte, len(lines))
	for i, l := range lines {
		summary.levelMask |= 1 << uint(l.level)
		summary.rawBytes += int64(len(l.raw))
		if body, ok := stripTimestamp(l.raw, l.tsNS); ok {
			bodies[i] = body
			continue
		}
		bodies[i] = l.raw
		verbatim[i] = 1
	}

	var buf []byte
	buf = append(buf, blockFormatV1)
	buf = binary.AppendUvarint(buf, uint64(len(lines)))

	// Field runs. Deltas keep the numbers small, and small numbers next to each
	// other are what the compressor turns into almost nothing.
	prevTS, prevSeq := int64(0), int64(0)
	for _, l := range lines {
		buf = binary.AppendVarint(buf, l.tsNS-prevTS)
		prevTS = l.tsNS
	}
	for _, l := range lines {
		buf = binary.AppendVarint(buf, l.seq-prevSeq)
		prevSeq = l.seq
	}
	for _, l := range lines {
		// stream and level both fit in one byte and always travel together.
		buf = append(buf, byte(l.stream)<<4|byte(l.level&0x0f))
	}
	buf = append(buf, verbatim...)
	for _, body := range bodies {
		buf = binary.AppendUvarint(buf, uint64(len(body)))
	}
	for _, body := range bodies {
		buf = append(buf, body...)
	}

	return c.enc.EncodeAll(buf, nil), buildDedupFilter(lines), summary, nil
}

// unpack decompresses a block back into its lines, rebuilding each raw line
// exactly as the engine sent it.
func (c *blockCodec) unpack(payload []byte, scratch []byte) ([]sealedLine, []byte, error) {
	buf, err := c.dec.DecodeAll(payload, scratch[:0])
	if err != nil {
		return nil, scratch, fmt.Errorf("decompress log block: %w", err)
	}
	scratch = buf

	r := reader{buf: buf}
	format := r.byteAt()
	if format != blockFormatV1 {
		return nil, scratch, fmt.Errorf("unknown log block format %d", format)
	}
	count := int(r.uvarint())
	if count < 0 || count > blockLines*16 {
		return nil, scratch, fmt.Errorf("log block claims %d lines", count)
	}

	lines := make([]sealedLine, count)
	prevTS, prevSeq := int64(0), int64(0)
	for i := range lines {
		prevTS += r.varint()
		lines[i].tsNS = prevTS
	}
	for i := range lines {
		prevSeq += r.varint()
		lines[i].seq = prevSeq
	}
	for i := range lines {
		packed := r.byteAt()
		lines[i].stream = int(packed >> 4)
		lines[i].level = int(packed & 0x0f)
	}
	verbatim := make([]byte, count)
	for i := range verbatim {
		verbatim[i] = r.byteAt()
	}
	lengths := make([]int, count)
	for i := range lengths {
		lengths[i] = int(r.uvarint())
	}
	if r.err != nil {
		return nil, scratch, fmt.Errorf("decode log block: %w", r.err)
	}
	for i := range lines {
		body, err := r.stringOf(lengths[i])
		if err != nil {
			return nil, scratch, fmt.Errorf("decode log block: %w", err)
		}
		if verbatim[i] == 1 {
			lines[i].raw = body
			continue
		}
		lines[i].raw = time.Unix(0, lines[i].tsNS).UTC().Format(fixedNanoLayout) + " " + body
	}
	return lines, scratch, nil
}

// stripTimestamp removes the engine's timestamp prefix from raw when — and only
// when — rebuilding it from tsNS reproduces the original bytes exactly. The
// store promises that a stored line parses identically to a live one, so a
// prefix this cannot reproduce is not lifted out at all.
func stripTimestamp(raw string, tsNS int64) (string, bool) {
	space := strings.IndexByte(raw, ' ')
	if space <= 0 {
		return "", false
	}
	if time.Unix(0, tsNS).UTC().Format(fixedNanoLayout) != raw[:space] {
		return "", false
	}
	return raw[space+1:], true
}

// reader is a bounds-checked cursor over a decoded block.
type reader struct {
	buf []byte
	pos int
	err error
}

func (r *reader) byteAt() byte {
	if r.pos >= len(r.buf) {
		r.err = errTruncatedBlock
		return 0
	}
	b := r.buf[r.pos]
	r.pos++
	return b
}

func (r *reader) uvarint() uint64 {
	v, n := binary.Uvarint(r.buf[min(r.pos, len(r.buf)):])
	if n <= 0 {
		r.err = errTruncatedBlock
		return 0
	}
	r.pos += n
	return v
}

func (r *reader) varint() int64 {
	v, n := binary.Varint(r.buf[min(r.pos, len(r.buf)):])
	if n <= 0 {
		r.err = errTruncatedBlock
		return 0
	}
	r.pos += n
	return v
}

func (r *reader) stringOf(n int) (string, error) {
	if r.err != nil {
		return "", r.err
	}
	if n < 0 || r.pos+n > len(r.buf) {
		return "", errTruncatedBlock
	}
	s := string(r.buf[r.pos : r.pos+n])
	r.pos += n
	return s, nil
}

var errTruncatedBlock = fmt.Errorf("log block payload is truncated")

// ---- dedup filter ----

// A block's dedup filter answers "could this exact line already be in here?".
// Backfill re-reads windows the store has already persisted, and the insert
// path must not store those lines twice. Once a line is sealed it can no longer
// be found by an index seek, so the filter stands in for one: a negative is
// proof the line is absent, and a positive means the block is decompressed and
// checked exactly. Live ingestion never pays for this — its timestamps are
// newer than every sealed block, so no block is ever consulted.

// lineKey identifies a line the way insertLine's uniqueness check does:
// timestamp, stream, and the raw bytes.
func lineKey(tsNS int64, stream int, raw string) uint64 {
	h := fnv.New64a()
	var buf [9]byte
	binary.LittleEndian.PutUint64(buf[:8], uint64(tsNS))
	buf[8] = byte(stream)
	h.Write(buf[:])
	h.Write([]byte(raw))
	return h.Sum64()
}

// buildDedupFilter returns a bloom filter over the block's line keys, laid out
// as a big-endian bit count followed by the bits themselves.
func buildDedupFilter(lines []sealedLine) []byte {
	bits := filterBits(len(lines))
	out := make([]byte, 4+(bits+7)/8)
	binary.BigEndian.PutUint32(out[:4], uint32(bits))
	for _, l := range lines {
		for _, pos := range filterPositions(bits, lineKey(l.tsNS, l.stream, l.raw)) {
			out[4+pos/8] |= 1 << (pos % 8)
		}
	}
	return out
}

// filterMayContain reports whether the filter can rule the key out. A malformed
// or missing filter reports true, which only costs a decompression.
func filterMayContain(filter []byte, key uint64) bool {
	if len(filter) < 4 {
		return true
	}
	bits := int(binary.BigEndian.Uint32(filter[:4]))
	body := filter[4:]
	if bits <= 0 || len(body) < (bits+7)/8 {
		return true
	}
	for _, pos := range filterPositions(bits, key) {
		if body[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}

// filterPositions returns the bit positions one key occupies. The number of
// hashes depends only on the target rate, never on the member count, because a
// reader derives it without knowing how many lines went in.
func filterPositions(bits int, key uint64) []int {
	k := max(1, min(int(math.Round(math.Log(1/dedupFPR)/math.Ln2)), 12))
	h1, h2 := key, key>>32|1
	out := make([]int, k)
	for i := range out {
		out[i] = int((h1 + uint64(i)*h2) % uint64(bits))
	}
	return out
}

func filterBits(n int) int {
	if n <= 0 {
		n = 1
	}
	bits := int(math.Ceil(float64(n) * math.Log(1/dedupFPR) / (math.Ln2 * math.Ln2)))
	return max(bits, 64)
}
