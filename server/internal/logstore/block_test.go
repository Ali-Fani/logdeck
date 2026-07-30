package logstore

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/AmoabaKelvin/logdeck/internal/config"
	"github.com/AmoabaKelvin/logdeck/internal/models"
)

// dockerLine renders a line the way the engine actually hands it over: an
// RFC 3339 prefix whose fraction always carries nine digits. The suite's
// rawLine helper uses Go's RFC3339Nano instead, which trims trailing zeros, so
// the two together cover both sides of the packer's round-trip check.
func dockerLine(ts time.Time, message string) string {
	return ts.UTC().Format(fixedNanoLayout) + " " + message
}

func dockerEntry(ts time.Time, stream, message string) models.LogEntry {
	return models.ParseLogLine(dockerLine(ts, message), stream)
}

// writeAndSeal commits entries and then seals whatever full blocks they
// completed, which is exactly what writeLoop does after every batch.
func writeAndSeal(t *testing.T, s *Store, key genKey, name string, entries ...models.LogEntry) {
	t.Helper()
	state := mustWriterState(t, s)
	batch := make([]ingestMsg, 0, len(entries))
	for _, entry := range entries {
		batch = append(batch, ingestMsg{kind: msgLine, key: key, name: name, line: lineFromEntry(entry)})
	}
	if err := s.commit(batch, state); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := s.sealFullBlocks(state); err != nil {
		t.Fatalf("seal: %v", err)
	}
}

// chatty builds n entries a second apart, shaped like a real service log.
func chatty(n int, ts time.Time, render func(time.Time, string, string) models.LogEntry) []models.LogEntry {
	entries := make([]models.LogEntry, n)
	for i := range entries {
		at := ts.Add(time.Duration(i) * time.Second)
		entries[i] = render(at, "stdout",
			fmt.Sprintf("INFO  GET /v1/accounts/%d 200 in %dms", i%997, i%80+3))
	}
	return entries
}

// noisy builds n entries whose bodies barely compress, so a test can reach a
// retention cap without writing hundreds of thousands of lines. Real container
// logs compress far better than this; poorly-compressible input is the
// conservative case for retention.
func noisy(n int, ts time.Time) []models.LogEntry {
	const hex = "0123456789abcdef"
	entries := make([]models.LogEntry, n)
	state := uint64(0x9e3779b97f4a7c15)
	token := make([]byte, 96)
	for i := range entries {
		for j := range token {
			state ^= state << 13
			state ^= state >> 7
			state ^= state << 17
			token[j] = hex[state&0xf]
		}
		at := ts.Add(time.Duration(i) * time.Millisecond)
		entries[i] = dockerEntry(at, "stdout", string(token))
	}
	return entries
}

// readAll pages through a container's whole stored history, oldest first.
func readAll(t *testing.T, s *Store, host, name string) []models.LogEntry {
	t.Helper()
	var all []models.LogEntry
	cursor := ""
	for round := 0; ; round++ {
		if round > 500 {
			t.Fatal("paging did not terminate")
		}
		page, err := s.Query(context.Background(), LogQuery{
			Host: host, Container: name, Limit: 200, Cursor: cursor,
		})
		if err != nil {
			t.Fatalf("Query: %v", err)
		}
		all = append(page.Entries, all...)
		if page.NextCursor == "" {
			return all
		}
		cursor = page.NextCursor
	}
}

func countRows(t *testing.T, s *Store, statement string) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(statement).Scan(&n); err != nil {
		t.Fatalf("%s: %v", statement, err)
	}
	return n
}

// TestBlockCodecRebuildsLinesByteForByte is the codec's core promise. A stored
// line has to come back exactly as the engine sent it, including its timestamp
// prefix — the parse that turns it into an entry must not be able to drift from
// the parse the live path runs.
func TestBlockCodecRebuildsLinesByteForByte(t *testing.T) {
	codec, err := newBlockCodec()
	if err != nil {
		t.Fatalf("newBlockCodec: %v", err)
	}
	defer codec.close()

	ts := baseTime
	lines := []sealedLine{
		// A nine-digit prefix: the timestamp is lifted out and rebuilt.
		{seq: 1, tsNS: ts.UnixNano(), stream: streamStdout, level: 3, raw: dockerLine(ts, "started")},
		// A prefix with the fraction trimmed: the packer cannot reproduce it, so
		// the line has to be kept whole instead.
		{seq: 2, tsNS: ts.Add(time.Second).UnixNano(), stream: streamStderr, level: 5,
			raw: ts.Add(time.Second).UTC().Format(time.RFC3339Nano) + " ERROR boom"},
		// No prefix at all, from a driver that dropped it.
		{seq: 3, tsNS: ts.Add(2 * time.Second).UnixNano(), stream: streamStdout, level: 3,
			raw: "bare line with no timestamp"},
		// A body that itself contains spaces and looks like a timestamp.
		{seq: 4, tsNS: ts.Add(3 * time.Second).UnixNano(), stream: streamStdout, level: 3,
			raw: dockerLine(ts.Add(3*time.Second), "replayed 2026-07-01T12:00:00.000000000Z from queue")},
		{seq: 5, tsNS: ts.Add(4 * time.Second).UnixNano(), stream: streamStdout, level: 3,
			raw: dockerLine(ts.Add(4*time.Second), "")},
	}

	payload, filter, summary, err := codec.pack(lines)
	if err != nil {
		t.Fatalf("pack: %v", err)
	}
	got, _, err := codec.unpack(payload, nil)
	if err != nil {
		t.Fatalf("unpack: %v", err)
	}

	if len(got) != len(lines) {
		t.Fatalf("unpacked %d lines, want %d", len(got), len(lines))
	}
	for i := range lines {
		if got[i] != lines[i] {
			t.Errorf("line %d round-tripped as %+v, want %+v", i, got[i], lines[i])
		}
	}
	if summary.tsMinNS != lines[0].tsNS || summary.tsMaxNS != lines[len(lines)-1].tsNS {
		t.Errorf("summary spans [%d,%d], want [%d,%d]",
			summary.tsMinNS, summary.tsMaxNS, lines[0].tsNS, lines[len(lines)-1].tsNS)
	}
	if summary.lines != len(lines) {
		t.Errorf("summary counts %d lines, want %d", summary.lines, len(lines))
	}

	// Every line that went in must be reported as possibly present; that is what
	// makes the filter safe to use as a dedup pre-check.
	for _, l := range lines {
		if !filterMayContain(filter, lineKey(l.tsNS, l.stream, l.raw)) {
			t.Errorf("dedup filter rules out a line it holds: %q", l.raw)
		}
	}
}

// TestSealingPreservesEveryLineAndShrinksTheStore checks the whole point of the
// change: history comes back intact, and it costs far less disk than a row per
// line.
func TestSealingPreservesEveryLineAndShrinksTheStore(t *testing.T) {
	store := newTestStore(t)
	key := genKey{"local", "api-1"}
	entries := chatty(2500, baseTime, dockerEntry)

	writeAndSeal(t, store, key, "api", entries...)

	if blocks := countRows(t, store, "SELECT COUNT(*) FROM log_blocks"); blocks != 2 {
		t.Fatalf("sealed %d blocks, want 2 for 2500 lines", blocks)
	}
	if hot := countRows(t, store, "SELECT COUNT(*) FROM log_lines"); hot != 500 {
		t.Fatalf("%d lines left hot, want the 500 that did not fill a block", hot)
	}

	got := readAll(t, store, "local", "api")
	if len(got) != len(entries) {
		t.Fatalf("read back %d lines, want %d", len(got), len(entries))
	}
	for i := range entries {
		if got[i].Raw != entries[i].Raw {
			t.Fatalf("line %d came back as %q, want %q", i, got[i].Raw, entries[i].Raw)
		}
		if !got[i].Timestamp.Equal(entries[i].Timestamp) {
			t.Fatalf("line %d timestamp %v, want %v", i, got[i].Timestamp, entries[i].Timestamp)
		}
	}

	// The sealed bytes must be a small fraction of the text they hold. The
	// measured ratio on real logs is far better than this; the bar is set low so
	// the test fails only on a real regression, never on compressor drift.
	var rawBytes, payloadBytes int64
	if err := store.db.QueryRow(
		"SELECT SUM(raw_bytes), SUM(length(payload)) FROM log_blocks").Scan(&rawBytes, &payloadBytes); err != nil {
		t.Fatalf("measure blocks: %v", err)
	}
	if payloadBytes*4 > rawBytes {
		t.Errorf("sealed payload is %d bytes for %d bytes of log text, want at least 4x smaller",
			payloadBytes, rawBytes)
	}
}

// TestDedupSurvivesSealing is the correctness risk the block layout introduces.
// Once a line is sealed it can no longer be found by an index seek, so a
// backfill re-reading a window the store already holds must still be recognised
// as a duplicate rather than stored twice.
func TestDedupSurvivesSealing(t *testing.T) {
	store := newTestStore(t)
	key := genKey{"local", "api-1"}
	entries := chatty(1200, baseTime, dockerEntry)

	writeAndSeal(t, store, key, "api", entries...)
	if blocks := countRows(t, store, "SELECT COUNT(*) FROM log_blocks"); blocks != 1 {
		t.Fatalf("sealed %d blocks, want 1", blocks)
	}

	// The engine is re-read from the start, exactly as it is after a restart or
	// a dropped-line gap.
	writeAndSeal(t, store, key, "api", entries...)

	got := readAll(t, store, "local", "api")
	if len(got) != len(entries) {
		t.Fatalf("after a full re-read the store holds %d lines, want %d", len(got), len(entries))
	}
	for i := range entries {
		if got[i].Raw != entries[i].Raw {
			t.Fatalf("line %d is %q, want %q", i, got[i].Raw, entries[i].Raw)
		}
	}
}

// TestPagingIsStableAcrossTheSealBoundary walks the whole history one small page
// at a time. The cursor has to keep working where a page ends inside a sealed
// block and continues into the hot table, which is what the sequence number is
// for.
func TestPagingIsStableAcrossTheSealBoundary(t *testing.T) {
	store := newTestStore(t)
	key := genKey{"local", "api-1"}
	entries := chatty(2300, baseTime, dockerEntry)
	writeAndSeal(t, store, key, "api", entries...)

	var (
		seen   []string
		cursor string
	)
	for round := 0; ; round++ {
		if round > 200 {
			t.Fatal("paging did not terminate")
		}
		page, err := store.Query(context.Background(), LogQuery{
			Host: "local", Container: "api", Limit: 37, Cursor: cursor,
		})
		if err != nil {
			t.Fatalf("Query: %v", err)
		}
		if len(page.Entries) == 0 && page.NextCursor != "" {
			t.Fatal("an empty page must never carry a cursor")
		}
		messages := make([]string, len(page.Entries))
		for i, e := range page.Entries {
			messages[i] = e.Raw
		}
		seen = append(messages, seen...)
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}

	if len(seen) != len(entries) {
		t.Fatalf("paging returned %d lines, want %d", len(seen), len(entries))
	}
	for i := range entries {
		if seen[i] != entries[i].Raw {
			t.Fatalf("paged line %d is %q, want %q", i, seen[i], entries[i].Raw)
		}
	}
}

// TestFiltersReachIntoSealedBlocks confirms level and search filters see sealed
// history, not just the hot tail.
func TestFiltersReachIntoSealedBlocks(t *testing.T) {
	store := newTestStore(t)
	key := genKey{"local", "api-1"}

	entries := chatty(1500, baseTime, dockerEntry)
	// One distinctive error early enough to be sealed.
	marker := baseTime.Add(50 * time.Second)
	entries[50] = dockerEntry(marker, "stderr", "ERROR payment gateway unreachable")
	writeAndSeal(t, store, key, "api", entries...)

	if blocks := countRows(t, store, "SELECT COUNT(*) FROM log_blocks"); blocks == 0 {
		t.Fatal("nothing was sealed, so this test proves nothing")
	}

	page, err := store.Query(context.Background(), LogQuery{
		Host: "local", Container: "api", Search: "payment gateway",
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(page.Entries) != 1 {
		t.Fatalf("search found %d entries in sealed history, want 1", len(page.Entries))
	}
	if page.Entries[0].Raw != entries[50].Raw {
		t.Fatalf("search returned %q, want %q", page.Entries[0].Raw, entries[50].Raw)
	}

	levelled, err := store.Query(context.Background(), LogQuery{
		Host: "local", Container: "api", Levels: []string{"ERROR"},
	})
	if err != nil {
		t.Fatalf("Query by level: %v", err)
	}
	if len(levelled.Entries) != 1 {
		t.Fatalf("level filter found %d entries in sealed history, want 1", len(levelled.Entries))
	}
}

// TestTimeRangeQueriesReadSealedHistory covers the Since/Until path, which rules
// blocks out by their stored bounds before decompressing anything.
func TestTimeRangeQueriesReadSealedHistory(t *testing.T) {
	store := newTestStore(t)
	key := genKey{"local", "api-1"}
	entries := chatty(2000, baseTime, dockerEntry)
	writeAndSeal(t, store, key, "api", entries...)

	since := baseTime.Add(100 * time.Second)
	until := baseTime.Add(199 * time.Second)
	page, err := store.Query(context.Background(), LogQuery{
		Host: "local", Container: "api", Since: since, Until: until, Limit: MaxQueryLimit,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(page.Entries) != 100 {
		t.Fatalf("time range returned %d entries, want 100", len(page.Entries))
	}
	for _, e := range page.Entries {
		if e.Timestamp.Before(since) || e.Timestamp.After(until) {
			t.Fatalf("entry at %v falls outside [%v, %v]", e.Timestamp, since, until)
		}
	}
}

// TestRetentionEvictsSealedBlocks checks that the caps still bound the file once
// most of the history lives in blocks.
func TestRetentionEvictsSealedBlocks(t *testing.T) {
	limits := func() config.ResolvedLogStoreConfig {
		return config.ResolvedLogStoreConfig{Enabled: true, PerContainerMB: 1, TotalMB: 1}
	}
	store, err := Open(filepath.Join(t.TempDir(), "logs.db"), limits)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	key := genKey{"local", "api-1"}
	// Enough history that the sealed blocks alone exceed the 1 MB cap.
	for round := range 12 {
		start := baseTime.Add(time.Duration(round) * 4000 * time.Millisecond)
		writeAndSeal(t, store, key, "api", noisy(4000, start)...)
	}

	blocksBefore := countRows(t, store, "SELECT COUNT(*) FROM log_blocks")
	if blocksBefore == 0 {
		t.Fatal("nothing was sealed, so this test proves nothing")
	}
	var storedBefore int64
	if err := store.db.QueryRow("SELECT SUM(stored_bytes) FROM containers").Scan(&storedBefore); err != nil {
		t.Fatalf("read stored_bytes: %v", err)
	}
	if storedBefore <= bytesPerMB {
		t.Fatalf("the store holds %d bytes, under the %d byte cap: nothing would be evicted",
			storedBefore, bytesPerMB)
	}

	if err := store.retain(context.Background()); err != nil {
		t.Fatalf("retain: %v", err)
	}

	var stored int64
	if err := store.db.QueryRow("SELECT SUM(stored_bytes) FROM containers").Scan(&stored); err != nil {
		t.Fatalf("read stored_bytes: %v", err)
	}
	if stored > bytesPerMB {
		t.Fatalf("stored_bytes is %d after retention, want at most %d", stored, bytesPerMB)
	}
	if blocksAfter := countRows(t, store, "SELECT COUNT(*) FROM log_blocks"); blocksAfter >= blocksBefore {
		t.Fatalf("retention kept %d blocks of %d, want it to evict sealed history",
			blocksAfter, blocksBefore)
	}

	// What survives must still be readable, and must be the newest history.
	got := readAll(t, store, "local", "api")
	if len(got) == 0 {
		t.Fatal("retention evicted everything")
	}
	for i := 1; i < len(got); i++ {
		if got[i].Timestamp.Before(got[i-1].Timestamp) {
			t.Fatalf("surviving history is out of order at %d", i)
		}
	}
}

// TestSealedLinesSurviveAReopen covers the writer's startup state: sequence
// numbers must continue past everything already sealed, or a reopened store
// would hand two different lines the same page position.
func TestSealedLinesSurviveAReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logs.db")
	store, err := Open(path, testLimits)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	key := genKey{"local", "api-1"}
	first := chatty(1500, baseTime, dockerEntry)
	writeAndSeal(t, store, key, "api", first...)
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(path, testLimits)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	state := mustWriterState(t, reopened)
	var sealedMax int64
	if err := reopened.db.QueryRow("SELECT MAX(seq_max) FROM log_blocks").Scan(&sealedMax); err != nil {
		t.Fatalf("read sealed seq: %v", err)
	}
	if state.nextSeq <= sealedMax {
		t.Fatalf("nextSeq is %d but a sealed line already holds %d", state.nextSeq, sealedMax)
	}

	second := chatty(600, baseTime.Add(2000*time.Second), dockerEntry)
	writeAndSeal(t, reopened, key, "api", second...)

	got := readAll(t, reopened, "local", "api")
	if len(got) != len(first)+len(second) {
		t.Fatalf("after reopening the store holds %d lines, want %d", len(got), len(first)+len(second))
	}
}

// TestMigrationFromV3SealsNothingAndKeepsEveryLine checks the upgrade path: an
// existing database keeps all of its history, and its rows keep their order.
func TestMigrationFromV3SealsNothingAndKeepsEveryLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logs.db")
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatalf("open raw database: %v", err)
	}

	for _, statement := range []string{schemaV1, schemaV2, schemaV3} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("build v3 schema: %v", err)
		}
	}
	if _, err := db.Exec("PRAGMA user_version = 3"); err != nil {
		t.Fatalf("stamp version: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO containers
		(id, host, container_id, name, first_seen_ms, last_seen_ms)
		VALUES (1, 'local', 'api-1', 'api', 0, 0)`); err != nil {
		t.Fatalf("seed generation: %v", err)
	}
	want := make([]string, 0, 40)
	for i := range 40 {
		at := baseTime.Add(time.Duration(i) * time.Second)
		raw := dockerLine(at, fmt.Sprintf("line %d", i))
		want = append(want, raw)
		if _, err := db.Exec(
			"INSERT INTO log_lines (container_ref, ts_ns, stream, level, raw) VALUES (1, ?, 0, 3, ?)",
			at.UnixNano(), raw); err != nil {
			t.Fatalf("seed line: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close seeded database: %v", err)
	}

	store, err := Open(path, testLimits)
	if err != nil {
		t.Fatalf("open after migration: %v", err)
	}
	defer store.Close()

	var version int
	if err := store.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatalf("read version: %v", err)
	}
	if version != schemaVersion {
		t.Fatalf("schema version is %d, want %d", version, schemaVersion)
	}

	// Migrated rows must carry distinct sequence numbers, or paging would stall.
	distinct := countRows(t, store, "SELECT COUNT(DISTINCT seq) FROM log_lines")
	if distinct != len(want) {
		t.Fatalf("%d distinct sequence numbers across %d migrated rows", distinct, len(want))
	}

	got := readAll(t, store, "local", "api")
	if len(got) != len(want) {
		t.Fatalf("migration kept %d lines, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Raw != want[i] {
			t.Fatalf("migrated line %d is %q, want %q", i, got[i].Raw, want[i])
		}
	}
}
