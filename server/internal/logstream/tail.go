package logstream

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	"github.com/AmoabaKelvin/logdeck/internal/models"
)

// maxTailAttempts bounds how many times a tail is opened before giving up
// until the next resync or start event respawns it.
const maxTailAttempts = 5

// tail is the run loop's handle to one tail goroutine.
type tail struct {
	cancel   context.CancelFunc
	draining atomic.Bool // set by the run loop after die/destroy
}

// spawnTail starts a tail goroutine for one (subscription, container) pair.
// Runs on the loop. The Docker client is captured at spawn time: after a hot
// swap the old client's close ends the tail and resync respawns it on the
// new client.
func (h *Hub) spawnTail(sub *subscription, key containerKey, name string, labels map[string]string) {
	client := h.source()
	ctx, cancel := context.WithCancel(h.runCtx)
	t := &tail{cancel: cancel}
	sub.tails[key] = t

	h.tailWg.Add(1)
	go func() {
		defer h.tailWg.Done()
		defer cancel()
		h.runTail(ctx, client, sub, t, key, name, labels)
		// Tell the loop this tail is gone so resync can respawn it. Skipped
		// on shutdown, when the loop is no longer receiving.
		select {
		case h.tailExitCh <- tailExit{sub: sub, key: key, t: t}:
		case <-h.runCtx.Done():
		}
	}()
}

// runTail opens the log tail and keeps it open while desired, retrying with
// exponential backoff when the stream ends prematurely. Returns when ctx is
// cancelled (tail no longer desired) or after maxTailAttempts.
func (h *Hub) runTail(
	ctx context.Context,
	client engineClient,
	sub *subscription,
	t *tail,
	key containerKey,
	name string,
	labels map[string]string,
) {
	emit := func(entry models.LogEntry) {
		sub.push(Record{
			Host:          key.host,
			ContainerID:   key.id,
			ContainerName: name,
			Labels:        labels,
			Entry:         entry,
		})
	}

	delay := h.retryBaseDelay
	for attempt := 1; ; attempt++ {
		err := client.openTail(ctx, key.host, key.id, sub.opts, emit)
		if ctx.Err() != nil {
			return
		}
		// A clean EOF after die/destroy means Docker drained the followed
		// response. Without a lifecycle boundary, preserve the existing retry
		// behavior: a daemon or proxy can close an otherwise-live stream with a
		// plain EOF, and waiting for the minute resync would create a long gap.
		if err == nil && t.draining.Load() {
			return
		}
		if attempt >= maxTailAttempts {
			log.Printf("logstream: tail %s/%s gave up after %d attempts (last error: %v)", key.host, name, attempt, err)
			return
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return
		}
		delay = min(delay*2, h.retryMaxDelay)
	}
}
