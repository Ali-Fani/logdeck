package logstore

import (
	"context"
	"database/sql"
)

// This file exposes a tiny, read-only measurement surface consumed ONLY by the
// out-of-tree stress harness (server/cmd/logstore-stress). It adds no behavior
// to the store and changes no write path: it just reads counters the pipeline
// already maintains. It exists because those counters (s.drops) and the row
// table (s.db) are unexported, and the harness must observe the REAL store, not
// a copy. Do not use these methods in production code.

// Drops reports how many live records the ingest buffer has discarded because
// the writer fell behind — the store's backpressure signal. Stress/measurement
// use only.
func (s *Store) Drops() uint64 {
	return s.drops.Load()
}

// Committed reports the cumulative number of log lines the writer has inserted.
// It never decreases when retention evicts, so it measures true ingestion,
// whereas CountLines reports only what currently survives. Stress use only.
func (s *Store) Committed() int64 {
	return s.committed.Load()
}

// CountLines reports how many log lines are currently retained in the database
// (i.e. after retention eviction), counting sealed blocks as well as the hot
// table. Stress/measurement use only.
func (s *Store) CountLines(ctx context.Context) (int64, error) {
	var hot, sealed sql.NullInt64
	if err := s.db.QueryRowContext(ctx, "SELECT count(*) FROM log_lines").Scan(&hot); err != nil {
		return 0, err
	}
	if err := s.db.QueryRowContext(ctx, "SELECT SUM(lines) FROM log_blocks").Scan(&sealed); err != nil {
		return 0, err
	}
	return hot.Int64 + sealed.Int64, nil
}
