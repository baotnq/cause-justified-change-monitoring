// Package pgstore persists alerts and their hash chain in PostgreSQL.
//
// Part B lists three outputs: alerts on a bus for operators, history and
// evidence in Postgres, and an append-only hashed external report. This is the
// middle one, and it keeps the chain from package report so that history in the
// database and the file report can be checked against each other. If the two
// disagree, one of them was edited.
//
// The table is deliberately append-only in use: there is no update path in this
// package, and a deployment should grant the audit INSERT and SELECT only.
package pgstore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/baotnq/cause-justified-change-monitoring/internal/report"
)

// DefaultTable is the table the audit writes to.
const DefaultTable = "audit_alerts"

// schemaTemplate is applied at startup. Kept in one string so a reviewer can see
// the whole storage contract at once.
const schemaTemplate = `
CREATE TABLE IF NOT EXISTS %[1]s (
    seq         BIGINT PRIMARY KEY,
    ts          TIMESTAMPTZ  NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end   TIMESTAMPTZ NOT NULL,
    alert_type  TEXT         NOT NULL,
    actor_count INT          NOT NULL,
    payload     JSONB        NOT NULL,
    prev_hash   CHAR(64)     NOT NULL,
    hash        CHAR(64)     NOT NULL UNIQUE
);

CREATE INDEX IF NOT EXISTS %[1]s_window_idx ON %[1]s (window_start);
CREATE INDEX IF NOT EXISTS %[1]s_type_idx   ON %[1]s (alert_type);
`

// Schema renders the storage contract for a given table name.
func Schema(table string) string { return fmt.Sprintf(schemaTemplate, table) }

// Store writes alert entries.
type Store struct {
	pool  *pgxpool.Pool
	table string
}

// Open connects and applies the schema to DefaultTable.
func Open(ctx context.Context, dsn string) (*Store, error) {
	return OpenTable(ctx, dsn, DefaultTable)
}

// OpenTable is Open against a named table. The name is interpolated into DDL,
// so it comes from configuration, never from a request.
func OpenTable(ctx context.Context, dsn, table string) (*Store, error) {
	if !validTableName(table) {
		return nil, fmt.Errorf("invalid table name %q", table)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	if _, err := pool.Exec(ctx, Schema(table)); err != nil {
		pool.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return &Store{pool: pool, table: table}, nil
}

func validTableName(s string) bool {
	if s == "" || len(s) > 63 {
		return false
	}
	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r == '_':
		case r >= '0' && r <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}

// Table is the table this store writes to.
func (s *Store) Table() string { return s.table }

// Close releases the pool.
func (s *Store) Close() { s.pool.Close() }

// AlertRow is the queryable part of an entry, extracted from the payload so
// operators do not have to reach into JSON for the common questions.
type AlertRow struct {
	Type        string
	WindowStart time.Time
	WindowEnd   time.Time
	ActorCount  int
}

// Append stores one report entry. The primary key on seq and the unique hash
// mean a replayed or duplicated entry is rejected by the database rather than
// quietly written twice.
func (s *Store) Append(ctx context.Context, e report.Entry, row AlertRow) error {
	_, err := s.pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (seq, ts, window_start, window_end, alert_type, actor_count, payload, prev_hash, hash)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`, s.table),
		e.Seq, e.TS, row.WindowStart, row.WindowEnd, row.Type, row.ActorCount,
		[]byte(e.Payload), e.PrevHash, e.Hash)
	if err != nil {
		return fmt.Errorf("insert alert seq %d: %w", e.Seq, err)
	}
	return nil
}

// Entries reads the chain back in order.
func (s *Store) Entries(ctx context.Context) ([]report.Entry, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		SELECT seq, ts, payload, prev_hash, hash FROM %s ORDER BY seq`, s.table))
	if err != nil {
		return nil, fmt.Errorf("query alerts: %w", err)
	}
	defer rows.Close()

	var out []report.Entry
	for rows.Next() {
		var e report.Entry
		var payload []byte
		if err := rows.Scan(&e.Seq, &e.TS, &payload, &e.PrevHash, &e.Hash); err != nil {
			return nil, fmt.Errorf("scan alert: %w", err)
		}
		e.Payload = json.RawMessage(payload)
		out = append(out, e)
	}
	return out, rows.Err()
}

// Verify replays the stored chain. Same arithmetic as the file report, so the
// database copy stands on its own as evidence.
func (s *Store) Verify(ctx context.Context) (int, string, error) {
	entries, err := s.Entries(ctx)
	if err != nil {
		return 0, "", err
	}
	buf, err := report.Encode(entries)
	if err != nil {
		return 0, "", err
	}
	verified, head, err := report.Verify(buf)
	return len(verified), head, err
}
