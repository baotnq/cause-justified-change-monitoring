package pgstore

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/baotnq/cause-justified-change-monitoring/internal/report"
)

//	docker compose up -d postgres
//	PG_DSN=postgres://audit:audit@127.0.0.1:5432/audit go test ./internal/pgstore

func open(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		t.Skip("PG_DSN not set; skipping Postgres integration test")
	}
	table := fmt.Sprintf("audit_alerts_test_%d", time.Now().UnixNano())
	s, err := OpenTable(context.Background(), dsn, table)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		_, _ = s.pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table)
		s.Close()
	})
	return s
}

type alert struct {
	Type  string `json:"type"`
	Actor string `json:"actor"`
}

func TestAppendAndVerifyChain(t *testing.T) {
	s := open(t)
	ctx := context.Background()

	var buf bytes.Buffer
	w := report.NewWriter(&buf)

	for _, a := range []alert{
		{Type: "unauthorized_change", Actor: "u-mallory"},
		{Type: "missing_change", Actor: "u-dave"},
	} {
		e, err := w.Append(a)
		if err != nil {
			t.Fatal(err)
		}
		row := AlertRow{
			Type:        a.Type,
			WindowStart: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
			WindowEnd:   time.Date(2026, 1, 1, 12, 1, 0, 0, time.UTC),
			ActorCount:  1,
		}
		if err := s.Append(ctx, e, row); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	n, head, err := s.Verify(ctx)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if n != 2 {
		t.Fatalf("verified %d entries, want 2", n)
	}
	if head != w.Head() {
		t.Fatalf("database head %s does not match the writer's head %s — the two copies disagree", head, w.Head())
	}
}

// A replayed entry must not land twice. Two copies of one alert would make the
// stored chain disagree with the file report, and an evidence trail that
// disagrees with itself is not evidence.
func TestDuplicateEntryIsRejectedByTheDatabase(t *testing.T) {
	s := open(t)
	ctx := context.Background()

	var buf bytes.Buffer
	w := report.NewWriter(&buf)
	e, err := w.Append(alert{Type: "unauthorized_change", Actor: "u-mallory"})
	if err != nil {
		t.Fatal(err)
	}
	row := AlertRow{Type: "unauthorized_change", WindowStart: time.Now(), WindowEnd: time.Now(), ActorCount: 1}

	if err := s.Append(ctx, e, row); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(ctx, e, row); err == nil {
		t.Fatal("the same entry was stored twice")
	}
}

// The rendering the storage layer does must not change what the hash covers.
func TestPayloadSurvivesTheRoundTripThroughJSONB(t *testing.T) {
	s := open(t)
	ctx := context.Background()

	var buf bytes.Buffer
	w := report.NewWriter(&buf)
	e, err := w.Append(alert{Type: "forged_cause", Actor: "u-mallory"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Append(ctx, e, AlertRow{Type: "forged_cause", WindowStart: time.Now(), WindowEnd: time.Now(), ActorCount: 1}); err != nil {
		t.Fatal(err)
	}

	entries, err := s.Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("read %d entries, want 1", len(entries))
	}
	if entries[0].Hash != e.Hash {
		t.Fatal("stored hash differs from the written hash")
	}
	if _, _, err := report.Verify(mustEncode(t, entries)); err != nil {
		t.Fatalf("chain read back from Postgres does not verify: %v", err)
	}
}

func TestVerifyDetectsATamperedRow(t *testing.T) {
	s := open(t)
	ctx := context.Background()

	var buf bytes.Buffer
	w := report.NewWriter(&buf)
	for _, a := range []alert{{Type: "a", Actor: "u-1"}, {Type: "b", Actor: "u-2"}} {
		e, err := w.Append(a)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Append(ctx, e, AlertRow{Type: a.Type, WindowStart: time.Now(), WindowEnd: time.Now(), ActorCount: 1}); err != nil {
			t.Fatal(err)
		}
	}

	// Someone with UPDATE on the table edits the alert that names them. In a
	// real deployment the audit role has INSERT and SELECT only, so this is
	// what a compromised operator account looks like, not the audit itself.
	if _, err := s.pool.Exec(ctx,
		fmt.Sprintf(`UPDATE %s SET payload = jsonb_set(payload, '{actor}', '"u-nobody"') WHERE seq = 0`, s.table),
	); err != nil {
		t.Fatal(err)
	}

	if _, _, err := s.Verify(ctx); err == nil {
		t.Fatal("an edited row verified")
	}
}

func TestInvalidTableNameIsRejected(t *testing.T) {
	if _, err := OpenTable(context.Background(), "postgres://ignored", `alerts"; DROP TABLE users; --`); err == nil {
		t.Fatal("a table name carrying SQL was accepted")
	}
}

func mustEncode(t *testing.T, entries []report.Entry) *bytes.Reader {
	t.Helper()
	r, err := report.Encode(entries)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(r); err != nil {
		t.Fatal(err)
	}
	return bytes.NewReader(buf.Bytes())
}
