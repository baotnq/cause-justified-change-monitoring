// The six scenarios of README Part C, as executable tests.
//
// Each one drives the engine with a synthetic trace and compares the resulting
// alerts against a golden fixture in testdata/. Run with -update to rewrite the
// fixtures after an intentional change:
//
//	go test ./internal/monitor -update
//
// These run in-process with no infrastructure, so `go test ./...` on a fresh
// clone reproduces every claim the README makes about what gets caught. The
// same scenarios run against real Redis and NATS in cmd/scenario.
package monitor

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/baotnq/cause-justified-change-monitoring/internal/checkpoint"
	"github.com/baotnq/cause-justified-change-monitoring/internal/idmap"
)

var update = flag.Bool("update", false, "rewrite golden alert fixtures")

// A fixed clock keeps fixtures byte-stable.
var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func at(seconds int) time.Time { return t0.Add(time.Duration(seconds) * time.Second) }

// exchangeConfig is the configuration Part B describes: one-minute windows,
// ten seconds of grace, a trusted set of producers, and the nested invariant
// wired up so that a bare balance.updated does not justify itself.
func exchangeConfig() Config {
	return Config{
		WindowSize: time.Minute,
		Grace:      10 * time.Second,
		Capacity:   1 << 16,
		TrustedProducers: map[string]bool{
			"asset-service":    true,
			"matching-service": true,
			"payment-service":  true,
		},
		DerivedCauseKinds: map[string]bool{"asset.account.updated": true},
		RootCauseKinds: map[string]bool{
			"matching.order.matched": true,
			"transfer.completed":     true,
			"deposit.credited":       true,
		},
	}
}

// deposit is a well-formed pair: the store reported a write, and a trusted
// producer published the root cause behind it.
func deposit(m *Monitor, actor string, sec int, amount int64) {
	m.ObserveCause(CauseEvent{
		Actor: actor, TS: at(sec), Kind: "deposit.credited",
		Producer: "payment-service", Amount: amount, Ref: "dep-" + actor,
	})
	m.ObserveChange(ChangeEvent{Actor: actor, TS: at(sec), Delta: amount, Source: "redis-keyspace"})
}

func TestScenario1_NormalTraffic(t *testing.T) {
	m := New(exchangeConfig(), idmap.New())
	for i, actor := range []string{"u-alice", "u-bob", "u-carol"} {
		deposit(m, actor, 5+i, 100)
	}
	// A matched trade: both sides have a root cause and both sides moved.
	for _, actor := range []string{"u-alice", "u-bob"} {
		m.ObserveCause(CauseEvent{
			Actor: actor, TS: at(20), Kind: "matching.order.matched",
			Producer: "matching-service", Amount: 50, Ref: "ord-771",
		})
		m.ObserveChange(ChangeEvent{Actor: actor, TS: at(20), Delta: 50, Source: "redis-keyspace"})
	}

	alerts := m.CloseAll()
	if len(alerts) != 0 {
		t.Fatalf("clean traffic produced %d alerts:\n%s", len(alerts), render(alerts))
	}
	golden(t, "scenario1_normal_traffic", alerts)
}

func TestScenario2_DirectTamper(t *testing.T) {
	m := New(exchangeConfig(), idmap.New())
	deposit(m, "u-alice", 5, 100)

	// HSET asset:account:u-mallory balance 1000000 straight into the store.
	// No service ran, so no cause event exists — but the change feed still sees
	// the write, which is the entire reason the change channel is the store's
	// own feed and not the application's logs.
	m.ObserveChange(ChangeEvent{Actor: "u-mallory", TS: at(30), Delta: 1_000_000, Source: "redis-keyspace"})

	alerts := m.CloseAll()
	assertOneAlert(t, alerts, AlertUnauthorizedChange, "u-mallory")
	golden(t, "scenario2_direct_tamper", alerts)
}

func TestScenario3_MissingEvent(t *testing.T) {
	m := New(exchangeConfig(), idmap.New())
	deposit(m, "u-alice", 5, 100)

	// The application wrote and the bus dropped the message. The invariant
	// Changed ⊆ Justified is violated in the same direction as a tamper: from
	// the monitor's seat the two are indistinguishable, and that is correct —
	// both mean the evidence trail is broken.
	m.ObserveChange(ChangeEvent{Actor: "u-bob", TS: at(30), Delta: 250, Source: "redis-keyspace"})

	alerts := m.CloseAll()
	assertOneAlert(t, alerts, AlertUnauthorizedChange, "u-bob")
	golden(t, "scenario3_missing_event", alerts)
}

func TestScenario3b_SilencedChangeFeed(t *testing.T) {
	m := New(exchangeConfig(), idmap.New())
	// The mirror image: an authorized cause with no write observed. Either the
	// change feed lagged, or someone turned it off — and turning it off is the
	// obvious way to hide from this monitor, so it has to be an alert.
	m.ObserveCause(CauseEvent{
		Actor: "u-dave", TS: at(15), Kind: "deposit.credited",
		Producer: "payment-service", Amount: 500, Ref: "dep-u-dave",
	})

	alerts := m.CloseAll()
	assertOneAlert(t, alerts, AlertMissingChange, "u-dave")
	golden(t, "scenario3b_silenced_change_feed", alerts)
}

func TestScenario4_ForgedJustification(t *testing.T) {
	m := New(exchangeConfig(), idmap.New())
	deposit(m, "u-alice", 5, 100)

	// The attacker has publish access and knows the monitor compares two sets,
	// so they supply the missing cause themselves: a balance.updated for the
	// actor whose balance they tampered with. The flat invariant is satisfied.
	m.ObserveChange(ChangeEvent{Actor: "u-mallory", TS: at(30), Delta: 999_999, Source: "redis-keyspace"})
	m.ObserveCause(CauseEvent{
		Actor: "u-mallory", TS: at(30), Kind: "asset.account.updated",
		Producer: "asset-service", Amount: 999_999,
	})

	// The nested invariant is not: a balance.updated is derived, and there is no
	// order, transfer or deposit behind it.
	alerts := m.CloseAll()
	assertOneAlert(t, alerts, AlertForgedCause, "u-mallory")
	golden(t, "scenario4_forged_justification", alerts)
}

func TestScenario5_SettlementFailureBurst(t *testing.T) {
	// One taker with a forged balance sweeps many makers. Every leg is checked
	// against real state at the checkpoint, so nothing commits; the value of the
	// scenario is what happens to the failures afterwards.
	book := checkpoint.NewBook(time.Minute, 3)
	book.Credit("u-mallory", 10) // the real balance, not the tampered one
	for _, maker := range []string{"u-alice", "u-bob", "u-carol", "u-dave"} {
		book.Credit(maker, 1_000)
	}

	var errs []string
	for i, maker := range []string{"u-alice", "u-bob", "u-carol", "u-dave"} {
		err := book.Settle(checkpoint.Settlement{
			ID: fmt.Sprintf("stl-%d", i+1),
			TS: at(10 + i),
			Legs: []checkpoint.Leg{
				{From: "u-mallory", To: maker, Amount: 500},
			},
		})
		if err == nil {
			t.Fatalf("settlement %d committed against a balance of 10", i+1)
		}
		errs = append(errs, err.Error())
	}

	// No maker lost anything: all legs failed atomically.
	for _, maker := range []string{"u-alice", "u-bob", "u-carol", "u-dave"} {
		if got := book.Balance(maker); got != 1_000 {
			t.Fatalf("maker %s balance = %d, want 1000 — a leg committed on its own", maker, got)
		}
	}
	if got := book.Balance("u-mallory"); got != 10 {
		t.Fatalf("taker balance = %d, want 10", got)
	}

	risks := book.RiskAlerts()
	if len(risks) != 1 || risks[0].Actor != "u-mallory" {
		t.Fatalf("risk alerts = %+v, want one for u-mallory", risks)
	}
	if allowed, reason := book.WithdrawalAllowed("u-mallory"); allowed {
		t.Fatal("withdrawal allowed for an actor with a settlement failure burst")
	} else if reason == "" {
		t.Fatal("withdrawal blocked with no reason recorded")
	}
	if allowed, _ := book.WithdrawalAllowed("u-alice"); !allowed {
		t.Fatal("an uninvolved maker was gated")
	}

	golden(t, "scenario5_settlement_failure_burst", map[string]any{
		"settlement_errors": errs,
		"risk_alerts":       risks,
		"failures":          book.Failures(),
	})
}

func TestScenario6_AgentActingWithoutAuthorization(t *testing.T) {
	// Same engine, different channels. The change channel is the agent's
	// tool-call log — every external side effect it performed. The cause channel
	// is the approved plan. An agent that calls an effectful tool with no
	// approved step behind it lands in V(w).
	cfg := Config{
		WindowSize:       time.Minute,
		Grace:            10 * time.Second,
		Capacity:         1 << 16,
		TrustedProducers: map[string]bool{"plan-approver": true},
	}
	m := New(cfg, idmap.New())

	// An approved step, executed: fine.
	m.ObserveCause(CauseEvent{
		Actor: "agent-7/step-1", TS: at(5), Kind: "plan.step.approved",
		Producer: "plan-approver", Ref: "plan-42",
	})
	m.ObserveChange(ChangeEvent{Actor: "agent-7/step-1", TS: at(6), Source: "tool-call-log"})

	// A payment tool call with no approved step behind it.
	m.ObserveChange(ChangeEvent{Actor: "agent-7/step-2", TS: at(20), Source: "tool-call-log"})

	// The agent publishes its own authorization. Producer identity rejects it
	// before it can justify anything: the approver is the only trusted producer,
	// and an actor vouching for itself is not an authorization.
	m.ObserveCause(CauseEvent{
		Actor: "agent-7/step-2", TS: at(20), Kind: "plan.step.approved",
		Producer: "agent-7", Ref: "plan-42",
	})

	alerts := m.CloseAll()
	byType := map[string]Alert{}
	for _, a := range alerts {
		byType[a.Type] = a
	}
	if _, ok := byType[AlertUntrustedProducer]; !ok {
		t.Fatalf("self-published authorization was accepted; alerts:\n%s", render(alerts))
	}
	if a, ok := byType[AlertUnauthorizedChange]; !ok {
		t.Fatalf("unauthorized tool call not reported; alerts:\n%s", render(alerts))
	} else if len(a.ActorIDs) != 1 || a.ActorIDs[0] != "agent-7/step-2" {
		t.Fatalf("unauthorized_change actors = %v, want [agent-7/step-2]", a.ActorIDs)
	}
	golden(t, "scenario6_agent_without_authorization", alerts)
}

// The conservation layer of Part E: the cause exists, and is still wrong.
func TestScenario7_AmountMismatch(t *testing.T) {
	cfg := exchangeConfig()
	cfg.CheckAmounts = true
	m := New(cfg, idmap.New())

	// Authorized for 100, moved by 100_000. Existence checks pass; the sums do not.
	m.ObserveCause(CauseEvent{
		Actor: "u-eve", TS: at(10), Kind: "deposit.credited",
		Producer: "payment-service", Amount: 100, Ref: "dep-u-eve",
	})
	m.ObserveChange(ChangeEvent{Actor: "u-eve", TS: at(10), Delta: 100_000, Source: "redis-keyspace"})

	alerts := m.CloseAll()
	assertOneAlert(t, alerts, AlertAmountMismatch, "u-eve")
	golden(t, "scenario7_amount_mismatch", alerts)
}

// --- helpers ---

func assertOneAlert(t *testing.T, alerts []Alert, wantType, wantActor string) {
	t.Helper()
	var found *Alert
	for i := range alerts {
		if alerts[i].Type == wantType {
			found = &alerts[i]
		}
	}
	if found == nil {
		t.Fatalf("no %s alert; got:\n%s", wantType, render(alerts))
	}
	if len(found.ActorIDs) != 1 || found.ActorIDs[0] != wantActor {
		t.Fatalf("%s actors = %v, want [%s]", wantType, found.ActorIDs, wantActor)
	}
}

func render(alerts []Alert) string {
	b, _ := json.MarshalIndent(alerts, "", "  ")
	return string(b)
}

// golden compares a value against testdata/<name>.json.
func golden(t *testing.T, name string, v any) {
	t.Helper()

	if alerts, ok := v.([]Alert); ok {
		sort.SliceStable(alerts, func(i, j int) bool {
			if alerts[i].Type != alerts[j].Type {
				return alerts[i].Type < alerts[j].Type
			}
			return fmt.Sprint(alerts[i].ActorIDs) < fmt.Sprint(alerts[j].ActorIDs)
		})
		v = alerts
	}

	got, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got = append(got, '\n')

	path := filepath.Join("testdata", name+".json")
	if *update {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatalf("mkdir testdata: %v", err)
		}
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		return
	}

	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v (run: go test ./internal/monitor -update)", path, err)
	}
	if string(got) != string(want) {
		t.Errorf("alerts differ from %s\n--- got ---\n%s\n--- want ---\n%s", path, got, want)
	}
}
