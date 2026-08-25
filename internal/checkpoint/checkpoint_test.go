package checkpoint

import (
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func at(sec int) time.Time { return t0.Add(time.Duration(sec) * time.Second) }

func TestSuccessfulSettlementAppliesEveryLeg(t *testing.T) {
	b := NewBook(time.Minute, 3)
	b.Credit("u-alice", 1_000)
	b.Credit("u-bob", 1_000)

	err := b.Settle(Settlement{ID: "stl-1", TS: at(1), Legs: []Leg{
		{From: "u-alice", To: "u-bob", Amount: 300},
		{From: "u-bob", To: "u-alice", Amount: 100},
	}})
	if err != nil {
		t.Fatalf("Settle: %v", err)
	}
	if got := b.Balance("u-alice"); got != 800 {
		t.Fatalf("alice = %d, want 800", got)
	}
	if got := b.Balance("u-bob"); got != 1_200 {
		t.Fatalf("bob = %d, want 1200", got)
	}
}

// The property that makes this a checkpoint rather than a formality: a
// settlement that fails leaves no trace in the balances at all.
func TestFailedSettlementAppliesNothing(t *testing.T) {
	b := NewBook(time.Minute, 3)
	b.Credit("u-alice", 1_000)
	b.Credit("u-mallory", 10)
	b.Credit("u-bob", 1_000)

	err := b.Settle(Settlement{ID: "stl-1", TS: at(1), Legs: []Leg{
		{From: "u-alice", To: "u-bob", Amount: 100},     // fine on its own
		{From: "u-mallory", To: "u-bob", Amount: 5_000}, // not fine
	}})
	if err == nil {
		t.Fatal("settlement committed with an underfunded leg")
	}
	if got := b.Balance("u-alice"); got != 1_000 {
		t.Fatalf("alice = %d, want 1000 — the good leg committed on its own", got)
	}
	if got := b.Balance("u-bob"); got != 1_000 {
		t.Fatalf("bob = %d, want 1000", got)
	}
	if got := b.Balance("u-mallory"); got != 10 {
		t.Fatalf("mallory = %d, want 10", got)
	}
}

// Legs are netted before the balance check, so the same balance cannot be
// promised to two counterparties inside one settlement.
func TestBalanceCannotBeSpentTwiceAcrossLegs(t *testing.T) {
	b := NewBook(time.Minute, 3)
	b.Credit("u-mallory", 100)
	b.Credit("u-alice", 0)
	b.Credit("u-bob", 0)

	err := b.Settle(Settlement{ID: "stl-1", TS: at(1), Legs: []Leg{
		{From: "u-mallory", To: "u-alice", Amount: 100},
		{From: "u-mallory", To: "u-bob", Amount: 100},
	}})
	if err == nil {
		t.Fatal("100 was spent twice in one settlement")
	}
	if got := b.Balance("u-mallory"); got != 100 {
		t.Fatalf("mallory = %d, want 100", got)
	}
}

// Incoming legs within the same settlement do count, so a genuine pass-through
// does not need a pre-funded balance.
func TestNettingAllowsPassThrough(t *testing.T) {
	b := NewBook(time.Minute, 3)
	b.Credit("u-alice", 500)

	err := b.Settle(Settlement{ID: "stl-1", TS: at(1), Legs: []Leg{
		{From: "u-alice", To: "u-broker", Amount: 500},
		{From: "u-broker", To: "u-bob", Amount: 500},
	}})
	if err != nil {
		t.Fatalf("pass-through rejected: %v", err)
	}
	if got := b.Balance("u-broker"); got != 0 {
		t.Fatalf("broker = %d, want 0", got)
	}
	if got := b.Balance("u-bob"); got != 500 {
		t.Fatalf("bob = %d, want 500", got)
	}
}

// Ban state is re-read here, not inherited from whatever check ran when the
// order was accepted.
func TestBannedPartyBlocksTheWholeSettlement(t *testing.T) {
	b := NewBook(time.Minute, 3)
	b.Credit("u-alice", 1_000)
	b.Credit("u-bob", 1_000)
	b.Ban("u-bob")

	if err := b.Settle(Settlement{ID: "stl-1", TS: at(1), Legs: []Leg{
		{From: "u-alice", To: "u-bob", Amount: 100},
	}}); err == nil {
		t.Fatal("settlement with a banned counterparty committed")
	}
	if got := b.Balance("u-alice"); got != 1_000 {
		t.Fatalf("alice = %d, want 1000", got)
	}
}

func TestFailureBurstGatesWithdrawalOnlyForTheActorAtFault(t *testing.T) {
	b := NewBook(time.Minute, 3)
	b.Credit("u-mallory", 10)
	for _, m := range []string{"u-alice", "u-bob", "u-carol"} {
		b.Credit(m, 1_000)
		_ = b.Settle(Settlement{ID: "stl-" + m, TS: at(1), Legs: []Leg{
			{From: "u-mallory", To: m, Amount: 500},
		}})
	}

	if allowed, reason := b.WithdrawalAllowed("u-mallory"); allowed || reason == "" {
		t.Fatalf("withdrawal allowed=%v reason=%q, want blocked with a reason", allowed, reason)
	}
	if allowed, _ := b.WithdrawalAllowed("u-alice"); !allowed {
		t.Fatal("a counterparty was gated for someone else's failures")
	}
	if got := len(b.Failures()); got != 3 {
		t.Fatalf("recorded %d failures, want 3", got)
	}
}

// Failures are grouped per window, so an actor who fails slowly over hours does
// not accumulate into a burst that never happened.
func TestFailuresInDifferentWindowsDoNotFormABurst(t *testing.T) {
	b := NewBook(time.Minute, 3)
	b.Credit("u-mallory", 10)
	b.Credit("u-alice", 1_000)

	for i := 0; i < 3; i++ {
		_ = b.Settle(Settlement{
			ID:   "stl-" + time.Duration(i).String(),
			TS:   at(i * 120), // one failure every two minutes
			Legs: []Leg{{From: "u-mallory", To: "u-alice", Amount: 500}},
		})
	}
	if alerts := b.RiskAlerts(); len(alerts) != 0 {
		t.Fatalf("risk alerts = %+v, want none: the failures are in different windows", alerts)
	}
	if allowed, _ := b.WithdrawalAllowed("u-mallory"); !allowed {
		t.Fatal("withdrawal gated without a burst")
	}
}

func TestNonPositiveAmountIsRejected(t *testing.T) {
	b := NewBook(time.Minute, 3)
	b.Credit("u-alice", 1_000)
	if err := b.Settle(Settlement{ID: "stl-1", TS: at(1), Legs: []Leg{
		{From: "u-alice", To: "u-bob", Amount: -100}, // a "transfer" that credits the sender
	}}); err == nil {
		t.Fatal("negative-amount leg accepted")
	}
	if got := b.Balance("u-alice"); got != 1_000 {
		t.Fatalf("alice = %d, want 1000", got)
	}
}
