// Package checkpoint implements the second layer of Part A6: the final
// money-moving step, and the audit of its own failures.
//
// The first layer (package monitor) is detective — it tells you afterwards that
// something moved without a cause. This layer is preventive: at the last step
// before money actually leaves, every party is re-checked and all legs commit
// or none do. An earlier decision — a match, an approved transfer — is not
// treated as final.
//
// The part that is easy to leave out, and that A6 insists on, is that failed
// settlements are data. One actor whose settlements keep failing against many
// counterparties is not a nuisance, it is a signal: someone is trying to spend
// a balance they do not have. So failures are counted per actor and per window,
// and a burst gates the downstream withdrawal.
package checkpoint

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// Account is the minimal state the checkpoint re-reads. Real systems have more;
// what matters is that these are read again here, not trusted from earlier.
type Account struct {
	Balance int64
	Banned  bool
}

// Leg is one movement inside a settlement.
type Leg struct {
	From   string
	To     string
	Amount int64
}

// Settlement is an all-or-nothing group of legs.
type Settlement struct {
	ID   string
	TS   time.Time
	Legs []Leg
}

// Failure records why a settlement did not commit.
type Failure struct {
	SettlementID string
	TS           time.Time
	Actor        string // the party that caused the failure
	Reason       string
}

// RiskAlert is raised when one actor's failures cluster in a window.
type RiskAlert struct {
	Actor        string    `json:"actor"`
	WindowStart  time.Time `json:"window_start"`
	Failures     int       `json:"failures"`
	Counterparty []string  `json:"counterparties"`
	Reason       string    `json:"reason"`
}

// Book is an in-memory account store with an atomic settlement checkpoint.
// It stands in for the real ledger; the logic that matters is the ordering:
// re-check everything, then apply everything, and never half of either.
type Book struct {
	mu       sync.Mutex
	accounts map[string]*Account

	windowSize     time.Duration
	burstThreshold int

	failures map[int64]map[string][]Failure // window start -> actor -> failures
	flagged  map[string]RiskAlert           // actors currently gated
}

// NewBook returns a book whose failure audit groups by windowSize and raises a
// risk alert at burstThreshold failures by the same actor in one window.
func NewBook(windowSize time.Duration, burstThreshold int) *Book {
	if windowSize <= 0 {
		windowSize = time.Minute
	}
	if burstThreshold <= 0 {
		burstThreshold = 3
	}
	return &Book{
		accounts:       map[string]*Account{},
		windowSize:     windowSize,
		burstThreshold: burstThreshold,
		failures:       map[int64]map[string][]Failure{},
		flagged:        map[string]RiskAlert{},
	}
}

// Credit sets up an account. Test and demo helper; a real deployment reads the
// ledger.
func (b *Book) Credit(actor string, amount int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.account(actor).Balance += amount
}

// Ban marks an actor as blacklisted.
func (b *Book) Ban(actor string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.account(actor).Banned = true
}

// Balance reads an account.
func (b *Book) Balance(actor string) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.account(actor).Balance
}

// caller holds b.mu
func (b *Book) account(actor string) *Account {
	a, ok := b.accounts[actor]
	if !ok {
		a = &Account{}
		b.accounts[actor] = a
	}
	return a
}

// Settle re-checks every party and applies all legs atomically. On the first
// problem it applies nothing, records a failure against the party at fault, and
// returns the error.
func (b *Book) Settle(s Settlement) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Phase 1: validate against current state. Balances are netted across legs
	// first, so a settlement cannot pass by spending the same balance twice.
	net := map[string]int64{}
	for _, leg := range s.Legs {
		net[leg.From] -= leg.Amount
		net[leg.To] += leg.Amount
		if leg.Amount <= 0 {
			return b.fail(s, leg.From, fmt.Sprintf("non-positive amount %d", leg.Amount))
		}
	}

	parties := make([]string, 0, len(net))
	for actor := range net {
		parties = append(parties, actor)
	}
	sort.Strings(parties) // deterministic party ordering, so failures are reproducible

	for _, actor := range parties {
		acct := b.account(actor)
		if acct.Banned {
			return b.fail(s, actor, "party is banned")
		}
		if acct.Balance+net[actor] < 0 {
			return b.fail(s, actor, fmt.Sprintf("insufficient balance: has %d, needs %d", acct.Balance, -net[actor]))
		}
	}

	// Phase 2: apply. Nothing above can fail here, because nothing here can
	// observe state the validation did not already see — the lock is held for
	// both phases.
	for _, actor := range parties {
		b.account(actor).Balance += net[actor]
	}
	return nil
}

// caller holds b.mu
func (b *Book) fail(s Settlement, actor, reason string) error {
	ts := s.TS
	if ts.IsZero() {
		ts = time.Now()
	}
	f := Failure{SettlementID: s.ID, TS: ts, Actor: actor, Reason: reason}

	key := ts.Truncate(b.windowSize).UnixNano()
	if b.failures[key] == nil {
		b.failures[key] = map[string][]Failure{}
	}
	b.failures[key][actor] = append(b.failures[key][actor], f)

	if n := len(b.failures[key][actor]); n >= b.burstThreshold {
		counterparties := map[string]bool{}
		for _, leg := range s.Legs {
			if leg.From != actor {
				counterparties[leg.From] = true
			}
			if leg.To != actor {
				counterparties[leg.To] = true
			}
		}
		names := make([]string, 0, len(counterparties))
		for c := range counterparties {
			names = append(names, c)
		}
		sort.Strings(names)

		b.flagged[actor] = RiskAlert{
			Actor:        actor,
			WindowStart:  ts.Truncate(b.windowSize),
			Failures:     n,
			Counterparty: names,
			Reason:       "settlement failure burst",
		}
	}
	return fmt.Errorf("settlement %s rejected: %s: %s", s.ID, actor, reason)
}

// Failures returns every recorded failure, oldest first.
func (b *Book) Failures() []Failure {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []Failure
	for _, byActor := range b.failures {
		for _, fs := range byActor {
			out = append(out, fs...)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].TS.Equal(out[j].TS) {
			return out[i].SettlementID < out[j].SettlementID
		}
		return out[i].TS.Before(out[j].TS)
	})
	return out
}

// RiskAlerts returns the actors whose failures clustered, sorted by actor.
func (b *Book) RiskAlerts() []RiskAlert {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]RiskAlert, 0, len(b.flagged))
	for _, a := range b.flagged {
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Actor < out[j].Actor })
	return out
}

// WithdrawalAllowed is the gate A6 describes: the downstream money-out step
// consults the audit before proceeding.
func (b *Book) WithdrawalAllowed(actor string) (bool, string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if a, ok := b.flagged[actor]; ok {
		return false, fmt.Sprintf("%s (%d failures in window starting %s)",
			a.Reason, a.Failures, a.WindowStart.UTC().Format(time.RFC3339))
	}
	return true, ""
}
