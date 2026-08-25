package monitor

import (
	"sync"
	"testing"
	"time"

	"github.com/baotnq/cause-justified-change-monitoring/internal/idmap"
)

// Windows are assigned by event time, not arrival time. If arrival time were
// used, a lagging consumer would shift events into the wrong window and
// manufacture violations out of nothing but its own slowness.
func TestWindowsAreAssignedByEventTime(t *testing.T) {
	m := New(Config{WindowSize: time.Minute, Capacity: 1024}, idmap.New())

	// Both halves of a well-formed pair, delivered out of order and late, but
	// stamped inside the same window.
	m.ObserveChange(ChangeEvent{Actor: "u-alice", TS: at(10)})
	m.ObserveCause(CauseEvent{Actor: "u-alice", TS: at(5), Kind: "deposit.credited"})

	if alerts := m.CloseAll(); len(alerts) != 0 {
		t.Fatalf("out-of-order delivery inside one window produced alerts:\n%s", render(alerts))
	}
}

// An event that straddles a window boundary is the classic false positive of
// Part E. Grace is what buys the second half time to arrive.
func TestGraceHoldsTheWindowOpen(t *testing.T) {
	cfg := Config{WindowSize: time.Minute, Grace: 10 * time.Second, Capacity: 1024}
	m := New(cfg, idmap.New())

	m.ObserveChange(ChangeEvent{Actor: "u-alice", TS: at(59)})

	// One second after the window ended: still inside grace, nothing reported.
	if alerts := m.CloseDue(at(61)); len(alerts) != 0 {
		t.Fatalf("window closed during its grace period:\n%s", render(alerts))
	}

	// The cause lands late but inside grace.
	m.ObserveCause(CauseEvent{Actor: "u-alice", TS: at(59), Kind: "deposit.credited"})

	if alerts := m.CloseDue(at(75)); len(alerts) != 0 {
		t.Fatalf("a pair completed within grace was still reported:\n%s", render(alerts))
	}
}

func TestWindowClosesAfterGrace(t *testing.T) {
	cfg := Config{WindowSize: time.Minute, Grace: 10 * time.Second, Capacity: 1024}
	m := New(cfg, idmap.New())

	m.ObserveChange(ChangeEvent{Actor: "u-mallory", TS: at(30)})

	if alerts := m.CloseDue(at(65)); len(alerts) != 0 {
		t.Fatalf("closed before grace elapsed: %s", render(alerts))
	}
	alerts := m.CloseDue(at(71)) // 12:01:00 end + 10s grace
	assertOneAlert(t, alerts, AlertUnauthorizedChange, "u-mallory")
}

// Beyond grace the window is gone. The event is not silently dropped: an
// operator needs to know the monitor's view of that window is incomplete.
func TestEventAfterGraceIsReportedAsLate(t *testing.T) {
	cfg := Config{WindowSize: time.Minute, Grace: 10 * time.Second, Capacity: 1024}
	m := New(cfg, idmap.New())

	m.ObserveChange(ChangeEvent{Actor: "u-alice", TS: at(10)})
	m.ObserveCause(CauseEvent{Actor: "u-alice", TS: at(10), Kind: "deposit.credited"})
	if alerts := m.CloseDue(at(71)); len(alerts) != 0 {
		t.Fatalf("clean window reported: %s", render(alerts))
	}

	m.ObserveCause(CauseEvent{Actor: "u-bob", TS: at(10), Kind: "deposit.credited"})
	alerts := m.CloseDue(at(80))
	assertOneAlert(t, alerts, AlertLateEvent, "u-bob")
}

// An id outside the bit vectors cannot be tracked. The monitor says so rather
// than going quiet, because a silently untracked actor is a blind spot an
// attacker can aim for.
func TestCapacityOverflowIsLoud(t *testing.T) {
	m := New(Config{WindowSize: time.Minute, Capacity: 2}, idmap.New())

	m.ObserveChange(ChangeEvent{Actor: "u-1", TS: at(1)})
	m.ObserveChange(ChangeEvent{Actor: "u-2", TS: at(1)})
	m.ObserveChange(ChangeEvent{Actor: "u-3", TS: at(1)}) // id 2, capacity 2

	var sawOverflow bool
	for _, a := range m.CloseAll() {
		if a.Type == AlertCapacityExceeded && len(a.ActorIDs) == 1 && a.ActorIDs[0] == "u-3" {
			sawOverflow = true
		}
	}
	if !sawOverflow {
		t.Fatal("an actor outside the id space was dropped without an alert")
	}
}

// Producer identity is checked before the event can justify anything. Without
// it, publish access to the bus is enough to launder a tampered balance.
func TestUntrustedProducerCannotJustifyAChange(t *testing.T) {
	cfg := Config{
		WindowSize:       time.Minute,
		Capacity:         1024,
		TrustedProducers: map[string]bool{"payment-service": true},
	}
	m := New(cfg, idmap.New())

	m.ObserveChange(ChangeEvent{Actor: "u-mallory", TS: at(10), Delta: 1_000_000})
	m.ObserveCause(CauseEvent{
		Actor: "u-mallory", TS: at(10), Kind: "deposit.credited",
		Producer: "u-mallory-laptop", Amount: 1_000_000,
	})

	types := map[string]bool{}
	for _, a := range m.CloseAll() {
		types[a.Type] = true
	}
	if !types[AlertUntrustedProducer] {
		t.Fatal("event from an unknown producer was accepted silently")
	}
	if !types[AlertUnauthorizedChange] {
		t.Fatal("the change was treated as justified by an untrusted event")
	}
}

// Two channels means two goroutines. The engine is the shared thing between
// them, so it has to hold up under -race.
func TestConcurrentIngest(t *testing.T) {
	cfg := Config{WindowSize: time.Minute, Grace: time.Second, Capacity: 1 << 14}
	m := New(cfg, idmap.New())

	const n = 500
	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			m.ObserveChange(ChangeEvent{Actor: actorName(i), TS: at(i % 50), Delta: 1})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			m.ObserveCause(CauseEvent{Actor: actorName(i), TS: at(i % 50), Kind: "deposit.credited", Amount: 1})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			m.CloseDue(at(0)) // nothing is due yet; exercises the read path
		}
	}()
	wg.Wait()

	// Every actor has both halves, so the fully closed set must be clean.
	if alerts := m.CloseAll(); len(alerts) != 0 {
		t.Fatalf("balanced concurrent traffic produced alerts:\n%s", render(alerts))
	}
}

func actorName(i int) string {
	return "u-" + string(rune('a'+i%26)) + "-" + time.Duration(i).String()
}
