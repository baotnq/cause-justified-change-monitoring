// Package monitor is the invariant engine of Part A: it consumes a change
// channel and a cause channel, and for every window decides
//
//	V(w) = Changed(w) \ Justified(w)
//
// It knows nothing about Redis, NATS or Postgres. The channels are interfaces
// filled in by the adapters, which is what makes the same engine usable for an
// exchange, a warehouse or an AI agent's tool-call log.
//
// Everything here is off the audited system's critical path by construction:
// the engine only accepts events that were pushed to it, and it never calls out.
package monitor

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/baotnq/cause-justified-change-monitoring/internal/bitset"
	"github.com/baotnq/cause-justified-change-monitoring/internal/idmap"
)

// ChangeEvent is one observation from the change channel: the store reported
// that an actor's protected state was written. It carries no authorisation —
// that is the whole point, the change channel sees writes that bypassed every
// service.
type ChangeEvent struct {
	Actor  string    // actor key as the store knows it
	TS     time.Time // event time, not arrival time
	Delta  int64     // optional, only used when amount conservation is enabled
	Source string    // which feed saw it, e.g. "redis-keyspace" or "pg-cdc"
}

// CauseEvent is one authorized business event from the cause channel.
type CauseEvent struct {
	Actor    string
	TS       time.Time
	Kind     string // "deposit", "order.matched", "balance.updated", "tool.call.approved"
	Producer string // authenticated publisher identity
	Amount   int64
	Ref      string // deeper reference (order id, plan step) for the nested invariant
}

// Config fixes the behaviour of one monitor instance.
type Config struct {
	// WindowSize cuts time into windows; Grace is how long after a window ends
	// we keep accepting events for it before closing (Part E: window skew).
	WindowSize time.Duration
	Grace      time.Duration

	// Capacity is the id space the bit vectors cover. Fixed, not grown: see
	// bitset.Set and Part B1.
	Capacity uint32

	// TrustedProducers, when non-empty, is the allow-list for the cause channel.
	// A cause event from anyone else does not justify anything — otherwise an
	// attacker who can publish to the bus can manufacture their own innocence.
	TrustedProducers map[string]bool

	// DerivedCauseKinds are cause kinds that are not self-justifying and must
	// themselves be backed by a RootCauseKind for the same actor in the window.
	// This is the nested invariant of Part A2:
	//
	//	BalanceUpdated(w) ⊆ (Ordered ∪ Matched ∪ Transferred)(w)
	DerivedCauseKinds map[string]bool
	RootCauseKinds    map[string]bool

	// CheckAmounts turns on the conservation layer of Part E: per actor, the
	// sum of authorized amounts must equal the observed change in state.
	// Existence of a cause and correctness of the amount are different
	// questions; this answers the second one.
	CheckAmounts bool

	// MaxEvidencePerActor bounds how many raw events an alert carries.
	MaxEvidencePerActor int
}

func (c *Config) withDefaults() {
	if c.WindowSize <= 0 {
		c.WindowSize = time.Minute
	}
	if c.Grace < 0 {
		c.Grace = 0
	}
	if c.Capacity == 0 {
		c.Capacity = 1 << 20
	}
	if c.MaxEvidencePerActor == 0 {
		c.MaxEvidencePerActor = 3
	}
}

// Alert types. One alert is one finding about one window.
const (
	// AlertUnauthorizedChange is V(w) ≠ ∅: state moved with no authorized cause.
	AlertUnauthorizedChange = "unauthorized_change"
	// AlertMissingChange is Justified \ Changed: an authorized cause with no
	// observed write. Not a violation of the invariant, but it means the change
	// feed dropped an event, lagged, or was silenced — and a silenced feed is
	// how an attacker would hide from this monitor.
	AlertMissingChange = "missing_change"
	// AlertForgedCause is a derived cause with no root cause behind it.
	AlertForgedCause = "forged_cause"
	// AlertUntrustedProducer is a cause event from outside the allow-list.
	AlertUntrustedProducer = "untrusted_producer"
	// AlertAmountMismatch is a cause that exists but does not add up.
	AlertAmountMismatch = "amount_mismatch"
	// AlertCapacityExceeded means an actor id fell outside the bit vectors. The
	// monitor cannot see that actor, so it says so instead of going quiet.
	AlertCapacityExceeded = "capacity_exceeded"
	// AlertLateEvent means an event arrived for an already-closed window.
	AlertLateEvent = "late_event"
)

// WindowRef identifies the window an alert is about.
type WindowRef struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

// Alert is the monitor's only output. Shape matches Part C:
// {type, actor_ids, window, evidence}.
type Alert struct {
	Type     string            `json:"type"`
	Window   WindowRef         `json:"window"`
	ActorIDs []string          `json:"actor_ids"`
	Evidence map[string]string `json:"evidence,omitempty"`
}

type window struct {
	start time.Time

	changed   *bitset.Set // ids the change channel reported
	justified *bitset.Set // ids with an authorized cause
	derived   *bitset.Set // ids with a derived cause (needs a root behind it)
	rooted    *bitset.Set // ids with a root cause

	changeSum map[uint32]int64
	causeSum  map[uint32]int64
	evidence  map[uint32][]string
}

func newWindow(start time.Time, capacity uint32) *window {
	return &window{
		start:     start,
		changed:   bitset.New(capacity),
		justified: bitset.New(capacity),
		derived:   bitset.New(capacity),
		rooted:    bitset.New(capacity),
		changeSum: map[uint32]int64{},
		causeSum:  map[uint32]int64{},
		evidence:  map[uint32][]string{},
	}
}

// Monitor is safe for concurrent use: the two channels are separate goroutines.
type Monitor struct {
	cfg Config
	ids *idmap.Map

	mu      sync.Mutex
	windows map[int64]*window // keyed by window start, unix nanos
	closed  map[int64]bool    // windows already reported, to detect late events
	pending []Alert           // alerts raised at ingest rather than at close
}

// New returns a monitor. The id map is shared with the adapters so that alerts
// can be rendered with the actor keys the operator recognises.
func New(cfg Config, ids *idmap.Map) *Monitor {
	cfg.withDefaults()
	if ids == nil {
		ids = idmap.New()
	}
	return &Monitor{
		cfg:     cfg,
		ids:     ids,
		windows: map[int64]*window{},
		closed:  map[int64]bool{},
	}
}

// IDs exposes the id map for adapters and for snapshotting.
func (m *Monitor) IDs() *idmap.Map { return m.ids }

func (m *Monitor) windowStart(ts time.Time) time.Time {
	return ts.Truncate(m.cfg.WindowSize)
}

// windowFor returns the window an event belongs to, or nil if that window has
// already been closed (in which case the caller raises a late-event alert).
// Caller holds m.mu.
func (m *Monitor) windowFor(ts time.Time) *window {
	start := m.windowStart(ts)
	key := start.UnixNano()
	if m.closed[key] {
		return nil
	}
	w, ok := m.windows[key]
	if !ok {
		w = newWindow(start, m.cfg.Capacity)
		m.windows[key] = w
	}
	return w
}

func (m *Monitor) addEvidence(w *window, id uint32, line string) {
	if len(w.evidence[id]) < m.cfg.MaxEvidencePerActor {
		w.evidence[id] = append(w.evidence[id], line)
	}
}

func (m *Monitor) ref(start time.Time) WindowRef {
	return WindowRef{Start: start, End: start.Add(m.cfg.WindowSize)}
}

// ObserveChange feeds one event from the change channel.
func (m *Monitor) ObserveChange(ev ChangeEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()

	w := m.windowFor(ev.TS)
	if w == nil {
		m.pending = append(m.pending, Alert{
			Type:     AlertLateEvent,
			Window:   m.ref(m.windowStart(ev.TS)),
			ActorIDs: []string{ev.Actor},
			Evidence: map[string]string{"channel": "change", "source": ev.Source},
		})
		return
	}

	id := m.ids.Intern(ev.Actor)
	if !w.changed.Add(id) {
		m.pending = append(m.pending, Alert{
			Type:     AlertCapacityExceeded,
			Window:   m.ref(w.start),
			ActorIDs: []string{ev.Actor},
			Evidence: map[string]string{"id": fmt.Sprint(id), "capacity": fmt.Sprint(m.cfg.Capacity)},
		})
		return
	}
	w.changeSum[id] += ev.Delta
	m.addEvidence(w, id, fmt.Sprintf("change from %s at %s delta=%d",
		orUnknown(ev.Source), ev.TS.UTC().Format(time.RFC3339), ev.Delta))
}

// ObserveCause feeds one event from the cause channel.
func (m *Monitor) ObserveCause(ev CauseEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Producer identity is checked before anything else. An event we do not
	// trust must not be allowed to justify a change; if it were, publishing to
	// the bus would be enough to launder a tampered balance.
	if len(m.cfg.TrustedProducers) > 0 && !m.cfg.TrustedProducers[ev.Producer] {
		m.pending = append(m.pending, Alert{
			Type:     AlertUntrustedProducer,
			Window:   m.ref(m.windowStart(ev.TS)),
			ActorIDs: []string{ev.Actor},
			Evidence: map[string]string{"kind": ev.Kind, "producer": ev.Producer},
		})
		return
	}

	w := m.windowFor(ev.TS)
	if w == nil {
		m.pending = append(m.pending, Alert{
			Type:     AlertLateEvent,
			Window:   m.ref(m.windowStart(ev.TS)),
			ActorIDs: []string{ev.Actor},
			Evidence: map[string]string{"channel": "cause", "kind": ev.Kind},
		})
		return
	}

	id := m.ids.Intern(ev.Actor)
	if !w.justified.Add(id) {
		m.pending = append(m.pending, Alert{
			Type:     AlertCapacityExceeded,
			Window:   m.ref(w.start),
			ActorIDs: []string{ev.Actor},
			Evidence: map[string]string{"id": fmt.Sprint(id), "capacity": fmt.Sprint(m.cfg.Capacity)},
		})
		return
	}
	if m.cfg.DerivedCauseKinds[ev.Kind] {
		w.derived.Add(id)
	}
	if m.cfg.RootCauseKinds[ev.Kind] {
		w.rooted.Add(id)
	}
	w.causeSum[id] += ev.Amount
	m.addEvidence(w, id, fmt.Sprintf("cause %s from %s at %s amount=%d ref=%s",
		ev.Kind, orUnknown(ev.Producer), ev.TS.UTC().Format(time.RFC3339), ev.Amount, orUnknown(ev.Ref)))
}

// CloseDue closes every window whose end plus grace has passed and returns the
// alerts for them, together with any raised at ingest. Cost per window is
// O(N/64) for the set algebra plus O(|V|) to render.
func (m *Monitor) CloseDue(now time.Time) []Alert {
	m.mu.Lock()
	starts := make([]int64, 0, len(m.windows))
	for key, w := range m.windows {
		if !now.Before(w.start.Add(m.cfg.WindowSize).Add(m.cfg.Grace)) {
			starts = append(starts, key)
		}
	}
	m.mu.Unlock()

	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })

	var out []Alert
	for _, key := range starts {
		out = append(out, m.closeWindow(key)...)
	}
	return append(m.drainPending(), out...)
}

// CloseAll closes every open window regardless of grace. Used by tests and by
// shutdown, so that a stopped monitor does not lose findings.
func (m *Monitor) CloseAll() []Alert {
	m.mu.Lock()
	starts := make([]int64, 0, len(m.windows))
	for key := range m.windows {
		starts = append(starts, key)
	}
	m.mu.Unlock()

	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })

	var out []Alert
	for _, key := range starts {
		out = append(out, m.closeWindow(key)...)
	}
	return append(m.drainPending(), out...)
}

func (m *Monitor) drainPending() []Alert {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.pending
	m.pending = nil
	return p
}

func (m *Monitor) closeWindow(key int64) []Alert {
	m.mu.Lock()
	w := m.windows[key]
	if w == nil {
		m.mu.Unlock()
		return nil
	}
	delete(m.windows, key)
	m.closed[key] = true
	m.mu.Unlock()

	ref := m.ref(w.start)
	var alerts []Alert

	// The invariant. Everything else in this function is secondary.
	if v := bitset.Difference(w.changed, w.justified); !v.IsEmpty() {
		alerts = append(alerts, m.alert(AlertUnauthorizedChange, ref, v, w))
	}

	// The reverse direction: authorized, but never observed.
	if missing := bitset.Difference(w.justified, w.changed); !missing.IsEmpty() {
		alerts = append(alerts, m.alert(AlertMissingChange, ref, missing, w))
	}

	// Nested invariant: a derived cause with no root cause behind it.
	if forged := bitset.Difference(w.derived, w.rooted); !forged.IsEmpty() {
		alerts = append(alerts, m.alert(AlertForgedCause, ref, forged, w))
	}

	// Conservation: the cause exists, but does it account for the movement?
	if m.cfg.CheckAmounts {
		var ids []uint32
		for id, sum := range w.changeSum {
			if w.causeSum[id] != sum {
				ids = append(ids, id)
			}
		}
		if len(ids) > 0 {
			sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
			a := m.alertFromIDs(AlertAmountMismatch, ref, ids, w)
			for _, id := range ids {
				name, _ := m.ids.Name(id)
				a.Evidence["sums:"+name] = fmt.Sprintf("observed=%d authorized=%d", w.changeSum[id], w.causeSum[id])
			}
			alerts = append(alerts, a)
		}
	}

	return alerts
}

func (m *Monitor) alert(kind string, ref WindowRef, s *bitset.Set, w *window) Alert {
	return m.alertFromIDs(kind, ref, s.Ids(), w)
}

func (m *Monitor) alertFromIDs(kind string, ref WindowRef, ids []uint32, w *window) Alert {
	a := Alert{
		Type:     kind,
		Window:   ref,
		ActorIDs: m.ids.Names(ids),
		Evidence: map[string]string{},
	}
	for _, id := range ids {
		name, _ := m.ids.Name(id)
		for i, line := range w.evidence[id] {
			a.Evidence[fmt.Sprintf("%s#%d", name, i)] = line
		}
	}
	if len(a.Evidence) == 0 {
		a.Evidence = nil
	}
	return a
}

func orUnknown(s string) string {
	if s == "" {
		return "unknown"
	}
	return s
}
