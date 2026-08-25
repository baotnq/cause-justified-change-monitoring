// Package idmap implements the identity mapping required by Part A4: real
// systems identify actors by string (UUID, external id), bit vectors need dense
// integers, so the pattern needs an explicit, bijective, append-only map.
//
// A4 calls this the method's most sensitive component, and it is: if the map is
// corrupted the monitor does not fail loudly, it mis-attributes — it accuses the
// wrong actor, or clears a guilty one. Hence the properties enforced here:
// ids are never reused, assignment is single-writer, and the map carries a
// checksum so a snapshot can be verified against the source of truth.
package idmap

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sync"
)

// Map is a bijection between string keys and dense uint32 ids.
type Map struct {
	mu  sync.RWMutex
	fwd map[string]uint32
	rev []string // rev[id] == key; index is the id, so append-only by construction
}

// New returns an empty map.
func New() *Map {
	return &Map{fwd: make(map[string]uint32)}
}

// Intern returns the id for key, assigning the next free id if the key is new.
// Called once per observed event, on the audit's own path — never on the
// audited application's path.
func (m *Map) Intern(key string) uint32 {
	m.mu.RLock()
	if id, ok := m.fwd[key]; ok {
		m.mu.RUnlock()
		return id
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()
	if id, ok := m.fwd[key]; ok { // another goroutine won the race
		return id
	}
	id := uint32(len(m.rev))
	m.fwd[key] = id
	m.rev = append(m.rev, key)
	return id
}

// Lookup resolves a key without assigning one.
func (m *Map) Lookup(key string) (uint32, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	id, ok := m.fwd[key]
	return id, ok
}

// Name resolves an id back to its key, for rendering alerts.
func (m *Map) Name(id uint32) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if int(id) >= len(m.rev) {
		return "", false
	}
	return m.rev[id], true
}

// Names resolves a list of ids, preserving order. Unknown ids are rendered
// explicitly rather than skipped: a violation we cannot name is still a
// violation, and dropping it would understate the alert.
func (m *Map) Names(ids []uint32) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if name, ok := m.Name(id); ok {
			out = append(out, name)
		} else {
			out = append(out, fmt.Sprintf("<unmapped id %d>", id))
		}
	}
	return out
}

// Len is the number of assigned ids, i.e. the capacity a window's bit vectors
// must cover.
func (m *Map) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.rev)
}

// Checksum is a SHA-256 over the id assignment in id order. Two replicas of the
// map agree if and only if their checksums agree; a snapshot can be re-derived
// from the source of truth and compared against this value.
func (m *Map) Checksum() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	h := sha256.New()
	for i, key := range m.rev {
		fmt.Fprintf(h, "%d\x00%s\x00", i, key)
	}
	return hex.EncodeToString(h.Sum(nil))
}

type snapshot struct {
	Keys     []string `json:"keys"` // index is the id
	Checksum string   `json:"checksum"`
}

// Snapshot writes the map so it can be reloaded or audited.
func (m *Map) Snapshot(w io.Writer) error {
	m.mu.RLock()
	keys := make([]string, len(m.rev))
	copy(keys, m.rev)
	m.mu.RUnlock()

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(snapshot{Keys: keys, Checksum: m.Checksum()})
}

// Load reads a snapshot and verifies its checksum. A map that fails this check
// is refused rather than repaired: silently loading a corrupted map is exactly
// the failure mode A4 warns about.
func Load(r io.Reader) (*Map, error) {
	var s snapshot
	if err := json.NewDecoder(r).Decode(&s); err != nil {
		return nil, fmt.Errorf("decode id map snapshot: %w", err)
	}
	m := New()
	for _, key := range s.Keys {
		if _, exists := m.fwd[key]; exists {
			return nil, fmt.Errorf("id map snapshot is not injective: duplicate key %q", key)
		}
		m.Intern(key)
	}
	if got := m.Checksum(); got != s.Checksum {
		return nil, fmt.Errorf("id map checksum mismatch: snapshot %s, recomputed %s", s.Checksum, got)
	}
	return m, nil
}
