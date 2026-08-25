package idmap

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
)

func TestInternIsStableAndDense(t *testing.T) {
	m := New()
	a := m.Intern("u-alice")
	b := m.Intern("u-bob")
	if a != 0 || b != 1 {
		t.Fatalf("ids = %d, %d; want dense 0, 1", a, b)
	}
	if again := m.Intern("u-alice"); again != a {
		t.Fatalf("re-intern gave %d, want %d — ids must never move", again, a)
	}
	if got := m.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2", got)
	}
}

func TestNameRoundTrip(t *testing.T) {
	m := New()
	id := m.Intern("u-carol")
	if name, ok := m.Name(id); !ok || name != "u-carol" {
		t.Fatalf("Name(%d) = %q, %v; want u-carol, true", id, name, ok)
	}
	if _, ok := m.Name(999); ok {
		t.Fatal("Name of an unassigned id reported ok")
	}
}

// An id the monitor cannot resolve must still appear in the alert. Dropping it
// would understate a violation, which is the failure direction A4 warns about.
func TestNamesRendersUnmappedIdsRatherThanDroppingThem(t *testing.T) {
	m := New()
	m.Intern("u-alice")
	got := m.Names([]uint32{0, 7})
	if len(got) != 2 {
		t.Fatalf("Names dropped an id: %v", got)
	}
	if !strings.Contains(got[1], "7") {
		t.Fatalf("unmapped id rendered as %q, want it to name the id", got[1])
	}
}

func TestConcurrentInternAssignsEachKeyExactlyOneId(t *testing.T) {
	m := New()
	const keys, writers = 50, 8

	var wg sync.WaitGroup
	seen := make([][]uint32, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ids := make([]uint32, keys)
			for k := 0; k < keys; k++ {
				ids[k] = m.Intern(fmt.Sprintf("actor-%d", k))
			}
			seen[w] = ids
		}(w)
	}
	wg.Wait()

	if got := m.Len(); got != keys {
		t.Fatalf("Len() = %d, want %d — a racing Intern assigned a duplicate id", got, keys)
	}
	for w := 1; w < writers; w++ {
		for k := 0; k < keys; k++ {
			if seen[w][k] != seen[0][k] {
				t.Fatalf("key %d got id %d in one goroutine and %d in another", k, seen[0][k], seen[w][k])
			}
		}
	}
}

func TestChecksumDetectsAnyDifferenceInAssignment(t *testing.T) {
	a, b := New(), New()
	a.Intern("u-alice")
	a.Intern("u-bob")
	b.Intern("u-alice")
	b.Intern("u-bob")
	if a.Checksum() != b.Checksum() {
		t.Fatal("identical assignments produced different checksums")
	}

	c := New()
	c.Intern("u-bob") // same keys, different ids
	c.Intern("u-alice")
	if a.Checksum() == c.Checksum() {
		t.Fatal("a permuted assignment produced the same checksum — mis-attribution would go unnoticed")
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	m := New()
	for _, k := range []string{"u-alice", "u-bob", "u-carol"} {
		m.Intern(k)
	}

	var buf bytes.Buffer
	if err := m.Snapshot(&buf); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	loaded, err := Load(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.Checksum() != m.Checksum() {
		t.Fatal("round trip changed the assignment")
	}
	for _, k := range []string{"u-alice", "u-bob", "u-carol"} {
		orig, _ := m.Lookup(k)
		back, ok := loaded.Lookup(k)
		if !ok || back != orig {
			t.Fatalf("key %q: id %d before, %d after", k, orig, back)
		}
	}
}

// A corrupted map does not fail loudly at use time — it silently accuses the
// wrong actor. So it has to fail loudly at load time.
func TestLoadRejectsTamperedSnapshot(t *testing.T) {
	m := New()
	m.Intern("u-alice")
	m.Intern("u-bob")

	var buf bytes.Buffer
	if err := m.Snapshot(&buf); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	var raw map[string]any
	if err := json.Unmarshal(buf.Bytes(), &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	raw["keys"] = []string{"u-bob", "u-alice"} // swap two actors, keep the checksum
	tampered, _ := json.Marshal(raw)

	if _, err := Load(bytes.NewReader(tampered)); err == nil {
		t.Fatal("Load accepted a snapshot whose checksum does not match its content")
	}
}

func TestLoadRejectsNonInjectiveSnapshot(t *testing.T) {
	raw := `{"keys":["u-alice","u-alice"],"checksum":"whatever"}`
	if _, err := Load(strings.NewReader(raw)); err == nil {
		t.Fatal("Load accepted a map that assigns two ids to one key")
	}
}
