package bitset

import (
	"reflect"
	"testing"
)

func setOf(capacity uint32, ids ...uint32) *Set {
	s := New(capacity)
	for _, id := range ids {
		if !s.Add(id) {
			panic("id out of capacity in test fixture")
		}
	}
	return s
}

func TestAddHasCount(t *testing.T) {
	s := setOf(1000, 0, 1, 63, 64, 999)
	for _, id := range []uint32{0, 1, 63, 64, 999} {
		if !s.Has(id) {
			t.Fatalf("Has(%d) = false, want true", id)
		}
	}
	if s.Has(2) {
		t.Fatal("Has(2) = true, want false")
	}
	if got := s.Count(); got != 5 {
		t.Fatalf("Count() = %d, want 5", got)
	}
}

func TestAddOutOfCapacityIsReported(t *testing.T) {
	s := New(64)
	if s.Add(64) {
		t.Fatal("Add(64) into capacity 64 reported success; a silently dropped id is a false negative")
	}
	if !s.IsEmpty() {
		t.Fatal("out-of-range Add mutated the set")
	}
}

func TestIdsAscending(t *testing.T) {
	s := setOf(200, 199, 5, 64, 0)
	want := []uint32{0, 5, 64, 199}
	if got := s.Ids(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Ids() = %v, want %v", got, want)
	}
}

func TestDifferenceIsTheViolationSet(t *testing.T) {
	// The invariant: Changed ⊆ Justified. Actor 7 changed with no cause.
	changed := setOf(1000, 3, 7, 42)
	justified := setOf(1000, 3, 42, 900)

	v := Difference(changed, justified)
	if got, want := v.Ids(), []uint32{7}; !reflect.DeepEqual(got, want) {
		t.Fatalf("violations = %v, want %v", got, want)
	}

	// The reverse set is a different alert, not a violation: an authorized cause
	// with no observed write means a dropped change event or a silenced feed.
	missing := Difference(justified, changed)
	if got, want := missing.Ids(), []uint32{900}; !reflect.DeepEqual(got, want) {
		t.Fatalf("missing = %v, want %v", got, want)
	}
}

// XOR is not the difference here. A XOR B equals A \ B only when B ⊆ A, and the
// containment this invariant guarantees runs the other way (Changed ⊆ Justified),
// so a XOR-based monitor reports the wrong set. Pinned as a regression test
// because the earlier draft of the README suggested XOR as an optimisation.
func TestXorWouldReportTheWrongSet(t *testing.T) {
	changed := setOf(1000, 3, 7)
	justified := setOf(1000, 3, 7, 900)

	xor := changed.Clone()
	for i := range xor.words {
		xor.words[i] ^= justified.words[i]
	}
	if xor.Count() == 0 {
		t.Fatal("test is vacuous")
	}
	if got, want := xor.Ids(), []uint32{900}; !reflect.DeepEqual(got, want) {
		t.Fatalf("xor = %v, want %v", got, want)
	}
	// 900 is justified and did not change: reporting it as a violation would be
	// an accusation against an innocent actor.
	if v := Difference(changed, justified); !v.IsEmpty() {
		t.Fatalf("true violation set = %v, want empty", v.Ids())
	}
}

// A shorter subtrahend must never erase members of the minuend. This is the
// Go-side statement of the Part B1 pitfall.
func TestSubtractWithShorterOperandKeepsHighMembers(t *testing.T) {
	changed := setOf(4096, 10, 4000)
	justified := setOf(128, 10) // only knows about low ids

	v := Difference(changed, justified)
	if got, want := v.Ids(), []uint32{4000}; !reflect.DeepEqual(got, want) {
		t.Fatalf("violations = %v, want %v — a high-id violation was lost", got, want)
	}
}

func TestUnionIntersect(t *testing.T) {
	a := setOf(256, 1, 2, 3)
	b := setOf(256, 3, 4)

	u := a.Clone()
	u.Union(b)
	if got, want := u.Ids(), []uint32{1, 2, 3, 4}; !reflect.DeepEqual(got, want) {
		t.Fatalf("union = %v, want %v", got, want)
	}

	i := a.Clone()
	i.Intersect(b)
	if got, want := i.Ids(), []uint32{3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("intersect = %v, want %v", got, want)
	}
}

func TestBytesRoundTripInRedisBitOrder(t *testing.T) {
	s := setOf(64, 0, 7, 8, 63)
	b := s.ToBytes()
	// Redis numbers bits from the most significant bit of byte 0.
	if b[0] != 0b1000_0001 {
		t.Fatalf("byte 0 = %08b, want 10000001", b[0])
	}
	if b[1] != 0b1000_0000 {
		t.Fatalf("byte 1 = %08b, want 10000000", b[1])
	}
	back := FromBytes(b, 64)
	if !reflect.DeepEqual(back.Ids(), s.Ids()) {
		t.Fatalf("round trip = %v, want %v", back.Ids(), s.Ids())
	}
}

func TestMemoryFootprintMatchesTheClaim(t *testing.T) {
	// README A3: N/8 bytes per set; 10M actors ≈ 1.25 MB.
	const n = 10_000_000
	s := New(n)
	if got, want := len(s.ToBytes()), n/8; got != want {
		t.Fatalf("serialised size = %d bytes, want %d", got, want)
	}
	if got := len(s.words) * 8; got > n/8+8 {
		t.Fatalf("in-memory size = %d bytes, want ~%d", got, n/8)
	}
}
