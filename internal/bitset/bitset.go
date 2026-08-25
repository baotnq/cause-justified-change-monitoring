// Package bitset implements the membership bit vector described in Part A3 of
// the README: a set of dense integer actor ids, stored one bit per id, with
// union / intersection / difference as word-wise machine operations.
//
// The point of the structure is that closing a window costs O(N/64) regardless
// of how much traffic the window saw, and that the answer is exact — no
// hashing per element, no probabilistic membership, no join.
package bitset

import (
	"math/bits"
)

const wordBits = 64

// Set is a bit vector over ids [0, capacity). Capacity is fixed at construction
// and never grows: a window's sets are sized once, from the id map, so that the
// set algebra below is total over the whole id space.
//
// That fixed capacity is not an optimisation, it is a correctness property.
// Length-trimmed bitmaps are the source of the Redis pitfall in Part B1: when a
// vector is shorter than the one it is subtracted from, the missing high bits
// are read as zero and violating actors disappear from the result. Difference
// here is defined so that cannot happen (see Difference), and the Redis adapter
// pins capacity explicitly to get the same guarantee out of BITOP.
type Set struct {
	words    []uint64
	capacity uint32
}

// New returns an empty set over ids [0, capacity).
func New(capacity uint32) *Set {
	return &Set{
		words:    make([]uint64, (int(capacity)+wordBits-1)/wordBits),
		capacity: capacity,
	}
}

// Cap reports the id space this set is defined over.
func (s *Set) Cap() uint32 { return s.capacity }

// Add records membership of id. Ids at or above capacity are out of the set's
// domain and are reported rather than silently dropped — a dropped id is a
// false negative, which is the one failure mode this whole pattern exists to
// avoid.
func (s *Set) Add(id uint32) (ok bool) {
	if id >= s.capacity {
		return false
	}
	s.words[id/wordBits] |= 1 << (id % wordBits)
	return true
}

// Has reports membership.
func (s *Set) Has(id uint32) bool {
	if id >= s.capacity {
		return false
	}
	return s.words[id/wordBits]&(1<<(id%wordBits)) != 0
}

// Count returns the number of members. O(N/64).
func (s *Set) Count() int {
	n := 0
	for _, w := range s.words {
		n += bits.OnesCount64(w)
	}
	return n
}

// IsEmpty is Count() == 0 without counting the whole vector.
func (s *Set) IsEmpty() bool {
	for _, w := range s.words {
		if w != 0 {
			return false
		}
	}
	return true
}

// Clone returns an independent copy.
func (s *Set) Clone() *Set {
	c := &Set{words: make([]uint64, len(s.words)), capacity: s.capacity}
	copy(c.words, s.words)
	return c
}

// Ids lists the members in ascending order. Used only to render an alert, never
// on the audit's hot path: the decision "is this window clean" is IsEmpty.
func (s *Set) Ids() []uint32 {
	out := make([]uint32, 0, s.Count())
	for wi, w := range s.words {
		for w != 0 {
			bit := bits.TrailingZeros64(w)
			out = append(out, uint32(wi*wordBits+bit))
			w &= w - 1 // clear lowest set bit
		}
	}
	return out
}

// Union is s |= o.
func (s *Set) Union(o *Set) {
	n := min(len(s.words), len(o.words))
	for i := 0; i < n; i++ {
		s.words[i] |= o.words[i]
	}
}

// Intersect is s &= o. Words beyond o's length are cleared: absent means absent.
func (s *Set) Intersect(o *Set) {
	for i := range s.words {
		if i < len(o.words) {
			s.words[i] &= o.words[i]
		} else {
			s.words[i] = 0
		}
	}
}

// Subtract is s &^= o, i.e. s \ o.
//
// Words of o beyond its own length are treated as zero, which for AND-NOT means
// the corresponding members of s are *kept*. This is the safe direction: a
// shorter subtrahend can never erase a member of s. Redis BITOP has the
// opposite bias (see Part B1), which is why the adapter pins both operands to
// the same byte length before asking Redis to do this same computation.
func (s *Set) Subtract(o *Set) {
	n := min(len(s.words), len(o.words))
	for i := 0; i < n; i++ {
		s.words[i] &^= o.words[i]
	}
}

// Difference returns a \ b without modifying either operand.
func Difference(a, b *Set) *Set {
	d := a.Clone()
	d.Subtract(b)
	return d
}

// ToBytes serialises the vector in the same little-endian bit order Redis uses
// for SETBIT/BITOP, so a Go-side set and a Redis-side bitmap of the same
// members compare byte for byte. Redis numbers bits from the most significant
// bit of byte 0, hence the reversal within each byte.
func (s *Set) ToBytes() []byte {
	out := make([]byte, (int(s.capacity)+7)/8)
	for _, id := range s.Ids() {
		out[id/8] |= 1 << (7 - id%8)
	}
	return out
}

// FromBytes reads a Redis-order bitmap back into a set of the given capacity.
func FromBytes(b []byte, capacity uint32) *Set {
	s := New(capacity)
	for i, by := range b {
		for bit := 0; bit < 8; bit++ {
			if by&(1<<(7-bit)) != 0 {
				s.Add(uint32(i*8 + bit))
			}
		}
	}
	return s
}
