package bitset

import (
	"fmt"
	"math/rand"
	"testing"
)

// Part D, second question: how does the cost of the set operations grow with the
// number of actors? The claim is O(N/64) per window regardless of how much
// traffic the window saw, so these benchmarks vary N and hold traffic at zero —
// the difference is over the whole id space either way.
//
//	go test ./internal/bitset -bench Difference -benchmem

var sizes = []uint32{10_000, 100_000, 1_000_000, 10_000_000}

// populated builds a set with density members per mille, deterministically.
func populated(capacity uint32, perMille int) *Set {
	s := New(capacity)
	// Distinct seeds per (capacity, density): sharing one seed would make the
	// smaller set a prefix of the larger, and the difference would be empty.
	r := rand.New(rand.NewSource(int64(capacity)*31 + int64(perMille)))
	n := int(capacity) * perMille / 1000
	for i := 0; i < n; i++ {
		s.Add(uint32(r.Int31n(int32(capacity))))
	}
	return s
}

func BenchmarkDifference(b *testing.B) {
	for _, n := range sizes {
		changed := populated(n, 10)   // 1% of actors moved
		justified := populated(n, 12) // and rather more had a cause
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				d := changed.Clone()
				d.Subtract(justified)
				if d.IsEmpty() {
					b.Fatal("expected a non-empty difference")
				}
			}
		})
	}
}

// The difference without the clone: what closing a window actually costs when
// the destination is reused.
func BenchmarkSubtractInPlace(b *testing.B) {
	for _, n := range sizes {
		justified := populated(n, 12)
		scratch := populated(n, 10)
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				scratch.Subtract(justified)
			}
		})
	}
}

// Ingest cost: one bit per observed event. This is the per-event path, so it is
// the one that has to stay flat as traffic grows.
func BenchmarkAdd(b *testing.B) {
	s := New(10_000_000)
	r := rand.New(rand.NewSource(1))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Add(uint32(r.Int31n(10_000_000)))
	}
}

// Rendering an alert is O(violations), not O(N) — but only because Ids stops at
// the set bits. Worth measuring on a sparse set, which is the realistic case.
func BenchmarkIdsSparse(b *testing.B) {
	for _, n := range []uint32{1_000_000, 10_000_000} {
		s := New(n)
		for i := uint32(0); i < 100; i++ {
			s.Add(i * (n / 100))
		}
		b.Run(fmt.Sprintf("N=%d/violations=100", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if got := len(s.Ids()); got != 100 {
					b.Fatalf("Ids() = %d members", got)
				}
			}
		})
	}
}

// IsEmpty is the question a clean window asks, and clean windows are the common
// case. It must not cost what listing the members costs.
func BenchmarkIsEmptyOnCleanWindow(b *testing.B) {
	for _, n := range sizes {
		s := New(n)
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if !s.IsEmpty() {
					b.Fatal("expected empty")
				}
			}
		})
	}
}

// Memory, stated as a benchmark so it appears in the same report as the timings.
func BenchmarkMemoryPerWindow(b *testing.B) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				s := New(n)
				_ = s
			}
		})
	}
}
