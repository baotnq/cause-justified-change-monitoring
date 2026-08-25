// Package redisbits is the Redis realization of Part B: the window's membership
// sets live as Redis bitmaps, and the set difference is computed by the server
// with BITOP rather than by shipping ids to the client.
//
// It exists mostly to make one thing concrete. The abstract method says
// "difference of two sets". Redis will happily compute something that is not
// that difference, silently, and in the direction that loses violations. See
// PinCapacity and the tests.
package redisbits

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/baotnq/cause-justified-change-monitoring/internal/bitset"
)

// Set names used by the monitor. They are just key suffixes.
const (
	SetChanged   = "changed"
	SetJustified = "justified"
	SetDerived   = "derived"
	SetRooted    = "rooted"
)

// Store holds one window's bitmaps in Redis.
type Store struct {
	rdb      redis.Cmdable
	prefix   string
	capacity uint32

	// PinCapacity makes every bitmap exactly capacity/8 bytes before any BITOP
	// touches it. Leave it on.
	//
	// Redis sizes a bitmap by its highest set bit, and the two BITOP variants
	// disagree about what to do with the difference:
	//
	//   BITOP NOT dst src   → dst is exactly as long as src
	//   BITOP AND dst a b   → shorter operands are zero-padded to the longest
	//
	// So in `changed AND NOT justified`, if the highest justified id is lower
	// than the highest changed id, the inverted vector is short, the padding
	// supplies zeros where it should supply ones, and every violating actor
	// above that id is ANDed out of the answer. The monitor then reports a clean
	// window. The failure is silent, and it lands precisely on the actors the
	// monitor exists to catch.
	//
	// Pinning costs one SETBIT per set per window and makes the algebra total.
	PinCapacity bool

	// TTL bounds how long window bitmaps and scratch keys survive. Windows are
	// closed and reported long before this; the TTL is only so a crashed audit
	// does not leak keys forever.
	TTL time.Duration
}

// New returns a store writing keys under prefix, over an id space of capacity.
func New(rdb redis.Cmdable, prefix string, capacity uint32) *Store {
	return &Store{
		rdb:         rdb,
		prefix:      prefix,
		capacity:    capacity,
		PinCapacity: true,
		TTL:         6 * time.Hour,
	}
}

// Key is the bitmap key for one set in one window.
func (s *Store) Key(set string, windowStart time.Time) string {
	return fmt.Sprintf("%s:%s:%d", s.prefix, set, windowStart.UTC().Unix())
}

// Pin fixes the byte length of a set's bitmap to capacity/8 by writing a zero
// at the last addressable bit. Idempotent, and safe to call before or after the
// window has members.
func (s *Store) Pin(ctx context.Context, set string, windowStart time.Time) error {
	key := s.Key(set, windowStart)
	if err := s.rdb.SetBit(ctx, key, int64(s.capacity)-1, 0).Err(); err != nil {
		return fmt.Errorf("pin %s: %w", key, err)
	}
	return s.expire(ctx, key)
}

// Add records membership of id in a set, for the given window.
func (s *Store) Add(ctx context.Context, set string, windowStart time.Time, id uint32) error {
	if id >= s.capacity {
		return fmt.Errorf("actor id %d is outside the configured capacity %d", id, s.capacity)
	}
	key := s.Key(set, windowStart)
	if err := s.rdb.SetBit(ctx, key, int64(id), 1).Err(); err != nil {
		return fmt.Errorf("setbit %s: %w", key, err)
	}
	return s.expire(ctx, key)
}

func (s *Store) expire(ctx context.Context, key string) error {
	if s.TTL <= 0 {
		return nil
	}
	return s.rdb.Expire(ctx, key, s.TTL).Err()
}

// Count is the number of members of a set.
func (s *Store) Count(ctx context.Context, set string, windowStart time.Time) (int64, error) {
	return s.rdb.BitCount(ctx, s.Key(set, windowStart), nil).Result()
}

// Difference computes minuend \ subtrahend for one window and returns the
// member ids. The set algebra runs inside Redis; only the answer crosses the
// wire, and only when the answer is non-empty.
func (s *Store) Difference(ctx context.Context, minuend, subtrahend string, windowStart time.Time) ([]uint32, error) {
	if s.PinCapacity {
		for _, set := range []string{minuend, subtrahend} {
			if err := s.Pin(ctx, set, windowStart); err != nil {
				return nil, err
			}
		}
	}

	a := s.Key(minuend, windowStart)
	b := s.Key(subtrahend, windowStart)
	notB := fmt.Sprintf("%s:tmp:not:%s:%d", s.prefix, subtrahend, windowStart.UTC().Unix())
	dst := fmt.Sprintf("%s:tmp:diff:%s_%s:%d", s.prefix, minuend, subtrahend, windowStart.UTC().Unix())
	defer s.rdb.Del(ctx, notB, dst)

	if err := s.rdb.BitOpNot(ctx, notB, b).Err(); err != nil {
		return nil, fmt.Errorf("bitop not %s: %w", b, err)
	}
	if err := s.rdb.BitOpAnd(ctx, dst, a, notB).Err(); err != nil {
		return nil, fmt.Errorf("bitop and: %w", err)
	}

	// Ask how many before asking which. A clean window — the overwhelmingly
	// common case — costs one integer, not capacity/8 bytes over the wire.
	n, err := s.rdb.BitCount(ctx, dst, nil).Result()
	if err != nil {
		return nil, fmt.Errorf("bitcount %s: %w", dst, err)
	}
	if n == 0 {
		return nil, nil
	}

	raw, err := s.rdb.Get(ctx, dst).Bytes()
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", dst, err)
	}
	return bitset.FromBytes(raw, s.capacity).Ids(), nil
}

// Load reads a set back as an in-memory bit vector, for tests and for the
// benchmark harness.
func (s *Store) Load(ctx context.Context, set string, windowStart time.Time) (*bitset.Set, error) {
	raw, err := s.rdb.Get(ctx, s.Key(set, windowStart)).Bytes()
	if err == redis.Nil {
		return bitset.New(s.capacity), nil
	}
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", s.Key(set, windowStart), err)
	}
	return bitset.FromBytes(raw, s.capacity), nil
}

// DropWindow deletes every key belonging to a window.
func (s *Store) DropWindow(ctx context.Context, windowStart time.Time) error {
	keys := make([]string, 0, 4)
	for _, set := range []string{SetChanged, SetJustified, SetDerived, SetRooted} {
		keys = append(keys, s.Key(set, windowStart))
	}
	return s.rdb.Del(ctx, keys...).Err()
}
