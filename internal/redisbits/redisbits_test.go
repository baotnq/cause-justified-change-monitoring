package redisbits

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// These tests need a real Redis, because the behaviour under test is Redis's
// own, not ours: how BITOP sizes its output. A reimplementation would prove
// nothing.
//
//	docker compose up -d redis
//	REDIS_ADDR=127.0.0.1:6379 go test ./internal/redisbits
func dial(t *testing.T) (*redis.Client, func()) {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set; skipping Redis integration test")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis at %s: %v", addr, err)
	}
	return rdb, func() { _ = rdb.Close() }
}

var windowStart = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func newStore(t *testing.T, rdb *redis.Client, capacity uint32) *Store {
	t.Helper()
	prefix := fmt.Sprintf("test:%s:%d", t.Name(), time.Now().UnixNano())
	s := New(rdb, prefix, capacity)
	t.Cleanup(func() { _ = s.DropWindow(context.Background(), windowStart) })
	return s
}

func TestDifferenceIsTheViolationSet(t *testing.T) {
	rdb, done := dial(t)
	defer done()
	ctx := context.Background()

	s := newStore(t, rdb, 4096)
	for _, id := range []uint32{3, 7, 42} {
		if err := s.Add(ctx, SetChanged, windowStart, id); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []uint32{3, 42, 900} {
		if err := s.Add(ctx, SetJustified, windowStart, id); err != nil {
			t.Fatal(err)
		}
	}

	got, err := s.Difference(ctx, SetChanged, SetJustified, windowStart)
	if err != nil {
		t.Fatal(err)
	}
	if want := []uint32{7}; !reflect.DeepEqual(got, want) {
		t.Fatalf("violations = %v, want %v", got, want)
	}
}

func TestCleanWindowReturnsNothing(t *testing.T) {
	rdb, done := dial(t)
	defer done()
	ctx := context.Background()

	s := newStore(t, rdb, 4096)
	for _, id := range []uint32{1, 2, 3} {
		if err := s.Add(ctx, SetChanged, windowStart, id); err != nil {
			t.Fatal(err)
		}
		if err := s.Add(ctx, SetJustified, windowStart, id); err != nil {
			t.Fatal(err)
		}
	}

	got, err := s.Difference(ctx, SetChanged, SetJustified, windowStart)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("clean window reported %v", got)
	}
}

// The Part B1 pitfall, demonstrated against the real server rather than
// asserted in prose.
//
// One actor tampered with (id 4000) and one ordinary justified actor (id 10).
// The justified bitmap therefore ends at byte 1, the changed bitmap at byte
// 500. With capacity pinned the answer is {4000}. With pinning off — the naive
// implementation — Redis returns an empty set and the audit reports a clean
// window.
func TestUnpinnedCapacityLosesHighIdViolations(t *testing.T) {
	rdb, done := dial(t)
	defer done()
	ctx := context.Background()

	const capacity = 8192
	const tampered = 4000

	seed := func(s *Store) {
		for _, id := range []uint32{10, tampered} {
			if err := s.Add(ctx, SetChanged, windowStart, id); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.Add(ctx, SetJustified, windowStart, 10); err != nil {
			t.Fatal(err)
		}
	}

	naive := newStore(t, rdb, capacity)
	naive.PinCapacity = false
	seed(naive)

	lost, err := naive.Difference(ctx, SetChanged, SetJustified, windowStart)
	if err != nil {
		t.Fatal(err)
	}
	if len(lost) != 0 {
		t.Fatalf("unpinned difference returned %v; this test documents that Redis loses "+
			"the high-id violation here. If Redis changed its BITOP semantics, update Part B1.", lost)
	}
	t.Logf("unpinned: violation %d was silently dropped — window looked clean", tampered)

	pinned := newStore(t, rdb, capacity)
	seed(pinned)

	got, err := pinned.Difference(ctx, SetChanged, SetJustified, windowStart)
	if err != nil {
		t.Fatal(err)
	}
	if want := []uint32{tampered}; !reflect.DeepEqual(got, want) {
		t.Fatalf("pinned difference = %v, want %v", got, want)
	}
}

// The lengths behind that behaviour, read straight off the server, so the
// explanation in Part B1 is checkable and not just plausible.
func TestBitmapLengthsExplainTheLoss(t *testing.T) {
	rdb, done := dial(t)
	defer done()
	ctx := context.Background()

	s := newStore(t, rdb, 8192)
	s.PinCapacity = false
	if err := s.Add(ctx, SetChanged, windowStart, 4000); err != nil {
		t.Fatal(err)
	}
	if err := s.Add(ctx, SetJustified, windowStart, 10); err != nil {
		t.Fatal(err)
	}

	changedLen, err := rdb.StrLen(ctx, s.Key(SetChanged, windowStart)).Result()
	if err != nil {
		t.Fatal(err)
	}
	justifiedLen, err := rdb.StrLen(ctx, s.Key(SetJustified, windowStart)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if justifiedLen >= changedLen {
		t.Fatalf("test premise broken: justified %d bytes, changed %d bytes", justifiedLen, changedLen)
	}
	t.Logf("changed = %d bytes (highest id 4000), justified = %d bytes (highest id 10)", changedLen, justifiedLen)

	// BITOP NOT inherits the operand's length...
	notKey := s.Key("tmp-not", windowStart)
	defer rdb.Del(ctx, notKey)
	if err := rdb.BitOpNot(ctx, notKey, s.Key(SetJustified, windowStart)).Err(); err != nil {
		t.Fatal(err)
	}
	notLen, err := rdb.StrLen(ctx, notKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	if notLen != justifiedLen {
		t.Fatalf("BITOP NOT produced %d bytes from a %d-byte operand", notLen, justifiedLen)
	}

	// ...so the ones that should mask in the high bytes simply are not there,
	// and BITOP AND pads with zeros instead.
	if notLen >= changedLen {
		t.Fatalf("premise broken: inverted vector %d bytes, minuend %d bytes", notLen, changedLen)
	}
	t.Logf("NOT(justified) = %d bytes; the %d high bytes of changed get ANDed against padding zeros",
		notLen, changedLen-notLen)
}

func TestPinMakesEverySetTheSameLength(t *testing.T) {
	rdb, done := dial(t)
	defer done()
	ctx := context.Background()

	const capacity = 8192
	s := newStore(t, rdb, capacity)
	if err := s.Add(ctx, SetChanged, windowStart, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.Pin(ctx, SetChanged, windowStart); err != nil {
		t.Fatal(err)
	}
	n, err := rdb.StrLen(ctx, s.Key(SetChanged, windowStart)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(capacity / 8); n != want {
		t.Fatalf("pinned bitmap = %d bytes, want %d", n, want)
	}
}

func TestAddRejectsIdsOutsideCapacity(t *testing.T) {
	rdb, done := dial(t)
	defer done()

	s := newStore(t, rdb, 1024)
	if err := s.Add(context.Background(), SetChanged, windowStart, 1024); err == nil {
		t.Fatal("an id outside the id space was accepted; it would be invisible to every later difference")
	}
}

func TestLoadRoundTripsAgainstTheServer(t *testing.T) {
	rdb, done := dial(t)
	defer done()
	ctx := context.Background()

	s := newStore(t, rdb, 4096)
	want := []uint32{0, 1, 64, 4095}
	for _, id := range want {
		if err := s.Add(ctx, SetChanged, windowStart, id); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.Load(ctx, SetChanged, windowStart)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Ids(), want) {
		t.Fatalf("round trip = %v, want %v", got.Ids(), want)
	}
}
