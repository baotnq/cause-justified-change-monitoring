package redisbits

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Part D against the real server:
//
//   - what closing a window costs in Redis as N grows, and
//   - what the audited application pays for being observed — the claim is
//     nothing, and this is where that claim is either kept or not.
//
//	REDIS_ADDR=127.0.0.1:6379 go test ./internal/redisbits -bench . -benchtime 200ms -run XXX

func benchClient(b *testing.B) *redis.Client {
	b.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		b.Skip("REDIS_ADDR not set")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		b.Fatalf("redis: %v", err)
	}
	b.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// Closing a window: pin both vectors, BITOP NOT, BITOP AND, BITCOUNT. The set
// algebra runs server-side, so this measures Redis, not the client.
func BenchmarkWindowClose(b *testing.B) {
	rdb := benchClient(b)
	ctx := context.Background()

	for _, n := range []uint32{10_000, 100_000, 1_000_000, 10_000_000} {
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			prefix := fmt.Sprintf("bench:%d:%d", n, time.Now().UnixNano())
			s := New(rdb, prefix, n)
			w := time.Unix(0, 0)
			b.Cleanup(func() { _ = s.DropWindow(context.Background(), w) })

			// 1% of actors changed, slightly more had a cause.
			r := rand.New(rand.NewSource(int64(n)))
			pipe := rdb.Pipeline()
			for i := 0; i < int(n)/100; i++ {
				pipe.SetBit(ctx, s.Key(SetChanged, w), int64(r.Int31n(int32(n))), 1)
			}
			r2 := rand.New(rand.NewSource(int64(n) + 7))
			for i := 0; i < int(n)/80; i++ {
				pipe.SetBit(ctx, s.Key(SetJustified, w), int64(r2.Int31n(int32(n))), 1)
			}
			if _, err := pipe.Exec(ctx); err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := s.Difference(ctx, SetChanged, SetJustified, w); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Ingest: one SETBIT per observed event, pipelined the way a real feed would.
func BenchmarkIngestPipelined(b *testing.B) {
	rdb := benchClient(b)
	ctx := context.Background()

	const n = 10_000_000
	prefix := fmt.Sprintf("bench:ingest:%d", time.Now().UnixNano())
	s := New(rdb, prefix, n)
	w := time.Unix(0, 0)
	b.Cleanup(func() { _ = s.DropWindow(context.Background(), w) })

	const batch = 1000
	r := rand.New(rand.NewSource(1))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		pipe := rdb.Pipeline()
		for j := 0; j < batch; j++ {
			pipe.SetBit(ctx, s.Key(SetChanged, w), int64(r.Int31n(n)), 1)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N*batch)/b.Elapsed().Seconds(), "events/sec")
}

// What the audited application pays. The monitor never sits in a request path,
// but it does ask Redis to emit keyspace notifications, and that is not free by
// definition — it is only free in practice. So measure it: the same HSET, with
// notifications off, then on with a subscriber attached.
//
// Reported side by side; the difference is the whole cost of being observed.
func BenchmarkApplicationWriteOverhead(b *testing.B) {
	rdb := benchClient(b)
	ctx := context.Background()

	before, err := rdb.ConfigGet(ctx, "notify-keyspace-events").Result()
	if err != nil {
		b.Fatal(err)
	}
	original := before["notify-keyspace-events"]
	b.Cleanup(func() { _ = rdb.ConfigSet(context.Background(), "notify-keyspace-events", original).Err() })

	key := fmt.Sprintf("bench:asset:account:%d", time.Now().UnixNano())
	b.Cleanup(func() { rdb.Del(context.Background(), key) })

	// Writes are pipelined in batches. A single sequential HSET on loopback is
	// dominated by the round trip — around 0.1 ms here, with run-to-run jitter
	// larger than any effect being measured — so that shape of benchmark cannot
	// answer the question. Batching removes the round trip and leaves the
	// server's own work, which is what the notification actually adds to.
	const batch = 100
	write := func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			pipe := rdb.Pipeline()
			for j := 0; j < batch; j++ {
				pipe.HSet(ctx, key, "balance", i*batch+j)
			}
			if _, err := pipe.Exec(ctx); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(b.N*batch)/b.Elapsed().Seconds(), "writes/sec")
	}

	b.Run("notifications=off", func(b *testing.B) {
		if err := rdb.ConfigSet(ctx, "notify-keyspace-events", "").Err(); err != nil {
			b.Skipf("cannot change notify-keyspace-events: %v", err)
		}
		write(b)
	})

	// KEA is what the documentation reaches for first, and it is the expensive
	// choice: K and E together make Redis publish two messages per write, one
	// per keyspace channel and one per keyevent channel, and A enables every
	// class of event. The monitor only ever reads the keyevent channel, and only
	// for the commands in WriteEvents, so the narrow flags are enough.
	withSubscriber := func(b *testing.B, flags string) {
		if err := rdb.ConfigSet(ctx, "notify-keyspace-events", flags).Err(); err != nil {
			b.Skipf("cannot change notify-keyspace-events: %v", err)
		}
		subCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		sub := rdb.PSubscribe(subCtx, "__keyevent@0__:*")
		defer sub.Close()
		if _, err := sub.Receive(subCtx); err != nil {
			b.Fatal(err)
		}
		ch := sub.Channel()
		go func() {
			for {
				select {
				case <-subCtx.Done():
					return
				case <-ch: // drain, as the audit would
				}
			}
		}()
		time.Sleep(100 * time.Millisecond)
		write(b)
	}

	b.Run("notifications=KEA,subscriber attached", func(b *testing.B) {
		withSubscriber(b, "KEA")
	})

	b.Run("notifications=Eh,subscriber attached", func(b *testing.B) {
		withSubscriber(b, "Eh")
	})
}
