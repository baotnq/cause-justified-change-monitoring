package channels

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"

	"github.com/baotnq/cause-justified-change-monitoring/internal/monitor"
)

// Both channels are tested against the real servers. Their delivery semantics
// are the thing under test, so a fake would be testing the fake.
//
//	docker compose up -d redis nats
//	REDIS_ADDR=127.0.0.1:6379 NATS_URL=nats://127.0.0.1:4222 go test ./internal/channels

func dialRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func dialNats(t *testing.T) *nats.Conn {
	t.Helper()
	url := os.Getenv("NATS_URL")
	if url == "" {
		t.Skip("NATS_URL not set")
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("nats: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func TestKeyspaceFeedSeesWritesThatBypassEveryService(t *testing.T) {
	rdb := dialRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	prefix := fmt.Sprintf("test:asset:account:%d:", time.Now().UnixNano())
	feed := NewKeyspaceFeed(rdb, 0, prefix)
	if err := feed.CheckConfig(ctx); err != nil {
		t.Skipf("redis is not emitting keyevent notifications: %v", err)
	}

	out := make(chan monitor.ChangeEvent, 8)
	go func() { _ = feed.Run(ctx, out) }()
	time.Sleep(200 * time.Millisecond) // let the subscription settle

	// The scenario-2 write: straight into the store, no service involved.
	key := prefix + "u-mallory"
	t.Cleanup(func() { rdb.Del(context.Background(), key) })
	if err := rdb.HSet(ctx, key, "balance", 1_000_000).Err(); err != nil {
		t.Fatal(err)
	}

	select {
	case ev := <-out:
		if ev.Actor != "u-mallory" {
			t.Fatalf("actor = %q, want u-mallory", ev.Actor)
		}
		if ev.Source == "" {
			t.Fatal("change event carries no source")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no change event within 5s — the store was written and the audit never heard about it")
	}
}

func TestKeyspaceFeedIgnoresKeysOutsideThePrefix(t *testing.T) {
	rdb := dialRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	prefix := fmt.Sprintf("test:asset:account:%d:", time.Now().UnixNano())
	feed := NewKeyspaceFeed(rdb, 0, prefix)
	if err := feed.CheckConfig(ctx); err != nil {
		t.Skipf("redis is not emitting keyevent notifications: %v", err)
	}

	out := make(chan monitor.ChangeEvent, 8)
	go func() { _ = feed.Run(ctx, out) }()
	time.Sleep(200 * time.Millisecond)

	other := fmt.Sprintf("test:unrelated:%d", time.Now().UnixNano())
	t.Cleanup(func() { rdb.Del(context.Background(), other) })
	if err := rdb.Set(ctx, other, "x", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}

	// Then a protected key, so the test does not merely time out.
	key := prefix + "u-alice"
	t.Cleanup(func() { rdb.Del(context.Background(), key) })
	if err := rdb.HSet(ctx, key, "balance", 5).Err(); err != nil {
		t.Fatal(err)
	}

	select {
	case ev := <-out:
		if ev.Actor != "u-alice" {
			t.Fatalf("first event = %q; an unprotected key leaked into the change channel", ev.Actor)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no change event within 5s")
	}
}

// A monitor subscribed to a channel nobody publishes on looks exactly like a
// system where nothing bad ever happens.
func TestCheckConfigRejectsSilentServer(t *testing.T) {
	rdb := dialRedis(t)
	ctx := context.Background()

	before, err := rdb.ConfigGet(ctx, "notify-keyspace-events").Result()
	if err != nil {
		t.Fatal(err)
	}
	original := before["notify-keyspace-events"]
	t.Cleanup(func() { _ = rdb.ConfigSet(ctx, "notify-keyspace-events", original).Err() })

	if err := rdb.ConfigSet(ctx, "notify-keyspace-events", "").Err(); err != nil {
		t.Skipf("cannot change notify-keyspace-events on this server: %v", err)
	}
	feed := NewKeyspaceFeed(rdb, 0, "test:")
	if err := feed.CheckConfig(ctx); err == nil {
		t.Fatal("CheckConfig accepted a server with notifications switched off")
	}

	if err := rdb.ConfigSet(ctx, "notify-keyspace-events", "KEA").Err(); err != nil {
		t.Fatal(err)
	}
	if err := feed.CheckConfig(ctx); err != nil {
		t.Fatalf("CheckConfig rejected a correctly configured server: %v", err)
	}
}

func TestCauseBusDeliversEvents(t *testing.T) {
	nc := dialNats(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	subject := fmt.Sprintf("test.cause.%d.>", time.Now().UnixNano())
	bus := NewCauseBus(nc, subject)

	out := make(chan monitor.CauseEvent, 8)
	go func() { _ = bus.Run(ctx, out) }()
	time.Sleep(200 * time.Millisecond)

	ts := time.Date(2026, 1, 1, 12, 0, 30, 0, time.UTC)
	err := Publish(nc, subject[:len(subject)-1]+"deposit", CauseMessage{
		Actor: "u-alice", TS: ts, Kind: "deposit.credited",
		Producer: "payment-service", Amount: 100, Ref: "dep-1",
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case ev := <-out:
		if ev.Actor != "u-alice" || ev.Kind != "deposit.credited" || ev.Producer != "payment-service" {
			t.Fatalf("event = %+v", ev)
		}
		if !ev.TS.Equal(ts) {
			t.Fatalf("ts = %s, want the producer's timestamp %s — arrival time would shift windows under lag", ev.TS, ts)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no cause event within 5s")
	}
}

func TestCauseBusReportsMalformedMessagesInsteadOfDroppingThem(t *testing.T) {
	nc := dialNats(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	subject := fmt.Sprintf("test.cause.%d", time.Now().UnixNano())
	bad := make(chan error, 4)
	bus := NewCauseBus(nc, subject)
	bus.OnMalformed = func(_ string, _ []byte, err error) { bad <- err }

	out := make(chan monitor.CauseEvent, 8)
	go func() { _ = bus.Run(ctx, out) }()
	time.Sleep(200 * time.Millisecond)

	if err := nc.Publish(subject, []byte("{not json")); err != nil {
		t.Fatal(err)
	}
	// A well-formed message with no actor justifies nothing either.
	noActor, _ := json.Marshal(CauseMessage{Kind: "deposit.credited", Producer: "payment-service"})
	if err := nc.Publish(subject, noActor); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		select {
		case <-bad:
		case <-time.After(5 * time.Second):
			t.Fatalf("malformed message %d was dropped silently", i+1)
		}
	}
	select {
	case ev := <-out:
		t.Fatalf("a malformed message produced a cause event: %+v", ev)
	default:
	}
}
