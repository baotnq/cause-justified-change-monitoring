// Command scenario runs the Part C scenarios against real infrastructure.
//
// The tests in internal/monitor prove the engine's logic in-process. This proves
// the wiring: an ordinary Redis write really does surface on the keyspace feed,
// a NATS message really does arrive as a cause, and the two really do meet in
// the same window. It writes to Redis exactly the way an attacker with a shell
// would, and publishes to NATS exactly the way a service would.
//
// Exit code is 0 only if every expectation held.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"

	"github.com/baotnq/cause-justified-change-monitoring/internal/channels"
	"github.com/baotnq/cause-justified-change-monitoring/internal/idmap"
	"github.com/baotnq/cause-justified-change-monitoring/internal/monitor"
	"github.com/baotnq/cause-justified-change-monitoring/internal/report"
)

type expectation struct {
	scenario string
	actor    string
	alert    string
	note     string
}

func main() {
	var (
		redisAddr = flag.String("redis", env("REDIS_ADDR", "127.0.0.1:6379"), "Redis address")
		natsURL   = flag.String("nats", env("NATS_URL", nats.DefaultURL), "NATS URL")
		window    = flag.Duration("window", 5*time.Second, "window length for the demo")
		grace     = flag.Duration("grace", 2*time.Second, "grace period")
	)
	flag.Parse()

	if err := run(*redisAddr, *natsURL, *window, *grace); err != nil {
		log.Fatalf("scenario: %v", err)
	}
}

func run(redisAddr, natsURL string, window, grace time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis %s: %w", redisAddr, err)
	}

	run := time.Now().UnixNano()
	keyPrefix := fmt.Sprintf("demo:%d:asset:account:", run)
	subject := fmt.Sprintf("demo.%d.cause", run)

	feed := channels.NewKeyspaceFeed(rdb, 0, keyPrefix)
	if err := feed.CheckConfig(ctx); err != nil {
		return err
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		return fmt.Errorf("nats %s: %w", natsURL, err)
	}
	defer nc.Close()

	cfg := monitor.Config{
		WindowSize:        window,
		Grace:             grace,
		Capacity:          1 << 16,
		TrustedProducers:  map[string]bool{"payment-service": true, "matching-service": true, "asset-service": true, "plan-approver": true},
		DerivedCauseKinds: map[string]bool{"asset.account.updated": true},
		RootCauseKinds:    map[string]bool{"deposit.credited": true, "matching.order.matched": true, "transfer.completed": true},
	}
	m := monitor.New(cfg, idmap.New())

	changes := make(chan monitor.ChangeEvent, 1024)
	causes := make(chan monitor.CauseEvent, 1024)
	go func() { _ = feed.Run(ctx, changes) }()
	go func() { _ = channels.NewCauseBus(nc, subject).Run(ctx, causes) }()

	var collected []monitor.Alert
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(200 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-changes:
				m.ObserveChange(ev)
			case ev := <-causes:
				m.ObserveCause(ev)
			case now := <-tick.C:
				collected = append(collected, m.CloseDue(now)...)
			}
		}
	}()

	time.Sleep(300 * time.Millisecond) // let both subscriptions settle

	cause := func(actor, kind, producer string, amount int64) error {
		return channels.Publish(nc, subject, channels.CauseMessage{
			Actor: actor, TS: time.Now(), Kind: kind, Producer: producer, Amount: amount, Ref: "ref-" + actor,
		})
	}
	write := func(actor string, balance int64) error {
		key := keyPrefix + actor
		return rdb.HSet(ctx, key, "balance", balance).Err()
	}

	log.Printf("driving scenarios against redis %s and nats %s", redisAddr, natsURL)

	// 1 — normal traffic: a service credits a deposit and publishes its cause.
	if err := cause("u-alice", "deposit.credited", "payment-service", 100); err != nil {
		return err
	}
	if err := write("u-alice", 100); err != nil {
		return err
	}

	// 2 — direct tamper: a write with no service behind it.
	if err := write("u-mallory", 1_000_000); err != nil {
		return err
	}

	// 3b — silenced change feed: a cause with no write observed.
	if err := cause("u-dave", "deposit.credited", "payment-service", 500); err != nil {
		return err
	}

	// 4 — forged justification: the attacker supplies the missing cause, but a
	// bare asset.account.updated is derived and has no root cause behind it.
	if err := write("u-eve", 999_999); err != nil {
		return err
	}
	if err := cause("u-eve", "asset.account.updated", "asset-service", 999_999); err != nil {
		return err
	}

	// 6 — an agent authorising itself. Producer identity rejects it.
	if err := write("agent-7-step-2", 1); err != nil {
		return err
	}
	if err := cause("agent-7-step-2", "plan.step.approved", "agent-7", 0); err != nil {
		return err
	}

	if err := nc.Flush(); err != nil {
		return err
	}

	// Wait for the window to close, plus grace, plus slack for delivery.
	wait := window + grace + 2*time.Second
	log.Printf("waiting %s for the window to close", wait)
	time.Sleep(wait)

	cancel()
	<-done
	collected = append(collected, m.CloseAll()...)

	// Keys are demo-scoped; clean up so a repeated run starts fresh.
	cleanup(rdb, keyPrefix)

	return check(collected, []expectation{
		{"1  normal traffic", "u-alice", "", "deposit with its cause: no alert"},
		{"2  direct tamper", "u-mallory", monitor.AlertUnauthorizedChange, "HSET straight into the store, no service involved"},
		{"3b silenced feed", "u-dave", monitor.AlertMissingChange, "authorized cause, no write observed"},
		{"4  forged cause", "u-eve", monitor.AlertForgedCause, "derived cause with no root cause behind it"},
		{"6  agent self-auth", "agent-7-step-2", monitor.AlertUntrustedProducer, "agent published its own authorization"},
		{"6  agent effect", "agent-7-step-2", monitor.AlertUnauthorizedChange, "effect with no approved step"},
	})
}

func check(alerts []monitor.Alert, want []expectation) error {
	type key struct{ actor, alert string }
	got := map[key]monitor.Alert{}
	for _, a := range alerts {
		for _, actor := range a.ActorIDs {
			got[key{actor, a.Type}] = a
		}
	}

	fmt.Println()
	var failures []string
	for _, w := range want {
		if w.alert == "" {
			var noisy []string
			for k := range got {
				if k.actor == w.actor {
					noisy = append(noisy, k.alert)
				}
			}
			if len(noisy) > 0 {
				sort.Strings(noisy)
				failures = append(failures, fmt.Sprintf("%s: expected silence for %s, got %s", w.scenario, w.actor, strings.Join(noisy, ", ")))
				fmt.Printf("  FAIL  %-20s %-16s %s\n", w.scenario, w.actor, strings.Join(noisy, ", "))
			} else {
				fmt.Printf("  ok    %-20s %-16s %s\n", w.scenario, w.actor, w.note)
			}
			continue
		}
		if _, ok := got[key{w.actor, w.alert}]; !ok {
			failures = append(failures, fmt.Sprintf("%s: no %s for %s", w.scenario, w.alert, w.actor))
			fmt.Printf("  FAIL  %-20s %-16s expected %s — %s\n", w.scenario, w.actor, w.alert, w.note)
			continue
		}
		fmt.Printf("  ok    %-20s %-16s %s — %s\n", w.scenario, w.actor, w.alert, w.note)
	}
	fmt.Println()

	// Every alert also goes through the hash chain, so the run leaves an
	// evidence trail that can be replayed rather than just printed.
	var chain bytes.Buffer
	w := report.NewWriter(&chain)
	for _, a := range alerts {
		if _, err := w.Append(a); err != nil {
			return err
		}
	}
	entries, head, err := report.Verify(bytes.NewReader(chain.Bytes()))
	if err != nil {
		return fmt.Errorf("report chain does not verify: %w", err)
	}
	fmt.Printf("  %d alerts, hash chain verified, head %s\n\n", len(entries), head[:16])

	if len(failures) > 0 {
		for _, f := range failures {
			fmt.Fprintf(os.Stderr, "FAIL: %s\n", f)
		}
		return fmt.Errorf("%d of %d expectations failed", len(failures), len(want))
	}

	if os.Getenv("SCENARIO_DUMP") != "" {
		out, _ := json.MarshalIndent(alerts, "", "  ")
		fmt.Println(string(out))
	}
	return nil
}

func cleanup(rdb *redis.Client, prefix string) {
	ctx := context.Background()
	iter := rdb.Scan(ctx, 0, prefix+"*", 100).Iterator()
	var keys []string
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if len(keys) > 0 {
		rdb.Del(ctx, keys...)
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
