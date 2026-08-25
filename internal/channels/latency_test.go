package channels

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/baotnq/cause-justified-change-monitoring/internal/monitor"
)

// Part D, first question: detection latency, write → alert.
//
// It has two parts that are worth keeping separate, because only one of them is
// a property of this design:
//
//   - pipeline latency — how long after a write the monitor knows about it. That
//     is what this measures.
//   - window latency — how long the monitor then waits before deciding, which is
//     the window length plus grace, chosen by configuration.
//
// Detection latency is the sum. At 60-second windows the second term dominates
// completely, which is why the README quotes 1–3 minutes and not microseconds:
// the design trades detection speed for a check that costs the same whatever the
// traffic, and the tuning knob is Part A2b, not the code.
func TestPipelineLatency(t *testing.T) {
	rdb := dialRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	prefix := fmt.Sprintf("test:latency:%d:", time.Now().UnixNano())
	feed := NewKeyspaceFeed(rdb, 0, prefix)
	if err := feed.CheckConfig(ctx); err != nil {
		t.Skipf("redis is not emitting keyevent notifications: %v", err)
	}

	out := make(chan monitor.ChangeEvent, 4096)
	go func() { _ = feed.Run(ctx, out) }()
	time.Sleep(300 * time.Millisecond)

	const n = 500
	samples := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("%su-%d", prefix, i)
		start := time.Now()
		if err := rdb.HSet(ctx, key, "balance", i).Err(); err != nil {
			t.Fatal(err)
		}
		select {
		case <-out:
			samples = append(samples, time.Since(start))
		case <-time.After(5 * time.Second):
			t.Fatalf("write %d never reached the change channel", i)
		}
		t.Cleanup(func() { rdb.Del(context.Background(), key) })
	}

	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	p := func(q float64) time.Duration { return samples[int(float64(len(samples)-1)*q)] }
	t.Logf("pipeline latency over %d writes: p50 %s  p90 %s  p99 %s  max %s",
		len(samples), p(0.50), p(0.90), p(0.99), samples[len(samples)-1])

	// A sanity bound, not a performance target: if the feed were seconds behind,
	// events would land in the wrong window and grace would stop absorbing skew.
	if p(0.99) > 100*time.Millisecond {
		t.Fatalf("p99 pipeline latency %s is too high for window assignment to be safe", p(0.99))
	}
}
