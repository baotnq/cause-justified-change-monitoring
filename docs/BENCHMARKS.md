# Benchmarks — Part D

What Part D asks for, and what the numbers say. Everything here is reproducible
from a clean clone:

```
docker compose up -d
make bench
```

Measured on an Apple M1 (8 cores), Go 1.26, Redis 8.10.1, all processes on one
machine over loopback. Absolute figures will differ on your hardware; the shapes
— flat, linear, and where the cost actually sits — are the point.

A note on method. Loopback round trips here cost ~0.1 ms and jitter by more than
that between runs, so any benchmark shaped as *one sequential command per
iteration* measures the round trip and nothing else. The write-overhead numbers
below are therefore pipelined in batches of 100, which removes the round trip and
leaves the server's own work. The first attempt at that benchmark was not
pipelined, produced a 20% difference in one run and none in the next, and was
worthless.

---

## 1. Detection latency: write → alert

Detection latency has two terms, and only one of them is a property of this
design:

| Term | What it is | Measured |
|---|---|---|
| Pipeline latency | write lands → monitor knows about it | **p50 108 µs · p90 163 µs · p99 220 µs · max 466 µs** over 500 writes |
| Window latency | monitor knows → monitor decides | window length + grace, by configuration |

At 60-second windows the second term dominates by four orders of magnitude. That
is the trade this design makes on purpose: a check whose cost does not depend on
traffic, in exchange for waiting out a window. The tuning knob is Part A2b, not
the code — 10-second windows give ~10–20 s detection, 60-second windows give the
**1–3 minutes** the README quotes, hourly windows cost almost nothing to run.

Reproduce: `go test ./internal/channels -run PipelineLatency -v`

The pipeline figure also bounds something less obvious: if the feed ran seconds
behind, events would be stamped into the wrong window and the grace period would
stop absorbing skew. The test fails above a p99 of 100 ms for that reason, not
for performance.

---

## 2. Cost of the set operations vs N

The claim in A5 is `O(N/64)` per window regardless of how much traffic the window
saw. In Go, closing a window over the whole id space:

| Actors (N) | Difference (alloc) | Subtract (in place) | IsEmpty (clean window) | Memory per set |
|---|---|---|---|---|
| 10 000 | 251 ns | 105 ns | 62 ns | 1.25 KB |
| 100 000 | 3.5 µs | 1.0 µs | 522 ns | 13.3 KB |
| 1 000 000 | 19.4 µs | 10.7 µs | 5.8 µs | 128 KB |
| 10 000 000 | 234 µs | 128 µs | 51 µs | **1.25 MB** |

Linear in N and nothing else: no traffic term appears, because none exists. Ten
million actors cost a quarter of a millisecond and 1.25 MB per set per window —
the figure A3 predicts, confirmed by `TestMemoryFootprintMatchesTheClaim`.

Ingest is one bit per observed event: **5.4 ns**, no allocation.

Rendering an alert is `O(violations)`, not `O(N)`: listing 100 violations out of
10 M actors is 174 µs, and a clean window never pays it — `IsEmpty` answers in
51 µs, which is why the engine asks that question first.

Server-side, closing a window in Redis (pin both vectors, `BITOP NOT`,
`BITOP AND`, `BITCOUNT`, plus the round trips):

| Actors (N) | Window close |
|---|---|
| 10 000 | 1.2 ms |
| 100 000 | 1.4 ms |
| 1 000 000 | 2.8 ms |
| 10 000 000 | 12.6 ms |

Under a million actors the round trips dominate and the curve is flat; past that
the byte-wise work takes over. Either way, once per window per set is nothing
against a 60-second window. Ingest into Redis, pipelined: **~700 k events/sec**.

Reproduce: `make bench` or
`go test ./internal/bitset ./internal/redisbits -bench . -run XXX`

---

## 3. What the audited application pays

This is the number that matters most, because it is the one the design is
advertised on, and the honest answer is more interesting than "zero".

**The monitor adds no code to any request path.** No service calls it, it calls
no service, and no application code changes. That part is structural and holds by
construction.

**The change feed is not free.** Asking Redis to emit keyspace notifications
makes it build and publish a message on every write, and with a subscriber
attached that costs real CPU on the server. Pipelined `HSET` throughput against
one Redis, median of four runs:

| Configuration | Writes/sec | vs baseline |
|---|---|---|
| `notify-keyspace-events` off | ~507 000 | — |
| `KEA` + subscriber attached | ~290 000 | **−43%** |
| `Eh` + subscriber attached | ~366 000 | **−28%** |

Two things follow.

**Use the narrow flags.** `KEA` is what the documentation reaches for first and
it is the expensive choice: `K` and `E` together publish *two* messages per write
— one on the keyspace channel, one on the keyevent channel — and `A` enables
every class of event. The monitor reads only the keyevent channel, and only for
the commands in `channels.WriteEvents`. Narrowing to `Eh` (keyevent, hash class)
recovers about a third of the loss for nothing.

**Read the number in context.** This is a saturated single Redis with pipelined
writes — the ceiling, not an operating point. A system doing 5 000 writes/sec is
using 1% of that ceiling, and a 28% cut to a ceiling it never approaches is
invisible. The overhead becomes real only when the store is already the
bottleneck, and then the mitigations are the ones Part E already lists for other
reasons: take the feed from a replica, or use CDC from the durable store instead
of the cache.

So the accurate claim is **no code on the critical path, and a measurable cost to
the store's write ceiling** — quantified above, with a configuration change that
halves it. "Zero overhead" would be a nicer sentence and a false one.

---

## 4. False positives from window skew

Covered by test rather than benchmark, because the answer is structural:

- `TestWindowsAreAssignedByEventTime` — windows are assigned by event time, so a
  lagging consumer cannot manufacture violations out of its own slowness.
- `TestGraceHoldsTheWindowOpen` — a pair straddling a boundary completes inside
  grace and produces nothing.
- `TestEventAfterGraceIsReportedAsLate` — past grace the event is reported, not
  dropped; the operator learns the window's view was incomplete.

With grace at 10 s and pipeline p99 at 220 µs, the margin is four orders of
magnitude. The realistic sources of skew are producer clock drift and consumer
lag under load, not the feed itself.

---

## 5. Not measured

Stated so the gaps are not mistaken for results:

- **Multi-node Redis.** Everything here is one instance on loopback. Cluster mode
  changes `BITOP` — operands must hash to the same slot — which is a design
  question, not a tuning one.
- **Sustained load over hours.** These are microbenchmarks. Memory behaviour with
  thousands of open windows, and what happens when the audit falls behind and
  catches up, are not covered.
- **CDC as the change channel.** Only the Redis keyspace feed is implemented and
  measured. A Postgres logical-replication feed carries commit timestamps and
  values, and would move both the latency and the overhead numbers.
- **Amount conservation at scale.** The conservation layer keeps a map entry per
  active actor per window rather than a bit, so its memory is `O(active actors)`,
  not `O(N/8)`. Not benchmarked.
