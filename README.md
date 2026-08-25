# Cause-Justified Change Monitoring

*A runtime-verification pattern for money-like state: every observed change must be justified by an authorized cause. Checked near-real-time with set algebra over compact bit vectors, on an observation channel independent of the application's write path. Presented first as an abstract method, then as one concrete realization on Redis.*

Status: reference implementation runs; scenarios and benchmarks reproduce from a clean clone (see **Running it** below). All examples are synthetic; no proprietary data or code.

## TL;DR

Money-like state should never move without an authorized cause. Watch the store's **own change feed** — a channel the application cannot influence, so it sees writes that bypass every service — watch the bus of **authorized business events**, and once per window subtract one set of actor ids from the other. Whatever is left changed with nothing to justify it.

The check is an exact set difference over bit vectors, not a heuristic or a sampled reconciliation: `O(N/64)` per window regardless of traffic, **1–3 minutes** from write to alert at 60-second windows. The monitor is subscribe-only — no code and no schema change in the audited system, nothing added to any request path, and it can be removed as easily as it was added, which is what makes it an *independent* control rather than one more feature of the same code base.

It is not free, though, and the measurements say where the cost sits: turning the change feed on costs the *store* about 28% of its pipelined write ceiling, which matters only if that ceiling is where you already are. Numbers, method and mitigations in [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

```
             writes, including any that bypass the application
                                    │
             ┌──────────────────────▼───────┐   ┌──────────────────────────────┐
             │  store change feed           │   │  authorized business events  │
             │  (CDC / keyspace notif.)     │   │  (signed bus, known producer)│
             └──────────────┬───────────────┘   └───────────────┬──────────────┘
                  Changed(w)│                       Justified(w)│
                            └──────────►  V(w) = Changed \ Justified  ◄──────────┘
                                                    │
                                    V(w) ≠ ∅  ──►  alert: state moved, no authorized cause
                                                    │
                                    final checkpoint (settlement / withdrawal)
                                    re-checks all parties, all legs atomic
```

*Written by Bao Trinh — MSc in formal verification (JAIST, lab of Prof. Kokichi Futatsugi; verified compiler in CafeOBJ, Springer LNCS 10795), 15+ years building correctness-critical systems: crypto exchange core (deterministic matching, atomic settlement, MPC custody), core banking and payments. This pattern is generalized from an audit module I designed for a spot exchange and reused on a second realtime, money-settled platform; re-derived here from the pattern, with no production data or code.*

## Running it

```sh
docker compose up -d          # redis, nats, postgres
make test                     # engine + Part C scenarios, no infrastructure needed
make test-all                 # adds the Redis / NATS / Postgres integration tests
make demo                     # drives the scenarios end to end against the real stack
make bench                    # Part D
```

`make demo` writes to Redis the way an attacker with a shell would, publishes to
NATS the way a service would, and prints what the monitor concluded:

```
  ok    1  normal traffic    u-alice          deposit with its cause: no alert
  ok    2  direct tamper     u-mallory        unauthorized_change — HSET straight into the store, no service involved
  ok    3b silenced feed     u-dave           missing_change — authorized cause, no write observed
  ok    4  forged cause      u-eve            forged_cause — derived cause with no root cause behind it
  ok    6  agent self-auth   agent-7-step-2   untrusted_producer — agent published its own authorization
  ok    6  agent effect      agent-7-step-2   unauthorized_change — effect with no approved step

  4 alerts, hash chain verified, head 1886a526f4dacf37
```

Layout: `internal/bitset` (the set algebra), `internal/idmap` (Part A4),
`internal/monitor` (the invariant engine, infrastructure-free),
`internal/checkpoint` (Part A6), `internal/redisbits` and `internal/channels`
(Part B), `internal/report` (hash-chained evidence), `internal/pgstore`
(history), `cmd/auditd` (the service), `cmd/scenario` (the demo).

---

## Part A — The abstract method

### A1. Model

- **Actors.** The subjects whose state we protect and whose actions we observe: users, admins, automated systems (bots, jobs). Each actor has a stable identity.
- **Protected state.** A per-actor quantity that must not change without cause (balance, stock level, permission set).
- **Windows.** Time is cut into windows `w` (sliding, with a grace period; comparisons use event timestamps, not arrival time).
- **Two observation channels, independent of each other:**
  - **Change channel** — reports *that* an actor's protected state was written, from the store itself (change feed / CDC). It sees every write, including writes that bypass the application.
  - **Cause channel** — reports authorized business events (deposit, order matched, transfer, disbursement approved…) from a trusted bus, with producer identity.

For each window define two sets of actor identities:

```
Changed(w)   = { a | change channel reports a write to state(a) during w }
Justified(w) = { a | cause channel reports an authorized event with subject a during w }
```

### A2. Invariant and violation set

Three symbols do all the work here, so they are worth stating plainly before they are used.

| | Read it as | |
|---|---|---|
| `A \ B` | **set difference** — the members of `A` that are **not** in `B` | Not division and not arithmetic subtraction. `{alice, mallory} \ {alice, dave}` is `{mallory}`. |
| `A ⊆ B` | **`A` is contained in `B`** — every member of `A` is also a member of `B` | `B` may hold more besides. Equivalent to saying `A \ B` is empty. |
| `A ∪ B` | **union** — everything in `A`, everything in `B` | Used once, in the nested invariant below. |
| `∀w` | **for every window `w`** | The property has to hold in each window separately, not on average. |

A worked window, to fix the idea. Three actors, one minute:

```
window 12:00:00 – 12:01:00

  Changed(w)   = { alice, mallory }      the store reported a write for these two
  Justified(w) = { alice, dave }         these two had an authorized business event

  Changed \ Justified = { mallory }      ← moved with nothing to justify it
  Justified \ Changed = { dave }         ← authorized, but no write was observed
```

`alice` appears in both and is of no further interest: her balance moved and there is a cause on record for it. The two leftovers are the whole output of the method — and they are **not** the same kind of finding, which is what the invariant is about.

```
Invariant:   ∀w.  Changed(w) ⊆ Justified(w)
Violations:  V(w) = Changed(w) \ Justified(w)
```

In words: *in every window, every actor whose state was written must also be an actor with an authorized cause.* `V(w)` is exact — a set difference, not a score, not a threshold, not a sampled reconciliation. A non-empty `V(w)` means someone's state moved with no authorized cause in that window: a bypass write, insider tampering, or a lost cause event (which is worth an alert of its own).

A **nested invariant** raises the bar against forged causes: the cause channel's own events must themselves have a deeper cause,

```
BalanceUpdated(w) ⊆ (Ordered ∪ Matched ∪ Transferred)(w)
```

so an attacker who can publish a fake `balance.updated` still cannot manufacture the order or transfer behind it.

Optionally, `Justified` can be split by actor class (users / admins / systems) to give class-specific policies (e.g. admin-caused changes are always flagged for review even if "authorized").

### A2a. Why containment, and not equality

The natural first reading of the invariant is that the two sets should simply be **equal** — every change has a cause, every cause has a change, so `Changed(w) = Justified(w)`. That is the wrong invariant, and getting it wrong is not a matter of taste. It decides whether the control survives contact with production.

Equality is a stronger claim, and the extra strength is all in the second direction: it also asserts that `Justified \ Changed` is empty — that an authorized cause is always matched by an observed write **inside the same window**. In a perfectly correct system, that is routinely false:

- **Window edges.** A service publishes `deposit.credited` at 11:59:59.9 and the write lands at 12:00:00.1. Two different windows. Grace (A2b) absorbs most of this and never all of it: whatever the grace period is, some event pair straddles the end of it.
- **Authorized events that legitimately move nothing.** A transfer that nets to zero across legs. An idempotent retry of an operation already applied. An order matched in this window whose settlement falls into the next. A correction that restores a previous value. All authorized, all real, none of them produce a write for that actor in that window.
- **A change channel that is late, lossy, or silenced.** Redis keyspace notifications are fire-and-forget pub/sub with no replay (see B, and `channels.KeyspaceFeed`). A momentary lag puts `dave` on one side of the comparison and not the other, through no fault of `dave`.

Under equality, every one of those becomes a **violation**, and the monitor starts accusing actors who did nothing. That failure is not cosmetic — it is the standard way this class of control dies.

**The danger of the false alert, in numbers.** Detection is not the hard part of security operations; *being believed* is. In the 2026 State of the SOC data, [46% of all alerts turn out to be false positives](https://www.stamus-networks.com/blog/what-the-2025-sans-detection-response-survey-reveals-false-positives-alert-fatigue-are-worsening), with enterprise rates frequently above 50% and reported as high as 80%; [73% of security teams name false positives as their single biggest detection challenge](https://www.stamus-networks.com/blog/what-the-2025-sans-detection-response-survey-reveals-false-positives-alert-fatigue-are-worsening), and analysts spend [over a quarter of their time working alerts that turn out to be nothing](https://www.dropzone.ai/glossary/alert-fatigue-in-cybersecurity-definition-causes-modern-solutions-5tz9b). In this pattern's own domain it is worse: [up to 95% of AML transaction-monitoring alerts are false positives](https://www.trapets.com/resources/blog/flagging-false-positives-in-aml-how-banks-can-reduce-98-wasted-alerts), which is why "we added another rule" is met with resignation rather than enthusiasm by the people who have to work the queue.

The consequence is not that the noisy alert is ignored. It is that the **real** one is. Target's FireEye deployment fired on the breach malware, repeatedly, [naming even the attackers' staging servers](https://www.scworld.com/news/target-did-not-respond-to-fireeye-security-alerts-prior-to-breach-according-to-report) — and [nobody acted for nearly three weeks](https://medium.com/@infosecguy_88900/the-target-breach-noisy-environments-alert-fatigue-and-the-challenge-of-connecting-the-dots-a7cc1f774204), because a genuinely dangerous alert looked exactly like the hundreds of harmless ones before it. Forty million payment cards. The detection worked; the credibility of the channel it arrived on did not.

So an alert nobody believes is worse than no alert at all: it costs the same to operate, and it buys a false sense of coverage. A monitor that accuses innocent actors twice a day will be muted within a month, and on the day it is right, the mute will still be in place.

Hence the asymmetry, which is a design decision and not an omission. Both differences are computed; they are named differently, mean different things, and go to different people:

| Computed | Alert | What it means | Who acts |
|---|---|---|---|
| `Changed \ Justified` | `unauthorized_change` | State moved with no authorized cause. An accusation. | Security — investigate the actor |
| `Justified \ Changed` | `missing_change` | An authorized cause with no observed write. Not an accusation. | Platform — investigate the feed |

The second direction still has to be watched, and for a sharp reason: **silencing the change channel is the cleanest way to hide from this monitor.** Switch off `notify-keyspace-events` and `Changed(w)` is empty, so `V(w)` is empty, and every window reports clean forever. `missing_change` is what makes that attack visible — the causes keep arriving while the writes stop being observed. It is the monitor watching its own eyes, not an accusation against anyone.

Stated as a rule the implementation follows throughout: **never make an accusation the evidence does not support.** The invariant is the direction where the evidence is conclusive — a write was observed and no authorization exists for it. The other direction is a question, and it is reported as one.

### A2b. Window length is a tuning knob, not a fixed constant

The invariant is checked per window, so the window length trades **detection latency** against **cost**:

- Short windows (e.g. 60 s, "realtime"): violations surface **1–3 minutes** after the write (one window to close plus the grace period); more windows to close, more bit vectors to keep, more chances of skew at window edges (mitigated by grace).
- Long windows (minutes to hours): fewer set operations and less memory; detection is slower; suitable as a second, cheaper tier over the same channels.

Both tiers can run at once (60 s for alerts, hourly for a consolidated report). Whatever the length, the monitor stays **off the critical path**: it only subscribes to the change and cause channels; the application never waits on it, and the cost of closing a window is `O(N/64)` regardless of traffic.

### A3. Data structure: bit vectors (bitmaps) over dense ids

Set operations on millions of actor ids per window must be cheap and exact. Represent each set as a **membership bit vector** indexed by a dense integer id:

- Size: `N/8` bytes for `N` actors (10 M actors ≈ 1.25 MB per set per window).
- Operations: union / intersection / difference are word-wise `OR / AND / AND-NOT`, i.e. `O(N/64)` per operation, cache-friendly, no joins, no hashing per element. (`XOR` computes `A \ B` only when `B ⊆ A`; that containment is the *opposite* of what this invariant guarantees, so the difference is always `AND-NOT` here — see B1.)
- Membership test and insertion are `O(1)`.

Alternatives and why not: hash sets (memory ~10–50× larger, per-element cost); Bloom filters (false positives are unacceptable for accusations); sorted id lists with merge (fine but slower and larger); compressed bitmaps (Roaring) are a good choice when ids are sparse — the same algebra applies.

### A4. Identity mapping (required)

Real systems use string identities (UUIDs, external ids). Bitmaps need dense integers. The method therefore includes an explicit **id map** `uuid ⇄ int32`:

- Bijective, append-only (ids are never reused), covers all actor classes (users, admins, systems).
- Assigned by a trusted component; stored with integrity protection (checksummed, replicated, periodically re-derived from the source of truth). If the map is corrupted, the monitor silently mis-attributes — this is the method's most sensitive component.
- Lookup on the hot path of the *audit* only (never on the application path); the change channel typically carries the string key, so mapping happens once per observed event.

### A5. Complexity and cost

Per window: `O(E)` to map and set bits for `E` observed events, plus `O(N/64)` for the set difference; memory `O(N/8)` per set. The application pays nothing: the monitor only subscribes.

### A6. Second layer — final checkpoint and failure audit

Where the domain has a final money-moving step (settlement, disbursement, withdrawal), the earlier decision (a match, an approved transfer) is *not* final:

- At settlement, re-check real balances and ban/blacklist state of **all** parties; succeed or fail **atomically for all legs**.
- **Failed settlements are data.** A burst of failures tied to one actor is a first-class risk signal (e.g. one taker with a forged balance sweeping many makers). Downstream actions (withdrawal) consult the audit before proceeding.

### A7. Independence: a bolt-on control, not a change to the system

The monitor is designed so it can be added to a running system without modifying it:

- **Separate process, subscribe-only.** It consumes the change channel and the cause channel; it never calls a service synchronously and no service calls it. Nothing is added to any request path.
- **No code or schema changes** in the audited services. Existing writes and existing events are enough; the store's change feed is already there.
- **Read-only credentials** on application data; its own keyspace for bit vectors and windows.
- **Fail-open for the business, fail-loud for operators.** If the monitor is down or lagging, trading/payment continues; the gap itself is alerted.
- **Removable and re-deployable** without touching the services — which is also what makes it credible as an *independent* control rather than another feature of the same code base.

In the original system this shows up as three outer audits (user/fraud, on-chain deposit–withdraw, exchange income/fees) sitting *around* the services (order, wallet, asset, matching, payment, gateway), each fed only by events, plus small internal hooks where a service already emits its own state.

### A8. What this is, in one line

A runtime monitor for the safety property "every effect has an authorized cause", implemented as an exact set difference over compact bit vectors, fed by an observation channel independent of the write path; complemented by an atomic final checkpoint that audits its own failures.

---

## Part B — One concrete realization on Redis

- **Change channel:** Redis keyspace notifications on `asset:account:*` (`HSET`, `HINCRBY`, …); optionally CDC from Postgres as a second, independent source.
- **Cause channel:** Kafka / NATS topics (`asset.account.updated`, `matching.order.matched`, `transfer.*`, `payment.*`), producers authenticated (ACL / signed).
- **Id map:** Redis hash `uuid → int32` (plus reverse map), written only by the audit's mapper; snapshot to Postgres.
- **Bitmaps:** one Redis bitmap key per set per window (`aud:changed:{w}`, `aud:justified:{w}`, …); `SETBIT` on ingest; `BITOP NOT` then `BITOP AND` at window close; `BITPOS`/scan to list violating ids; Lua/Redis Functions to make ingest+set atomic.
- **Outputs:** alerts to a Kafka topic and a Redis stream; history and evidence to Postgres; an append-only, hashed external report.
- **Isolation:** the audit uses read-only credentials on application keys, its own keyspace for bitmaps, separate infra where possible.

### B1. One pitfall worth stating: bitmap length

`V(w) = Changed \ Justified` is `BITOP AND dst changed not_justified`, where `not_justified = BITOP NOT tmp justified`. Two Redis semantics interact badly here:

- `BITOP NOT` produces a string **exactly as long as its input**, and
- `BITOP AND` zero-pads shorter operands to the length of the longest.

So if the highest id present in `justified` is lower than the highest id in `changed`, the inverted vector is short, the padding supplies zero bits, and every violating actor above that id is silently ANDed away — **false negatives, exactly at the actors the monitor exists to catch.** Fix: pin every window's bitmaps to a fixed capacity when the window opens (`SETBIT aud:justified:{w} N-1 0`, same for `changed`), so all vectors are `N/8` bytes and the algebra is total. Same care applies to any library that stores bitmaps length-trimmed.

The reverse set, `Justified \ Changed`, is not a violation but is worth its own alert: an authorized cause with no observed write means a dropped change event, a lagging feed, or a silenced channel — see Part E.

Nothing above is specific to Redis except convenience: the same method runs on any store with a change feed plus any bit-vector library.

---

## Part C — Demo scenarios (synthetic)

1. **Normal traffic** — N actors, deposits/trades through the app path → `V(w) = ∅`.
2. **Direct tamper** — `HSET asset:account:<uuid> balance 1000000` from a shell → that actor in `V(w)` within one window.
3. **Missing event** — app writes but the bus drops the message → flagged (bug detector).
4. **Forged justification** — attacker publishes a fake `balance.updated` for an actor → caught by the nested invariant.
5. **Settlement failure burst** — one taker with forged balance sweeps many makers → all legs fail atomically; failure burst raises a risk alert; a withdrawal for that actor is blocked.

6. **AI agent without authorization** — the change channel is the tool-call log (every external side-effect an agent performs); the cause channel is the approved plan / authorization events; an agent that calls a payment or deployment tool with no approved step for it in the window lands in `V(w)`, and a "cause" event published by the agent itself (not by the approver) is rejected by producer identity.

Each scenario is a script; expected output is a JSON alert `{type, actor_ids, window, evidence}`.

### Vocabulary mapping across domains

The pattern is the same; the words differ by audience. When talking to each community, use their terms:

| This document | Fintech / exchange | Security | AI / agents |
|---|---|---|---|
| cause-justified change monitoring | realtime reconciliation, ledger integrity control | detective control, integrity monitoring | runtime guardrail, agent observability |
| change channel | store change feed / CDC | telemetry of the protected asset | tool-call log, effect trace |
| cause channel | business events (orders, transfers) | approved change tickets | approved plan, authorization events |
| actor | account / user / admin / bot | principal | agent / tool identity |
| `V(w)` non-empty | unexplained balance change | unauthorized change | unauthorized action / effect |
| final checkpoint | settlement, withdrawal gate | enforcement point | pre-execution authorization of effectful tools |
| failure audit | failed-settlement risk signal | denied-action analytics | blocked-action analytics, agent misbehavior signal |

## Part D — What was measured

Full method, caveats and what is *not* covered: [docs/BENCHMARKS.md](docs/BENCHMARKS.md). Apple M1, Go 1.26, Redis 8.10.1, single machine.

| | Result |
|---|---|
| Pipeline latency, write → monitor knows | p50 **108 µs**, p99 **220 µs** over 500 writes |
| Detection latency | pipeline + window + grace; the window term dominates, which is where **1–3 minutes** comes from |
| Set difference, 10 M actors | **234 µs**, one allocation, **1.25 MB** per set — the figure A3 predicts |
| Clean window (`IsEmpty`), 10 M actors | **51 µs** — the common case never lists members |
| Ingest | **5.4 ns** per event in Go, ~**700 k events/sec** into Redis pipelined |
| Window close in Redis, 10 M actors | **12.6 ms** |
| Cost to the audited store | **−28%** pipelined write throughput with `Eh` notifications (−43% with `KEA`) |

That last row is the one worth reading twice. The monitor adds no code to any request path — that is structural. But asking the store to publish a notification per write is real work, and calling that "zero overhead" would be a nicer sentence than it is a true one. It is invisible at 1% of a store's ceiling and expensive at 100%; the mitigations are a replica feed or CDC, which Part E already recommends for other reasons.

Window skew is covered by test rather than benchmark — see `TestGraceHoldsTheWindowOpen` and `TestEventAfterGraceIsReportedAsLate`.

## Part E — Limitations and mitigations

- Window skew → false positives: sliding windows, grace, event timestamps.
- Forged causes: producer identity, signed events, nested invariant.
- Id-map integrity: protected, append-only, periodically re-derived.
- Correlated compromise (store owned → change feed silenced): second independent change source; separate credentials/infra; audit the audit.
- Existence, not amount: bit vectors answer "was there a cause", not "does Δstate equal the sum of causes". Extension: per-actor sums alongside bit vectors (conservation, not just justification). In a full deployment the amount layer lives in the ledger: double-entry entries (debits = credits per transaction), a ledger-wide equation check (Assets = Liabilities + Equity per currency), hash-chained immutable entries for tamper evidence, and reconciliation of ledger balances against on-chain wallet balances; the cause-justified monitor and the ledger checks are complementary.

## Part F — Generalization

| Domain | Protected state | Change channel | Authorized causes | Final checkpoint |
|---|---|---|---|---|
| Exchange / payments | balances, on-chain | keyspace, CDC, chain | orders, matches, transfers | settlement, withdrawal |
| Loyalty & rewards | point balances, entitlement tiers | store change feed / CDC | accrual and redemption events with matching transaction id | redemption |
| Warehouse / logistics | stock levels | DB change feed | receipts, issues, transfers | goods issue |
| IAM / config | permissions, config | IdP / cloud audit log | approved ticket, GitOps commit | apply to production |
| ML pipelines | checkpoints, metrics, datasets | registry / object-store events | run id, experiment log, lineage | model promotion |
| AI agents | external side-effects | tool-call log | approved plan / authorization | executing an effectful tool |

## Part G — Related work (short)

**Closest prior art.** US patent 8,886,570 ("Hacker-resistant balance monitoring") detects illicit modification of a fast wallet store or a ledger by periodically deriving a balance from ledger data and comparing it to the wallet balance, with detection frequency as a performance/security trade-off. It compares *values*; this pattern checks *existence of an authorized cause* on an event-driven change feed with bit-vector set difference — the two are complementary (see "Existence, not amount" in Part E). Using store change feeds (e.g. Redis keyspace notifications) as a transparent audit hook that logs every write without touching application code is documented practice; this pattern adds the cross-check against a cause channel. Fintech engineering guidance frames integrity as three complementary tiers — by construction, runtime checks, post-factum reconciliation — and this monitor sits in the runtime tier while staying off the critical path.

I have not found the full combination (independent change feed + existence check by bit-vector set difference + short windows + nested invariant + failure audit) documented as a pattern in public sources; internal controls at exchanges and banks are rarely published, so no novelty claim is made.

Runtime verification (Havelund; Leucker & Schallhart): monitors synthesized from formal properties over event streams. Trace validation of distributed programs against TLA+ specifications. Ledger practice: double-entry as a write-time constraint, immutability, idempotency, bi-temporality; batch "detective" reconciliation (T+1). Tamper-evident logs and provenance (Crosby & Wallach; W3C PROV). Runtime verification of AI-agent actions (authorization bound to effect; verified agent policies).

## Roadmap

- [x] Reference implementation — docker-compose (Redis, NATS, Postgres) and `cmd/auditd`
- [x] Scenarios 1–6 with expected alerts as golden fixtures, plus `cmd/scenario` end to end
- [x] Benchmarks (Part D) — [docs/BENCHMARKS.md](docs/BENCHMARKS.md)
- [x] Amount-conservation extension — `Config.CheckAmounts`, scenario 7
- [ ] Plots for the benchmark tables
- [ ] CDC change channel (Postgres logical replication) as the second, durable source
- [ ] Cluster-mode Redis: `BITOP` needs its operands in one hash slot
- [ ] Optional: state the invariant in MFOTL and cross-check with MonPoly on the same trace

## License

Apache-2.0. Contributions and corrections are welcome — particularly counter-examples where the invariant is too strong or too weak for a domain.
