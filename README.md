# Cause-Justified Change Monitoring

*A runtime-verification pattern for money-like state: every observed change must be justified by an authorized cause. Checked near-real-time with set algebra over compact bit vectors, on an observation channel independent of the application's write path. Presented first as an abstract method, then as one concrete realization on Redis.*

Status: design + reference implementation in progress. All examples are synthetic; no proprietary data or code.

---

## Part A — The abstract method

### A1. Model

- **Actors.** The subjects whose state we protect and whose actions we observe: users, admins, automated systems (bots, jobs). Each actor has a stable identity.
- **Protected state.** A per-actor quantity that must not change without cause (balance, stock level, permission set).
- **Windows.** Time is cut into windows `w` (sliding, with a grace period; comparisons use event timestamps, not arrival time).
- **Two observation channels, independent of each other:**
  - **Change channel** — reports *that* an actor's protected state was written, from the store itself (change feed / CDC). It sees every write, including writes that bypass the application.
  - **Cause channel** — reports authorized business events (deposit, order matched, transfer, bet debit, cashout credit…) from a trusted bus, with producer identity.

For each window define two sets of actor identities:

```
Changed(w)   = { a | change channel reports a write to state(a) during w }
Justified(w) = { a | cause channel reports an authorized event with subject a during w }
```

### A2. Invariant and violation set

```
Invariant:   ∀w.  Changed(w) ⊆ Justified(w)
Violations:  V(w) = Changed(w) \ Justified(w)
```

`V(w)` is exact (set difference), not a heuristic. A non-empty `V(w)` means someone's state moved with no authorized cause in the window: bypass write, insider tampering, or a lost event (which is also worth an alert).

A **nested invariant** raises the bar against forged causes: the cause channel's own events must themselves have a deeper cause,

```
BalanceUpdated(w) ⊆ (Ordered ∪ Matched ∪ Transferred)(w)
```

so an attacker who can publish a fake `balance.updated` still cannot manufacture the order or transfer behind it.

Optionally, `Justified` can be split by actor class (users / admins / systems) to give class-specific policies (e.g. admin-caused changes are always flagged for review even if "authorized").

### A2b. Window length is a tuning knob, not a fixed constant

The invariant is checked per window, so the window length trades **detection latency** against **cost**:

- Short windows (e.g. 60 s, "realtime"): violations surface within a minute or two; more windows to close, more bit vectors to keep, more chances of skew at window edges (mitigated by grace).
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

Where the domain has a final money-moving step (settlement, payout, cashout), the earlier decision (a match, a win) is *not* final:

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

## Part D — What to measure

Detection latency (write → alert) p50/p99 per window size (10 s / 60 s); cost of set operations vs. `N` (10^4 … 10^7); false positives from window skew with/without grace; application overhead (expected zero).

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
| iGaming | game wallet, RTP | store change feed | bet debit, cashout/reward credit with matching betId/round | cashout / reward |
| Warehouse / logistics | stock levels | DB change feed | receipts, issues, transfers | goods issue |
| IAM / config | permissions, config | IdP / cloud audit log | approved ticket, GitOps commit | apply to production |
| ML pipelines | checkpoints, metrics, datasets | registry / object-store events | run id, experiment log, lineage | model promotion |
| AI agents | external side-effects | tool-call log | approved plan / authorization | executing an effectful tool |

## Part G — Related work (short)

**Closest prior art.** US patent 8,886,570 ("Hacker-resistant balance monitoring") detects illicit modification of a fast wallet store or a ledger by periodically deriving a balance from ledger data and comparing it to the wallet balance, with detection frequency as a performance/security trade-off. It compares *values*; this pattern checks *existence of an authorized cause* on an event-driven change feed with bit-vector set difference — the two are complementary (see "Existence, not amount" in Part E). Using store change feeds (e.g. Redis keyspace notifications) as a transparent audit hook that logs every write without touching application code is documented practice; this pattern adds the cross-check against a cause channel. Fintech engineering guidance frames integrity as three complementary tiers — by construction, runtime checks, post-factum reconciliation — and this monitor sits in the runtime tier while staying off the critical path.

I have not found the full combination (independent change feed + existence check by bit-vector set difference + short windows + nested invariant + failure audit) documented as a pattern in public sources; internal controls at exchanges and banks are rarely published, so no novelty claim is made.

Runtime verification (Havelund; Leucker & Schallhart): monitors synthesized from formal properties over event streams. Trace validation of distributed programs against TLA+ specifications. Ledger practice: double-entry as a write-time constraint, immutability, idempotency, bi-temporality; batch "detective" reconciliation (T+1). Tamper-evident logs and provenance (Crosby & Wallach; W3C PROV). Runtime verification of AI-agent actions (authorization bound to effect; verified agent policies).

## Roadmap

- [ ] Reference implementation (docker-compose: Redis, NATS or Kafka, Postgres; audit service in Go)
- [ ] Scenarios 1–5 as scripts with expected alerts as fixtures
- [ ] Benchmarks (Part D) with plots
- [ ] Amount-conservation extension
- [ ] Optional: state the invariant in MFOTL and cross-check with MonPoly on the same trace

## Author's note

Generalized from an audit module I designed for a spot exchange and reused on a second realtime, money-settled platform; re-derived here from the pattern, with no production data or code. Background: MSc in formal verification (JAIST; verified compiler in CafeOBJ; LNCS 10795), 15+ years building correctness-critical systems.
