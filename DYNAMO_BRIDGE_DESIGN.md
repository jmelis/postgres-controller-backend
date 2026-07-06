# DynamoDB → PostgreSQL Bridge — Design (v4)

**Status:** Proposed · **Depends on:** DESIGN.md v4 (invariants I1–I8, lease fencing, gapless counters)

**Goal:** ingest **observed cluster state** from per-MC DynamoDB **read-desires** tables into PostgreSQL, so that the platform's controllers can watch it through the ordinary List/Watch machinery. The bridge is the **sole Postgres writer** for the observed GVKs — it inherits all fencing, counter, and concurrency guarantees by construction, and owns them alone.

**Change from v3:** v3 assumed the per-MC tables held the HostedCluster/NodePool objects themselves, with contested spec/status ownership (hence `WriteSpec`, version caches, conflict-retry machinery). The actual model is the **kube-applier desire pattern** (cf. ARO-HCP kube-applier): controllers write _ApplyDesires_ (manifests to apply on the MC) and _ReadDesires_ (resources to observe) into per-MC tables; the MC-side applier executes applies and populates each ReadDesire's `.status.kubeContent` with the **observed object**. The bridge consumes only the **read-desires table**: it extracts observed objects and mirrors them into Postgres. Desired state never round-trips through the bridge, so there is **no ownership split, no dual-write, and no conflicting writer** — deleting `WriteSpec`, the version cache, and the conflict-resolution protocol. v4 also: replaces the N-replica pool with **2 replicas, simple lease failover** (§6); promotes the **no-hang contract** to a first-class requirement (§1.2); and adds **no-op write suppression as a main-library feature** (§4.3) — required because the applier refreshes desire status on a poll cadence, and general enough that every writer benefits.

---

## 1. Architecture

```
 platform API ──► Cluster / NodePool            (Postgres-native, customer-facing)
                      │ watch
                      ▼
      cluster-controller / nodepool-controller
           │ write ApplyDesire + ReadDesire            ▲ watch observed objects
           ▼                                           │ (ordinary Postgres watch)
 ┌─ per-MC DynamoDB ──────────────┐                    │
 │  apply-desires table  (OUT OF SCOPE for the bridge) │
 │  read-desires table ◄── MC applier writes           │
 │        │               .status.kubeContent          │
 │        └── stream ─────────────┐                    │
 └────────────────────────────────┼────────────────────┤
                                  ▼                    │
                     ┌────────────────────────┐        │
                     │  Bridge (2 replicas,   │        │
                     │  lease failover, §6)   ├────────┘
                     │  per MC-unit:          │  writes observed
                     │   · stream consumer    │  HostedCluster /
                     │   · reconciler         │  HostedNodePool
                     │   · both-domain lease  │
                     └────────────────────────┘
```

An **MC-unit** is the unit of ownership and failover:

```
MC-unit(k) = { both-domain lease on bucket_id(k)  — sole write authority (§4.1)
             , read-desires stream consumer        — the latency path
             , reconciler for the read-desires table — the correctness path
             , checkpoints for stream-k }
```

The replica that holds MC-k's lease — and only that replica — reads MC-k's stream, runs MC-k's reconciler, and writes MC-k's observed objects.

### 1.1 Two-path principle

Same decision DESIGN.md §3.6 makes for the watch:

- **The stream is the doorbell.** It delivers most status updates within ~1 s. Its loss, trimming, lag, or outage costs latency, never correctness.
- **The reconciler is the poll.** A periodic diff of the MC's read-desires table against Postgres repairs _any_ divergence within one reconciler period `P` (15 min). Correctness never depends on the stream behaving.

Every "what if the stream …" question has the same answer: bounded staleness, then self-repair (§12).

### 1.2 The no-hang contract (top-level requirement)

The bridge must never wedge silently. Four rules, enforced by construction:

1. **Every external call carries a context deadline** (AWS SDK 30 s; Postgres 10 s). A stuck TCP connection becomes an error, not a hang.
2. **Every loop heartbeats on every completed iteration** — including error and empty iterations. Liveness means "the loop is running," not "records are flowing": an idle stream or an unreachable DynamoDB is heartbeat-healthy (a peer could do no better); a deadlocked goroutine is not.
3. **Renewal is earned, per unit.** A unit whose heartbeat is staler than 2× the renewal interval is excluded from lease renewal and torn down; its lease expires and the peer replica adopts it from the fenced checkpoint (§5.2). One wedged MC never blocks the others.
4. **The process is disposable.** A replica-wide wedge (teardown itself stuck) trips the Kubernetes liveness probe (wired to the same heartbeats); the restarted process comes back as a claimant. Worst case for any hang: lease TTL + one claim cycle ≈ **~40 s** to resume on the peer.

---

## 2. Data Model — desires and observed objects

### 2.1 ReadDesire → observed object

A ReadDesire names a resource on the MC to list/watch (`.spec.targetItem`); the MC applier populates `.status.kubeContent` with the observed object(s) and `.status.conditions["Successful"]` with sync health. The bridge's caller-provided **mapper** turns one read-desire item into **zero or more observed objects**:

```
mapper(item) → []ObservedObject{ GVK        (HostedCluster | HostedNodePool)
                                , namespace, name
                                , object     (full observed content)
                                }
```

Zero when `.status.kubeContent` is empty (desire created, applier not yet synced). Mapper rules:

- Output must be **deterministic and canonical** — no always-changing fields (observation timestamps, ordering jitter). No-op suppression (§4.3) compares mapped output against the Postgres row; a nondeterministic mapper defeats it and floods watchers.
- Every mapped object must belong to the unit's MC bucket (§3.1); anything else is a **poison record** (§9) — a mis-routed table or broken mapper must page, not write into another MC's bucket.
- An unparsable item is likewise poison: dead-letter + page. The reconciler hits the same mapper on the same item, so poison is a bug, not noise.

Stream events:

| eventName | Meaning                               | Bridge action                                                    |
| --------- | ------------------------------------- | ---------------------------------------------------------------- |
| `INSERT`  | new ReadDesire (status usually empty) | map → usually zero objects; no-op                                |
| `MODIFY`  | applier updated `.status`             | map → upsert observed objects (§4.2)                             |
| `REMOVE`  | controller deleted the ReadDesire     | tombstone the observed objects it produced (`Keys` + `OldImage`) |

A `REMOVE` means "the platform no longer tracks this" (cluster deprovisioned or observation withdrawn) — the observed object is tombstoned so watchers see the deletion.

### 2.2 Scope: read-desires table only

Apply-desires live in a **separate per-MC table** and are out of scope: controllers learn apply success/failure by another path. If ApplyDesire condition bridging is wanted later, it is additive — a second stream consumer and reconciler inside the same MC-unit, same bucket, same lease, mapping `.status.conditions` to a condition object. Nothing in this design forecloses it (`mc_registry` gains a second table ARN column, §3.3).

### 2.3 Naming

The customer-facing resources are **Cluster** and **NodePool** (platform API, Postgres-native, not bridged). The observed HyperShift resources are **HostedCluster** and **HostedNodePool** in this design and in Postgres GVK names — "HostedNodePool" disambiguates from the customer's NodePool; that HyperShift's API type is spelled `NodePool` is an implementation detail confined to the mapper.

---

## 3. Partitioning — the MC is the bucket

### 3.1 Bucket assignment

```
bucket_id(k) = MC_BASE + mc_index(k)        -- constant per MC-unit; no hashing
```

Every record in stream-k belongs to MC-k, hence to exactly one bucket. HostedCluster and HostedNodePool are co-located per MC by construction. Capacity: ≤100 HostedClusters + ~500 HostedNodePools ≈ **≤ ~600 observed objects per bucket**; real change rate ~4 writes/s per MC against the ~317/s measured single-connection per-bucket ceiling on RDS db.m6g.2xlarge (Multi-AZ sync commit).

### 3.2 Two partition schemes, one bucket_id space

| Scheme | GVKs                                                       | bucket_id            | Count                |
| ------ | ---------------------------------------------------------- | -------------------- | -------------------- |
| Fixed  | Cluster, NodePool + other control-plane GVKs (not bridged) | `hash(key) % 16`     | constant             |
| MC     | HostedCluster, HostedNodePool (bridged)                    | `MC_BASE + mc_index` | 50 → ~500 with fleet |

The storage layer needs no changes: `gvk_bucket_counters`, `kubernetes_resources`, and `compaction_horizon` are keyed `(bucket_id, gvk)`, and counter rows appear lazily on first write. **The one hard constraint: ranges must be disjoint** — `bucket_leases` is keyed `(bucket_id, domain)`, not per GVK, so a fixed-scheme hash colliding with an MC index would put a platform controller and a bridge replica on the same lease row. Fixed schemes own `0–4095`; `MC_BASE = 4096`.

### 3.3 MC registry

```sql
CREATE TABLE mc_registry (
    mc_id           TEXT PRIMARY KEY,
    mc_index        INT  NOT NULL UNIQUE,   -- allocated monotonically; NEVER reused
    read_table_arn  TEXT NOT NULL,          -- the read-desires table
    read_stream_arn TEXT,                   -- current stream; updated on rotation (§8.5)
    state           TEXT NOT NULL CHECK (state IN ('active', 'draining', 'retired')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Replicas poll the registry (60 s) to discover claimable MC-units (§6.2). `mc_index` is never reused: a retired MC's bucket keeps its counter and horizon rows forever, so a recycled index could revive a bucket whose sequence history belongs to a dead MC.

### 3.4 Bucket growth is additive — no epoch bump, no relist

Onboarding an MC creates a new bucket but moves no existing key. New buckets flow through DESIGN.md §3.2's path: a watcher whose composite RV lacks an entry for the bucket performs a scoped List for that bucket only and merges `bN:seq`. No timeline-epoch bump, no fleet-wide relist; the epoch-bump migration is only for true resharding, which the MC scheme never does.

---

## 4. Sole-Writer Model

### 4.1 Leases: the bridge owns both domains

The observed GVKs have exactly one Postgres writer — the bridge. Each MC-unit acquires **both** lease domains atomically via the existing `BothManager.AcquireBoth` (single multi-row statement). `Write()` fences against the spec row as usual; holding the status row too means no other process can ever write these objects, by lock discipline rather than convention. No `WriteSpec`, no split ownership, no cross-domain conflicts.

### 4.2 Write path — read-back, then `Write()`

Per observed object: `ReadBack` the current row (PK lookup; tombstones are rows, so no special list variant is needed), then `Write()` with `ExpectedVersion` = current version (0 if absent). Because the unit is the sole writer and processes its keys sequentially per shard, the read-back is always correct:

- `ErrConflict` **cannot occur** (no other writer exists). If it ever surfaces, it is treated as Fatal — it means the sole-writer assumption broke (a second claimant, i.e. a fencing bug) and must page, not retry.
- `ErrAlreadyExists` occurs only on **replay** of a create (crash before checkpoint): read back, convert to an update, retry — deterministic success, no contender.
- A `REMOVE` for an already-absent row (compacted tombstone, or never created) is the converged state: **success, no-op**. Consequence: replayed `REMOVE`s racing compaction are harmless; compaction retention (24 h) needs no coupling to the stream window.

No version cache: at ~4 real writes/s per MC, one extra PK read per write is noise, and deleting the cache deletes its staleness bugs.

### 4.3 No-op write suppression — a main-library feature (required)

**The applier refreshes desire status on a poll cadence (~3 min across all desires).** If it rewrites `.status` even when nothing changed, the stream carries a MODIFY per ReadDesire per cycle — potentially ~1,600 events/s fleet-wide of no-ops. Without suppression, every one would consume a sequence number, bump `object_version`, and wake every watcher of the bucket. Suppression is therefore mandatory for the bridge — and it is generally correct behavior, so it goes in the **main library**, not the bridge:

- Inside the `pgctl_write()` stored procedure, **after the fence and before the counter increment**: PK-read the current row; if `(spec, status, metadata, deletion_timestamp)` are semantically equal to the request (JSONB `=` is key-order-insensitive), return immediately with `changed=false` — no counter, no upsert, no doorbell.
- Because the check precedes step (b) of DESIGN.md §3.3, a suppressed write **consumes no sequence number** (I1 preserved by construction), emits **no doorbell**, bumps **no `object_version`**, and generates **no watch event** — matching Kubernetes API-server semantics, where an update that changes nothing does not advance resourceVersion.
- Semantics: if content is equal, the write succeeds as a no-op **regardless of `ExpectedVersion`** — the caller's intent ("make the state X") is already satisfied. This is level-based idempotence; it also makes at-least-once stream delivery literally free: a duplicated record is a suppressed write.
- One extra PK lookup per write; against the cost of a counter bump + row upsert + watcher wakeups it saves, strictly a win at any no-op ratio above ~0.
- Applies to `Write()` and `WriteStatus()` uniformly; opt-out flag for callers that want unconditional bumps (none known).

Side effect on the bridge: the reconciler's repair writes and all stream replays dedup automatically — a healthy reconciler pass generates **zero** Postgres writes and zero watch events.

### 4.4 Replay semantics — mostly no-ops now

After a crash mid-batch, uncheckpointed records replay in original per-key order. With §4.3, replays of content the row already holds are suppressed entirely; only genuinely superseded intermediate states write. Residual honesty: a resource may transiently regress to an older observed image and roll forward within one replayed batch (~1 s); level-triggered controllers tolerate this (indistinguishable from having watched the intermediate states live). Final state after replay = state after the original batch, by per-key ordering. I1/I2/I5/I8 hold throughout (§13).

---

## 5. Checkpointing

### 5.1 Checkpoint table

```sql
CREATE TABLE stream_checkpoints (
    stream_arn   TEXT        NOT NULL,
    shard_id     TEXT        NOT NULL,
    last_seq_num TEXT        NOT NULL,
    holder_id    TEXT        NOT NULL,   -- diagnostic; authority comes from the fence (§5.2)
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stream_arn, shard_id)
);
```

`stream_arn` is per-MC, so checkpoint rows are naturally unit-scoped.

### 5.2 Fenced checkpoint protocol

Resource writes are fenced; checkpoints must be too, or a zombie replica whose lease was stolen could move the peer's position past records it never wrote (an I9 violation). One stream maps to one bucket, so the fence is a single indexed lookup on the unit's two lease rows:

```sql
BEGIN;
-- Fence: the unit must still hold BOTH domain rows at the epochs it believes.
-- FOR SHARE conflicts with a claimant's grant UPDATE exactly as in DESIGN.md
-- §3.4 — a checkpoint cannot commit concurrently with a steal.
SELECT count(*) FROM bucket_leases
 WHERE bucket_id = $mc_bucket
   AND ((domain = 'spec'   AND epoch = $spec_epoch)
     OR (domain = 'status' AND epoch = $status_epoch))
   AND holder = $me AND expires_at > now()
 FOR SHARE;                       -- count < 2 => ROLLBACK, ErrFatal (unit teardown)

UPDATE stream_checkpoints
   SET last_seq_num = $seq, holder_id = $me, updated_at = now()
 WHERE stream_arn = $arn AND shard_id = $shard;
COMMIT;
```

A failed fence is `ErrFatal` for the unit (§6.4); the adopting replica resumes from the last legitimately committed checkpoint. One fenced transaction per `GetRecords` batch — negligible.

### 5.3 Protocol

After each batch: process records sequentially through §4.2; after the final record commits, run the fenced checkpoint. On restart/adoption: `GetShardIterator(AFTER_SEQUENCE_NUMBER, last_seq_num)`. No checkpoint → `TRIM_HORIZON` + an immediate reconciler pass (§10.4). Per-record checkpointing is not worth doubling checkpoint writes; the replay window is ≤ one batch and mostly suppressed (§4.4).

---

## 6. Topology — 2 Replicas, Simple Lease Failover

### 6.1 Model

**Two identical replicas run the same claim loop; the per-MC lease is the assignment mechanism.** No coordinator, no balancer, no cap/slack machinery:

- Both replicas poll `mc_registry` every 60 s (jittered) and attempt `AcquireBoth` on any active MC whose lease is unheld or expired. Acquisition is the existing atomic epoch-bump — exactly one winner per MC; the loser moves on.
- Steady state: units land on whichever replica claimed them first. The split is arbitrary and irrelevant — either replica can carry the whole fleet (§15.1); an uneven or 100/0 split is _fine_, and both replicas stay warm by construction.
- **Correctness is independent of the replica count** (I4: the lease admits one writer per bucket no matter who races). Two replicas is purely the availability choice; a third can be added later without design change.

### 6.2 Failover

A replica dies (or one unit is torn down by the watchdog): the affected leases stop renewing and expire (TTL 30 s); the peer's next claim cycle acquires them and runs unit startup (§6.3). Worst-case pause per MC: **TTL + one claim cycle ≈ ~40 s**, during which records buffer in the stream — no loss (§9). Graceful shutdown (deploys) is faster: finish the in-flight **record**, run the fenced checkpoint, **release** the leases — released leases are claimable immediately, so a rolling deploy hands units over in seconds, not TTL.

### 6.3 Unit startup sequence

```
1. AcquireBoth on bucket_id(k)                — the claim
2. Load checkpoints for read_stream_arn(k)
3. Start shard discovery + one goroutine per processable shard (§8)
4. Start the unit's reconciler timer (§10)
5. Enter processing loops
```

(No cache-seeding step — v3's version cache is gone, §4.2.) Adopting several units runs their startups concurrently.

### 6.4 Renewal, watchdog, teardown

One multi-row renewal for all held units every `ttl/3` (10 s), gated by the no-hang contract (§1.2): units with stale heartbeats are excluded and torn down. `ErrFatal` for a unit (fence violation, failed checkpoint fence, `ErrNotHolder` on renewal, unexpected `ErrConflict` §4.2) tears down **that unit only** — stop its goroutines, drop its claim; the replica keeps serving its other units. Whoever claims the MC next (either replica) starts clean from the checkpoint. Replica-wide failure is just all its units' leases expiring at once.

---

## 7. MC Onboarding & Offboarding

**Onboard:** insert into `mc_registry` (next `mc_index`, state `active`). Within one claim cycle a replica acquires the unit, finds no checkpoint → `TRIM_HORIZON` + immediate reconciler pass, which performs the initial backfill of already-populated ReadDesires. Watchers pick up the new bucket via scoped List (§3.4). No epoch bump, no relist, no migration.

**Offboard:** set state `draining` — the owning replica drains the stream backlog, runs a final reconciler pass, checkpoints, releases; set `retired`. Whether the observed objects are tombstoned or retained is fleet policy (normally the controllers delete the ReadDesires first, which tombstones through the normal path, §2.1). The `mc_index` is never reused (§3.3).

---

## 8. Shard Management (per MC-unit)

### 8.1 Shard properties

- Parent-child lineage: a parent shard must be fully processed before its children (per-key order crosses the boundary).
- Closed shards are finite — process to exhaustion, then release to children. Open shards are long-polled.
- `ExpiredIteratorException` (15 min idle): re-acquire from the last checkpoint.
- **`TrimmedDataAccessException`** (checkpoint older than the 24 h trim horizon): records were irrecoverably lost from the stream. Page, restart at `TRIM_HORIZON`, trigger the unit's reconciler (§10.4) — the reconciler is the repair; the page exists because being ~24 h behind deserves a human question.

### 8.2 Discovery

Every 60 s per unit, `DescribeStream` on `read_stream_arn` — following `LastEvaluatedShardId` pagination to completion. Build the parent DAG; shards whose parents were trimmed out are roots. Per-MC read-desires tables are small (≤ ~600 desires), so expect 1–4 open shards per stream.

### 8.3 Concurrency model

One goroutine per processable shard, per unit, concurrent across the replica's units. Per-key ordering needs no cross-shard locks (a partition key lives in one shard at a time; lineage gating preserves order across resharding). Units are disjoint by key space. Each shard goroutine heartbeats independently (§1.2), so one wedged shard is detected while others progress.

### 8.4 Stream ARN rotation

Disabling/re-enabling streams creates a **new stream ARN**; old checkpoints are orphaned and writes made while disabled never appear in the new stream. Handling: registry update to `read_stream_arn` → unit restarts its stream side → no checkpoint for the new ARN → `TRIM_HORIZON` + immediate reconciler pass + alarm (rotation should be an operator action, not a surprise).

---

## 9. Error Classification

The key property: **infrastructure failure produces backpressure, never skips.**

| Class            | Examples                                                                           | Handling                                                                                                                                                                                                       |
| ---------------- | ---------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Retryable**    | network error, Postgres down/failover, serialization failure                       | Retry the _same record_ with capped exponential backoff, indefinitely. The shard loop blocks; records buffer in the stream (that is what 24 h retention is for). Never skip, never DLQ, never checkpoint past. |
| **Resolvable**   | `ErrAlreadyExists` on replayed create                                              | Read-back, convert to update, retry — deterministic (§4.2).                                                                                                                                                    |
| **Poison**       | mapper cannot parse the item; mapped object outside the unit's MC (§2.1)           | Dead-letter + page + skip. Deterministic failures are bugs — the reconciler hits the same mapper on the same item, so a human must act.                                                                        |
| **Fatal (unit)** | `ErrFenceViolation`, failed checkpoint fence, lost lease, any `ErrConflict` (§4.2) | Tear down the unit (§6.4); the next claimant resumes from the fenced checkpoint.                                                                                                                               |

### 9.1 Unit shard loop (pseudocode)

```
func (u *MCUnit) processShardLoop(ctx, shardID, iterator):
    for ctx is not cancelled:
        records, nextIterator, err = dynamodb.GetRecords(iterator, limit=1000, deadline=30s)
        switch err:
            case ExpiredIteratorException:
                iterator = reacquireIterator(shardID, lastCheckpoint); continue
            case TrimmedDataAccessException:
                page(); iterator = trimHorizonIterator(shardID)
                u.reconciler.TriggerNow(); continue
            case retryable:
                backoff(); u.heartbeat(shardID); continue

        for each record in records:
            for each obj in u.mapper.Map(record):        // zero or more (§2.1)
                err = u.applyObserved(ctx, obj)          // read-back + Write, §4.2
                switch err:
                    case nil:            // includes suppressed no-ops
                    case ErrRetryable:   backoff(); retry same object  // blocks the shard
                    case ErrPoison:      deadLetter(record); page(); break record
                    case ErrFatal:       return          // unit teardown (§6.4)
            lastSeqNum = record.SequenceNumber

        if len(records) > 0:
            if err = u.fencedCheckpoint(shardID, lastSeqNum); err != nil:
                return                                   // ErrFatal
        u.heartbeat(shardID)

        if nextIterator == nil: break                    // closed shard exhausted
        iterator = nextIterator
        if len(records) == 0: sleep(250ms)
```

---

## 10. Reconciler — the correctness mechanism (per MC-unit)

The boring loop that makes every other component's failure survivable — the bridge's analog of the 5 s baseline poll.

### 10.1 Cycle

Runs inside the unit (it holds the lease), period `P` = **15 min**, plus event-triggered passes (§10.4).

1. **Forward pass:** `Scan` the MC's read-desires table (≤ ~600 items — no segmentation needed; rate-limit anyway; eventually-consistent reads are fine, the repair step re-checks). For each item: map → compare each observed object against its Postgres row. Divergent or missing → repair candidate.
2. **Deletion pass:** after a full scan generation completes, any live Postgres row in bucket k not produced by any scanned desire → deletion candidate. Mark-and-sweep with a scan generation id — never delete from a partial scan.
3. **Repair, per candidate:** strongly consistent `GetItem` → re-map → re-compare (the stream usually repaired it already) → if still divergent, `Write()` the observed image (or tombstone if the desire is gone). No-op suppression (§4.3) makes redundant repairs free.

The consistent-read-then-recompare closes two races: a stale scan must not overwrite a newer state the stream already delivered, and a desire created after the scan passed must not get its objects wrongly tombstoned.

### 10.2 Convergence

Both paths write states read from the read-desires table and are idempotent toward its current content. Any divergence, from any cause, lasts at most `P` after the cause stops — **I9** (§13.1). Trimmed streams, DLQ'd records, ARN rotations, and checkpoint mishaps all fall inside it. A healthy pass performs zero writes (§4.3) — "reconciler repairs per pass" is a direct leak detector for the stream path (§14).

### 10.3 Triggers for an immediate pass

`TrimmedDataAccessException` (§8.1) · missing checkpoint / new stream ARN (§7, §8.4) · MC onboarding backfill (§7) · operator request. Unit-scoped by construction.

---

## 11. Trusting the stream — the position

**Trust:** per-partition-key ordering within lineage, and at-least-once delivery — DynamoDB's documented contract; the bridge leans on both.

**Do not trust:** retention (24 h trim cliff), liveness (shards lag, iterators expire, the API degrades), availability (streams can be disabled or rotated while the table keeps taking writes). None are correctness inputs: the reconciler bounds them all at `P` = 15 min, and the watchdog + iterator-age alarms (§14) bound how long they go unnoticed.

---

## 12. Failure Modes — every row ends in self-repair

| Failure                               | Impact                                     | Detection                                      | Recovery                                                                                       |
| ------------------------------------- | ------------------------------------------ | ---------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| Replica crash                         | its units pause; records buffer in streams | lease expiry                                   | peer claims each unit ≤ TTL + claim cycle (~40 s); replay mostly suppressed (§4.4)             |
| One unit hangs (stuck call, deadlock) | one MC pauses                              | stale heartbeat (§1.2)                         | excluded from renewal → lease expires → peer adopts; other units unaffected                    |
| Whole replica wedges                  | its units pause                            | liveness probe (same heartbeats)               | pod restart; peer adopts meanwhile                                                             |
| Postgres down / RDS failover (~2 min) | all units block on retryable errors        | write/renewal errors                           | backpressure — nothing skipped (§9); resume, or leases expire and are re-claimed post-recovery |
| Postgres outage > lease TTL           | all leases expire                          | `ErrNotHolder` on renewal                      | units torn down; claim cycle redistributes once Postgres returns                               |
| Zombie unit writes or checkpoints     | — (prevented)                              | write fence / checkpoint fence (§5.2)          | successor's position never corrupted                                                           |
| Applier rewrites status every poll    | high stream volume, no real changes        | suppressed-write ratio metric (§14)            | no-op suppression absorbs it: zero seq consumption, zero watch events (§4.3)                   |
| Stream lags / hangs                   | one MC's staleness grows                   | iterator-age alarm                             | catch-up on recovery; reconciler bounds staleness at `P`                                       |
| Records trimmed (>24 h behind)        | stream-side loss, one MC                   | `TrimmedDataAccessException` → page            | `TRIM_HORIZON` + reconciler pass repairs from the table                                        |
| Stream disabled/re-enabled (new ARN)  | gap during disabled window                 | registry rotation / missing checkpoint → alarm | reconciler pass covers the gap (§8.4)                                                          |
| Poison record                         | one desire unprocessed                     | DLQ + page                                     | human fixes mapper; reconciler then repairs                                                    |
| Registry drift                        | new MC unserved / dead MC claimed          | claim-cycle diff (§14)                         | claim cycle converges ≤ 60 s + jitter                                                          |

---

## 13. Correctness Analysis

| Invariant                              | How the bridge preserves it                                                                                                                                                                                                                                                    |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **I1 — Gapless issuance**              | All writes go through the counter-in-transaction. Suppressed no-ops consume no sequence number by construction (§4.3 check precedes the increment); aborts leave no gap.                                                                                                       |
| **I2 — Commit order = sequence order** | Counter's exclusive row lock, unchanged.                                                                                                                                                                                                                                       |
| **I3 — No regression**                 | Sync Multi-AZ + writer tripwire, inherited. `mc_index` non-reuse (§3.3) prevents reviving a retired bucket's history.                                                                                                                                                          |
| **I4 — Single writer**                 | The bridge holds **both** domains per MC bucket (§4.1); every write and every checkpoint is `FOR SHARE`-fenced (§5.2). Claim = lease, so assignment and fencing cannot disagree.                                                                                               |
| **I5 — Exactly-once delivery**         | Watch path untouched. Replays are suppressed or re-applied states, never protocol violations (§4.4). No-op suppression removes spurious events rather than real ones — a state change always produces exactly one event.                                                       |
| **I6 — RV monotonicity**               | Timeline epochs unchanged; bucket growth is additive (§3.4), entering RVs via scoped List merge, never an epoch event.                                                                                                                                                         |
| **I7 — Compaction safety**             | Unchanged; replayed `REMOVE` on a compacted row is an idempotent no-op (§4.2).                                                                                                                                                                                                 |
| **I8 — Optimistic concurrency**        | Sole writer: `ExpectedVersion` from read-back is always current; any `ErrConflict` is a fencing-bug page, not a retry loop (§4.2). No-op semantics (content-equal ⇒ success regardless of version) is level-based idempotence, not a lost update — there is no update to lose. |

### 13.1 Bridge invariant

**I9 — Bounded convergence, per MC.** For every observed GVK in MC k, PostgreSQL converges to the read-desires table's observed content within `P` = 15 min after divergence causes cease — regardless of stream loss, trimming, replica failure, or skipped records. Upheld by the unit's reconciler alone; the stream reduces the typical bound to ~1 s. Faults are MC-scoped.

### 13.2 Ordering semantics

Per key: applier write order = stream order (within lineage) = Postgres commit order on the fast path. Replays and repairs can transiently interleave older observed states (§4.4), always converging. Across keys and across MCs: Postgres sequence order is processing order — a valid total order (I2); no invariant requires mirroring DynamoDB's cross-key timing. **End-to-end freshness caveat:** an observed status is as old as the applier's last sync of that ReadDesire (~3 min poll cadence) _plus_ bridge latency (~1 s). The bridge cannot make observations fresher than the applier produces them.

---

## 14. Monitoring (requirements)

- **Iterator age** per (unit, shard) — primary "stream lagging/hung" signal. Alert > 5 min.
- **Heartbeat age** per unit loop — the renewal gate acts on it; the alert explains it.
- **Checkpoint lag** per (stream, shard).
- **Reconciler per unit:** last completed generation age (alert > 2 P); **repairs per pass** (sustained nonzero = the stream path is leaking); deletion-pass tombstones.
- **Suppressed-write ratio** — high is expected (applier poll refreshes); a sudden _drop_ means the mapper went nondeterministic or the applier changed behavior.
- Units per replica; unclaimed active MCs (alert > 2 claim cycles); lease renewal failures; DLQ depth; any `ErrConflict` (pages — fencing bug, §4.2).

---

## 15. Performance

### 15.1 Load budget

| Tier               | MCs | Observed objects | Real change rate | Per-MC bucket | Bucket ceiling |
| ------------------ | --- | ---------------- | ---------------- | ------------- | -------------- |
| 5k HostedClusters  | 50  | ~30k             | ~187/s fleet     | ~4 writes/s   | ~317/s         |
| 50k HostedClusters | 500 | ~300k            | ~1,870/s fleet   | ~4 writes/s   | ~317/s         |

Per-bucket load is constant as the fleet grows (scaling adds MCs). **Stream-side volume may far exceed write volume** if the applier rewrites status each poll — up to ~1,600 records/s fleet-wide — but suppressed records cost one PK read each and no writes; `GetRecords` at 1,000 records/call absorbs it. Either single replica can carry the whole fleet: ~500 streams × 1–4 shards is a few hundred long-poll goroutines and ≤ ~2,000 records/s of mostly-suppressed processing.

### 15.2 Watch-side fan-out — the real cost of many buckets

A per-MC-scoped watcher pays 2 index probes per poll cycle (nothing). A **fleet-wide** watcher (your 1–2 controllers, if they watch all MCs) pays 500 buckets × 2 GVKs = ~1,000 indexed probes per 5 s cycle. Cheap individually; budget it in a Phase-5-style certification with the real controller count. This — not write throughput — is the number to watch as MC count grows (§17, open).

### 15.3 End-to-end latency

Applier observation cadence (~3 min poll) **dominates**. Bridge adds: stream propagation ~100 ms–1 s + read-back/write ~15–35 ms. Worst-case bridge staleness: `P` = 15 min per MC, reached only when that MC's stream path is broken and unnoticed.

### 15.4 Postgres connections per replica

Shared pgx pool (~4–8 conns) for all units' writes and reconcilers · 1 conn for leases/claims/checkpoints. Two replicas ≈ ~20 connections total. The bridge uses `writer.Writer` directly (long-lived conns, explicit retry control); `pgruntime` is shaped for reconcilers, not CDC.

---

## 16. Required additions to the storage layer

1. **No-op write suppression** (§4.3) in `Write()`/`WriteStatus()` — content-equal writes consume no sequence number, no `object_version` bump, no doorbell, no watch event; returns `Changed: false`. General library behavior (opt-out flag), not bridge-specific. Needs tests: suppressed write leaves counter untouched; suppression under the fence; interleaving with a real change still orders correctly.
2. **`stream_checkpoints` table** (§5.1) + fenced checkpoint helper (§5.2, two-row `FOR SHARE`).
3. **`mc_registry` table** (§3.3) + reserved bucket_id ranges (`MC_BASE = 4096`, fixed schemes `0–4095`) (§3.2).
4. **Dynamic bucket sets for fleet-wide watchers** (§3.4). `reader.Watcher` fixes its bucket set at construction; it cannot add a bucket mid-stream. Either (a) registry-driven bucket addition in `Watcher`/`pgruntime.ListerWatcher`, or (b) accept a fleet-wide-watcher restart per MC onboarding — viable at low onboarding cadence, but must be a stated decision.

Deleted relative to v3 (bridge requirements): `WriteSpec` and tombstone-inclusive list are no longer bridge requirements (no split ownership, no version cache). Note: `WriteSpec` now exists in the storage layer for the controller-runtime bridge (pgruntime); `reader.List` excludes fully-deleted tombstones but includes dying objects with finalizers. The DynamoDB bridge uses `Write()` exclusively since it holds sole ownership of both domains.

---

## 17. Bridge Race Catalog — deterministic tests (RB1–RB9)

Same discipline as DESIGN.md §5: name the invariant, force the interleaving.

- **RB1 — Zombie checkpoint (I4/I9).** Unit A pauses (hook) inside the fenced checkpoint txn after `FOR SHARE`; a rival claims A's bucket — the grant must block; after A completes, A's next checkpoint must fail the fence. Both orderings.
- **RB2 — Postgres outage mid-batch (I9).** Toxiproxy severs Postgres mid-batch. Assert: zero dead-letters, zero checkpoint advance, shard blocked; on heal, every record lands exactly once by content.
- **RB3 — Crash-replay convergence (§4.4).** Kill the replica after N of M records; peer adopts. Assert final state = state after M; replayed content-equal records produce **zero** new sequence numbers (suppression); the transient window contains only states from the original sequence.
- **RB4 — No-op suppression under the fence (I1/I5).** Write identical content twice; assert the second consumes no sequence number, emits no event, and `object_version` is unchanged. Then interleave a real change between two no-ops (hook) and assert exactly one event, correctly ordered.
- **RB5 — Reconciler vs. stream interleave (I9).** Deliver a stale buffered stream record after a reconciler repair of the same key. Assert convergence to the table's observed content.
- **RB6 — Deletion-pass guard (I9).** Create a ReadDesire after the scan passed; run the deletion pass. Assert the consistent `GetItem` re-check drops the tombstone candidate.
- **RB7 — Hang takeover (liveness).** Wedge one unit's shard goroutine (hook blocks past deadline handling). Assert: excluded from renewal within 2× interval; lease expires; the peer adopts from the checkpoint; no gap, no dual-writer commit; the wedged replica's other units keep processing throughout.
- **RB8 — Claim race (I4).** Both replicas race `AcquireBoth` on the same expired MC. Assert exactly one wins and the loser's write and checkpoint attempts are fenced. Repeat under rapid expiry cycling.
- **RB9 — Additive bucket growth (I6).** Onboard an MC while a fleet-wide watcher is mid-stream. Assert the watcher merges the new bucket via scoped List — no epoch bump, no 410, no missed or duplicated events on existing buckets.

---

## 18. Open Questions

1. **Applier write behavior** — does it rewrite `.status` every poll or only on change? Doesn't affect correctness (suppression absorbs either), but sets stream volume and DynamoDB cost expectations.
2. **ApplyDesire conditions** (§2.2) — confirm controllers learn apply success/failure elsewhere; if not, scope the additive second consumer.
3. **Fleet-wide watcher fan-out** (§15.2) — certify the real controller count against ~500-bucket poll cycles; decide watcher-restart vs. dynamic buckets (§16.4).
4. **Registry authority** — what writes `mc_registry`? Presumably MC onboarding automation; define the owner and the offboarding data policy (§7).
5. **DLQ target** for poison records — SQS vs. Postgres error table.
6. **Mapper contract details** — exact `ReadDesire` schema (one object per desire, or list results with many?), canonicalization rules (§2.1).
