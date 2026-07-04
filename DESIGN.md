# High-Performance Postgres-Backed Control Plane — Design & Hardening Plan (v4)

**Status:** Proposed · **Platform:** AWS RDS PostgreSQL 16+ Multi-AZ (single region) · **Supersedes:** v3
**Prime directive:** correctness first. Every mechanism in this document is justified by a named invariant, every invariant has a named attack (race/failure), and every attack has a named test. Performance targets are retained but subordinate.

**Change from v3:** the design (§3) is carried forward essentially unchanged — poll-primary watch, doorbell as latency-only optimization, per-(GVK, bucket) gapless counters, lease fencing, timeline epochs, tombstone compaction. v4 adds: a formal invariant catalog (§2), one write-path hardening (lease share-lock closes the fence-expiry race, §3.4), a race-condition catalog with deterministic tests (§5), a continuous production invariant verifier (§6), and an expanded certification plan (§7). Sizing defaults to the 5,000-cluster tier with an in-place scale-up path to 50,000 (§4).

---

## 1. Problem Statement

etcd imposes an ~8 GB practical ceiling and a single raft group; for a regional fleet control plane at 5,000+ managed clusters it is an operational bottleneck. Moving storage to PostgreSQL creates three synchronization hazards for controller-runtime's List/Watch engine:

1. **Out-of-order commits.** With naive sequences, a transaction can take sequence N but commit after N+1; a watcher tracking sequences advances past N and misses it forever.
2. **Shard affinity.** Parent-child co-location (NodePool in its Cluster's bucket) requires per-shard sequences without a global write lock.
3. **Failover sequence regression.** A gapless counter is only trustworthy if failover can never lose an acknowledged commit; otherwise the promoted node re-issues consumed sequence numbers — same sequence, different payload — silently corrupting every downstream cache.

The system must deliver gap-free, commit-ordered event streams per (GVK, bucket); enforce single-writer semantics against split-brain; survive unplanned failover with zero committed-write loss; and never make event delivery depend on a push mechanism.

## 2. Correctness Invariants

These are the properties the system promises. Everything else exists to uphold them.

* **I1 — Gapless issuance.** Within a (GVK, bucket), committed sequence numbers are exactly 1, 2, 3, … with no holes. (Aborted transactions leave no hole because the counter increment aborts with them.)
* **I2 — Commit order = sequence order.** Within a (GVK, bucket), if seq(A) < seq(B) then A committed before B became visible. No watcher can observe B while A is still invisible.
* **I3 — No regression.** `current_seq` for any (GVK, bucket) never decreases — across crashes, failovers, promotions, restores. `(timeline_epoch, seq)` is strictly monotonic even in disaster scenarios.
* **I4 — Single writer.** At most one replica's writes for a (GVK, bucket) can commit at any instant; a replica holding a stale lease cannot commit even if it believes it holds the lease.
* **I5 — Exactly-once delivery per state.** A watcher starting from resourceVersion RV receives every object state-change with seq > RV exactly once (coalescing rapid updates to the latest state per object is permitted — Kubernetes watch semantics), with no duplicates and no losses, regardless of doorbell behavior.
* **I6 — RV monotonicity.** The composite resourceVersion observed by any client never moves backwards, including across failover and lease rebalancing.
* **I7 — Compaction safety.** A watcher can never silently skip a compacted event: if its RV predates the compaction horizon for any bucket, it receives `410 Gone` and relists; otherwise its stream is complete.
* **I8 — Optimistic concurrency.** An update presenting a stale `object_version` is rejected (409); no lost updates on a single object.

## 3. Architectural Design

```
+---------------------------------------------------------------+
|                      controller-runtime                       |
+---------------------------------------------------------------+
      | List (REPEATABLE READ snapshot)         ^ Watch (poll loop)
      v                                         |
+---------------------------------------------------------------+
|                     Custom Client Layer                       |
|   WRITE: fence(share-lock) -> counter lock -> upsert -> bell  |
|   WATCH: 5s timer ┐                                           |
|         doorbell ─┼─► debounce(100ms, lead+trail) ─► poll     |
|         (LISTEN) ─┘        dirty-flag coalesce      seq>hwm   |
+---------------------------------------------------------------+
      v                                         |
+---------------------------------------------------------------+
|        RDS PostgreSQL 16+ Multi-AZ (synchronous standby)      |
+---------------------------------------------------------------+
```

### 3.1 Schema

```sql
-- 1. Gapless monotonic sequence per (bucket, GVK)
CREATE TABLE gvk_bucket_counters (
    bucket_id   INT    NOT NULL,
    gvk         TEXT   NOT NULL,
    current_seq BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket_id, gvk)
) WITH (fillfactor = 50);          -- hottest rows in the system: keep updates HOT

-- 2. Lease fencing: authoritative writer epoch per bucket
CREATE TABLE bucket_leases (
    bucket_id  INT    PRIMARY KEY,
    holder     TEXT   NOT NULL,
    epoch      BIGINT NOT NULL,     -- strictly increases on EVERY acquisition
    expires_at TIMESTAMPTZ NOT NULL
);

-- 3. Resources: one live row per object + tombstones
CREATE TABLE kubernetes_resources (
    gvk                TEXT NOT NULL,
    namespace          TEXT NOT NULL,
    name               TEXT NOT NULL,
    uid                UUID NOT NULL DEFAULT gen_random_uuid(),
    bucket_id          INT NOT NULL,
    gvk_bucket_seq     BIGINT NOT NULL,
    object_version     BIGINT NOT NULL DEFAULT 1,
    spec               JSONB NOT NULL,
    status             JSONB NOT NULL,
    metadata           JSONB NOT NULL,
    deletion_timestamp TIMESTAMPTZ NULL,
    created_at         TIMESTAMPTZ DEFAULT now(),
    updated_at         TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (gvk, namespace, name)
);

CREATE INDEX idx_resources_list
    ON kubernetes_resources (gvk, bucket_id)
    WHERE deletion_timestamp IS NULL;                 -- List: live rows only
CREATE INDEX idx_resources_watch
    ON kubernetes_resources (gvk, bucket_id, gvk_bucket_seq);  -- Poll: ordered range

-- 4. Failover epoch (written by promotion init hook)
CREATE TABLE cluster_epoch (
    singleton   BOOL PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    timeline_id BIGINT NOT NULL
);

-- 5. Compaction horizon per (bucket, GVK)
CREATE TABLE compaction_horizon (
    bucket_id     INT    NOT NULL,
    gvk           TEXT   NOT NULL,
    compacted_seq BIGINT NOT NULL,
    PRIMARY KEY (bucket_id, gvk)
);
```

### 3.2 Composite resourceVersion

`e7|b2:1044,b5:902,b9:4123` — timeline epoch prefix + per-bucket high-water map. Serialization is canonical (buckets sorted ascending) so equal states compare equal. Upholds **I3/I6**: the epoch increments on every promotion, so `(epoch, seq)` is monotonic even if a sequence were somehow rewound. A newly leased bucket has no entry → scoped List for that bucket only, merge `bN:seq`. Stale epoch or sub-horizon seq → `410 Gone` (**I7**).

### 3.3 Atomic Write Path

```sql
BEGIN;

-- (a) FENCE — upholds I4. FOR SHARE, held to COMMIT (see §3.4 for why).
SELECT 1 FROM bucket_leases
 WHERE bucket_id = $bucket AND holder = $replica_id
   AND epoch = $lease_epoch AND expires_at > now()
 FOR SHARE;                          -- zero rows => abort: fencing violation

-- (b) SEQUENCE — upholds I1/I2. Exclusive row lock held to COMMIT serializes
--     issuance and commit order identically.
INSERT INTO gvk_bucket_counters (bucket_id, gvk, current_seq)
VALUES ($bucket, $gvk, 1)
ON CONFLICT (bucket_id, gvk)
DO UPDATE SET current_seq = gvk_bucket_counters.current_seq + 1
RETURNING current_seq;               -- => $next_seq

-- (c) UPSERT with optimistic concurrency — upholds I8.
INSERT INTO kubernetes_resources AS r
    (gvk, namespace, name, bucket_id, gvk_bucket_seq,
     object_version, spec, status, metadata, deletion_timestamp)
VALUES ($gvk, $ns, $name, $bucket, $next_seq, 1, $spec, $status, $meta, $del_ts)
ON CONFLICT (gvk, namespace, name)
DO UPDATE SET
    gvk_bucket_seq     = EXCLUDED.gvk_bucket_seq,
    object_version     = r.object_version + 1,
    spec               = EXCLUDED.spec,
    status             = EXCLUDED.status,
    metadata           = EXCLUDED.metadata,
    deletion_timestamp = EXCLUDED.deletion_timestamp,
    updated_at         = now()
WHERE r.object_version = $expected_version;   -- zero rows => 409 Conflict

-- (d) DOORBELL — latency only; correctness never depends on it (I5 rests on
--     the poll). Transactional: fires only on COMMIT.
SELECT pg_notify('resource_changes_b' || $bucket,
    json_build_object('bucket_id',$bucket,'gvk',$gvk,'seq',$next_seq)::text);

COMMIT;
```

Client rules: a 409 from (c) must ROLLBACK (never retry inside the same txn — the counter increment must abort with it, preserving I1). Any ambiguous commit outcome (connection dropped mid-COMMIT) is resolved by reading back the row and `current_seq` before retrying — the write is idempotent to verify because `object_version` and seq identify it.

### 3.4 Lease Fencing — closing the expiry race (hardening added in v4)

The v3 fence checked the lease at transaction start, leaving a window: lease expires (or is reassigned) *after* the check but *before* COMMIT — a paused writer could commit under an epoch that is no longer authoritative.

v4 closes it with lock discipline, not timestamps:

* The writer's fence takes **`FOR SHARE`** on the lease row and holds it to COMMIT.
* The coordinator's **grant/steal is an `UPDATE bucket_leases SET holder=$new, epoch=epoch+1 ...`** — a row `UPDATE` requires the exclusive lock, which **conflicts with `FOR SHARE`**.

Consequence: a new epoch cannot be granted for a bucket while any in-flight fenced write on that bucket is between fence-check and COMMIT. Either the old writer's transaction commits first (it was still the legitimate holder for that write — I4 holds), or the coordinator's grant commits first and the late writer's fence finds the new epoch and aborts. There is no interleaving in which a stale-epoch write commits after a new epoch exists. Lease *renewal* by the current holder updates only `expires_at` and also serializes behind in-flight shares; renewals are cheap and infrequent (10 s cadence) so the contention is negligible.

Time is thus advisory (`expires_at` bounds how long a dead holder blocks reassignment); the epoch + lock conflict is the actual safety mechanism. Clock skew cannot violate I4 — at worst it delays a grant.

Writer regression tripwire (defense in depth for I3): each writer caches its highest committed seq per (GVK, bucket); on reconnect it reads `current_seq` and refuses + alarms if lower. With synchronous Multi-AZ this must never fire; if it fires, halt writes for that bucket and page.

### 3.5 List

Single `REPEATABLE READ` transaction: read `cluster_epoch` + counters (build RV), then live rows via the partial index, COMMIT. Snapshot and RV are the same instant — no skew window (supports I5/I6 handoff into Watch).

### 3.6 Watch — Poll-Primary with Doorbell

Polling is the correctness mechanism (**I5**); the doorbell only changes *when* a poll happens.

**Poll cycle** per (GVK, leased bucket): `SELECT ... WHERE gvk=$1 AND bucket_id=$b AND gvk_bucket_seq > $hwm ORDER BY gvk_bucket_seq ASC` (served by `idx_resources_watch`, no sort). Dispatch Added/Modified/Deleted (tombstone ⇒ Deleted); advance the high-water mark per row. Rapid updates to one object coalesce naturally. Gapless sequences make delivery auditable: within the result set, and against the previous hwm, any missing seq must correspond to a compacted tombstone — verify against `compaction_horizon`; if the gap is below the horizon → `410 Gone` for that bucket (I7); if not explainable → invariant violation, alarm (see §6).

**Scheduling** — three triggers, one loop:
1. **Baseline timer: 5 s** unconditional (liveness backstop; sole guarantee under doorbell loss).
2. **Doorbell:** `LISTEN resource_changes_b{N}`; any notification for a leased bucket requests an early poll.
3. **Debounce floor 100 ms, leading + trailing, dirty flag.** No poll in last 100 ms → poll now. Otherwise set dirty flag and schedule exactly one trailing poll at `last_poll+100ms`. **Ordering (load-bearing):** clear the dirty flag *before* taking the poll's snapshot, re-check after; a write landing mid-poll re-arms. Even if this ordering were broken the 5 s timer bounds staleness — it can never break I5, only latency.

**Doorbell loss:** on any LISTEN drop (including failover) reconnect, re-LISTEN; the next baseline poll reconciles. No catch-up/stream ordering hazard exists — there is only the poll.

**Bookmarks:** each cycle (even empty) may emit current per-bucket hwm as a progress event so informers advance RV without relist.

### 3.7 Tombstone Compaction

Compactor deletes `WHERE deletion_timestamp < now() - retention` (default 24 h) and advances `compaction_horizon` **in the same transaction** — the horizon must never lag the physical delete, or a watcher could see an unexplained gap (I7). Retention must exceed the slowest legitimate watcher resume interval; enforce with an alarm on watcher hwm age approaching retention/2.

### 3.8 Failover

* **Unplanned:** Multi-AZ promotes the sync standby (~60–120 s). No acknowledged commit lost ⇒ I1–I3 hold by construction. All connections drop; writers run tripwires; watchers reconnect and the next baseline poll reconciles; timeline epoch increments (I6).
* **Planned:** orchestrated in a window (quiesce, verify lag 0, fail over, resume) so the reconnect surge is scheduled.
* Async read replicas are never unplanned-promotion candidates.

## 4. Infrastructure & Sizing

| Item | 5,000-cluster tier (default) | 50,000-cluster path |
|---|---|---|
| Instance | `db.r6g.large` or `xlarge` (Multi-AZ) | resize in place to `db.r6g.2xlarge` |
| Data | ~2.6 GB (RAM-resident many times over) | ~26 GB (still RAM-resident) |
| Load | ~187 RPS steady / ~374 burst | 1,870 / 3,740 RPS |
| Engine | PostgreSQL 16/17 (avoid Extended Support cliff) | same |
| Storage | gp3, IOPS = write path + WAL, ×3 headroom | raise IOPS |
| Doorbell extras | single channel fine; skip writer debounce | enable per-bucket channels/debounce per Phase 5 |

Correctness controls are identical at both tiers — races don't scale down.

## 5. Race & Failure Catalog — each with a deterministic test

Every entry names the invariant at stake, the interleaving, the defense, and the test that forces the interleaving (not hopes for it). Go tests use the race detector plus explicit synchronization points (test hooks that pause a goroutine between fence and commit, etc.); DB-level interleavings are forced with two sessions and explicit `pg_sleep`/lock ordering, or with a proxy (e.g. Toxiproxy) injecting drops at exact protocol moments.

* **R1 — Fence-expiry race (I4).** Writer passes fence, GC-pauses 40 s, coordinator reassigns, writer's COMMIT arrives late. *Defense:* §3.4 share-lock — the grant UPDATE blocks until the in-flight txn ends. *Test:* two DB sessions; session A fences and pauses before COMMIT (test hook); session B attempts the epoch-bump UPDATE and must block; assert B completes only after A, and a subsequent A-write under the old epoch aborts. Run both orderings.
* **R2 — Dirty-flag swallow (latency only, but test anyway).** Write lands between poll snapshot and flag handling. *Defense:* clear-before-snapshot, recheck-after. *Test:* unit test with a hook that injects a doorbell during the snapshot window; assert a trailing poll follows. Run 10k iterations under `-race`.
* **R3 — Doorbell loss (I5).** LISTEN connection drops silently; notifications lost. *Defense:* poll-primary. *Test:* proxy kills the LISTEN socket mid-burst without client error; assert every event still delivered within baseline interval, zero dups.
* **R4 — Counter first-write race (I1).** Two txns race the counter's first INSERT. *Defense:* `ON CONFLICT` upsert under the unique PK. *Test:* two sessions insert concurrently; assert seqs are exactly {1, 2}.
* **R5 — Ambiguous commit (I1/I5).** Connection drops during COMMIT; client doesn't know if the write landed. *Defense:* read-back protocol (§3.3). *Test:* proxy drops the connection after COMMIT is sent but before the OK; assert the client's read-back + retry yields exactly one committed state change and no seq is skipped or double-issued.
* **R6 — Lease handover overlap (I4/I5).** Old holder's last write vs. new holder's first write on the same bucket. *Defense:* R1 mechanism + new holder's scoped List starts from post-grant counter state. *Test:* scripted handover under write load; verification watcher asserts a single totally-ordered gapless stream across the handover.
* **R7 — Compaction vs. slow watcher (I7).** Watcher resumes with hwm just below a freshly advanced horizon. *Defense:* horizon advanced transactionally with the delete; boundary check on poll. *Test:* freeze a watcher, compact past its hwm, resume; assert `410 Gone` (never a silent gap). Also the off-by-one: hwm == horizon exactly must succeed.
* **R8 — Failover mid-transaction (I1–I3).** Failover strikes between counter increment and COMMIT. *Defense:* the whole txn aborts atomically; sync standby has all acknowledged commits. *Test:* Phase 6 drill with writes in flight; assert no gap (aborted increment leaves none) and no regression.
* **R9 — RV backwards exposure (I6).** Client presents an RV from a previous timeline epoch after failover. *Defense:* epoch comparison → `410 Gone`, relist. *Test:* replay a pre-failover RV post-failover; assert rejection, never a partial stream.
* **R10 — 409 handling corrupting the stream (I1).** Buggy client retries the upsert inside the same txn after a version conflict. *Defense:* client library makes it structurally impossible (txn helper owns BEGIN/COMMIT); assert in code review + a library test that a conflict always rolls back the counter increment.

## 6. Continuous Invariant Verification (production, not just tests)

Correctness that is only tested pre-GA decays. Run a **verifier** as a permanent, low-priority consumer in every environment:

* Subscribes (via the ordinary poll path) to a sample of buckets — including the hottest — and checks per (GVK, bucket): seq contiguity (I1), monotonic hwm (I3/I6), no duplicate (object, seq) deliveries (I5), all gaps explained by the compaction horizon (I7).
* A second probe writes a synthetic canary object per bucket at low rate and measures write→delivery latency (doorbell health) and end-to-end ordering.
* Any violation pages; I3/I4 violations additionally trip a write-freeze on the affected bucket (tripwire, §3.4).
* The verifier is also the acceptance oracle for every phase in §7 — the same code judges tests and production.

## 7. Certification Test Plan

* **Phase 0 — Race catalog (new, gating).** All §5 tests green under `-race`, 1,000× repetition for the timing-sensitive ones, before any load phase runs. These are unit/two-session tests — cheap, deterministic, first.
* **Phase 1 — Counter ceiling.** 50 workers, one (bucket, GVK), target instance with sync standby in the commit path. ≥200 commits/s, p99 ≤10 ms, zero serialization failures, zero fencing false-positives. Record ceiling as the bucket-sizing budget.
* **Phase 2 — Steady state.** Tier load (187 RPS at 5k; 1,870 at 50k cert) for 48 h, verifier attached. CPU <60%, read IOPS ≈0, p50 write <15 ms, verifier silent.
* **Phase 2b — Hot-bucket skew.** Zipfian: hottest bucket 20% of writes. Phase 2 criteria + cold-bucket p99 <15 ms (no starvation).
* **Phase 3 — Avalanche & rebalancing.** Kill half the replicas under load; include one deliberately zombied stale-epoch writer. No CPU exhaustion or dropped connections; verifier reports zero dups/gaps across handover; zero stale-epoch commits.
* **Phase 4 — 7-day soak.** Autovacuum sawtooth on dead tuples; table+index ≤1.5× live set; counter HOT ratio ≥90%; `idx_resources_watch` bloat contained; compactor bounded; sub-horizon RVs get `410`.
* **Phase 5 — Poll & doorbell.** At tier burst with 10/50/200 watchers: baseline poll adds <5% CPU; healthy-doorbell delivery p99 <150 ms; **notify-loss drill:** disable pg_notify mid-run — delivery continues within baseline interval, verifier silent.
* **Phase 6 — Failover drills.** Unplanned (reboot-with-failover) under load + one orchestrated planned failover, repeated ≥5×. RTO ≤120 s; zero acknowledged-write loss; tripwires silent; epoch increments exactly once per promotion; verifier confirms gapless continuity through every drill. Include R8/R9 assertions.
* **Phase 7 — Backup/restore regression (new).** Restore a snapshot into a fresh instance (DR path). Because a restore *can* legitimately rewind state, the runbook must bump the timeline epoch manually before accepting writes; the drill asserts clients relist via `410` and no stale RV is honored (I3/I6 under the one scenario sync replication doesn't cover).

## 8. Open Items

* Bucket count (64) vs. Phase 1 ceiling — hot bucket ≤50% of ceiling.
* Baseline poll interval (5 s launch) vs. Phase 5 idle-load data.
* Compaction retention (24 h) vs. slowest informer restart — confirm with ops; alarm at retention/2.
* Verifier sampling breadth vs. its own read load — start with 8 buckets incl. hottest.
