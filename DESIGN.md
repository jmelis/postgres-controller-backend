# High-Performance Postgres-Backed Control Plane — Design & Hardening Plan (v4)

**Status:** Proposed · **Platform:** AWS RDS PostgreSQL 16+ Multi-AZ (single region) · **Supersedes:** v3
**Prime directive:** correctness first. Every mechanism in this document is justified by a named invariant, every invariant has a named attack (race/failure), and every attack has a named test. Performance targets are retained but subordinate.

**Change from v3:** the design (§3) is carried forward essentially unchanged — poll-primary watch, doorbell as latency-only optimization, per-(GVK, bucket) gapless counters, lease fencing, timeline epochs, tombstone compaction. v4 adds: a formal invariant catalog (§2), one write-path hardening (lease share-lock closes the fence-expiry race, §3.4), a spec/status split with independent fencing domains (§3.3b), a race-condition catalog with deterministic tests (§5, including R11/R12 for the split), a continuous production invariant verifier (§6), and an expanded certification plan (§7). Sizing defaults to the 5,000-cluster tier with an in-place scale-up path to 50,000 (§4).

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
|   WRITE: fence(share-lock) -> suppress? -> counter -> upsert -> bell  |
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

-- 2. Lease fencing: authoritative writer epoch per (bucket, domain).
--    Spec and status are independent domains — different holders can fence
--    the same bucket for different sub-resources. FOR SHARE on one domain's
--    row does not conflict with a grant UPDATE on the other domain's row.
CREATE TABLE bucket_leases (
    bucket_id  INT    NOT NULL,
    domain     TEXT   NOT NULL CHECK (domain IN ('spec', 'status')),
    holder     TEXT   NOT NULL,
    epoch      BIGINT NOT NULL,     -- strictly increases on EVERY acquisition
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (bucket_id, domain)
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

-- 6. DynamoDB stream checkpoint (fenced via bucket_leases FOR SHARE)
CREATE TABLE stream_checkpoints (
    stream_arn   TEXT        NOT NULL,
    shard_id     TEXT        NOT NULL,
    last_seq_num TEXT        NOT NULL,
    holder_id    TEXT        NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stream_arn, shard_id)
);

-- 7. MC registry: authoritative map from MC to bucket and DynamoDB table ARN
CREATE TABLE mc_registry (
    mc_id           TEXT PRIMARY KEY,
    mc_index        INT  NOT NULL UNIQUE,
    read_table_arn  TEXT NOT NULL,
    read_stream_arn TEXT,
    state           TEXT NOT NULL CHECK (state IN ('active', 'draining', 'retired')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### 3.2 Composite resourceVersion

`e7|b2:1044,b5:902,b9:4123` — timeline epoch prefix + per-bucket high-water map. Serialization is canonical (buckets sorted ascending) so equal states compare equal. Upholds **I3/I6**: the epoch increments on every promotion, so `(epoch, seq)` is monotonic even if a sequence were somehow rewound. A newly leased bucket has no entry → scoped List for that bucket only, merge `bN:seq`. Stale epoch or sub-horizon seq → `410 Gone` (**I7**).

### 3.3 Atomic Write Path

```sql
BEGIN;

-- (a) FENCE — upholds I4. FOR SHARE, held to COMMIT (see §3.4 for why).
SELECT 1 FROM bucket_leases
 WHERE bucket_id = $bucket AND domain = 'spec'
   AND holder = $replica_id AND epoch = $lease_epoch
   AND expires_at > now()
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
--     the poll). Transactional: fires only on COMMIT. Empty payload — the
--     watcher polls all pending changes regardless.
SELECT pg_notify('resource_changes_b' || $bucket, '');

COMMIT;
```

Client rules: a 409 from (c) must ROLLBACK (never retry inside the same txn — the counter increment must abort with it, preserving I1). Any ambiguous commit outcome (connection dropped mid-COMMIT) is resolved by reading back the row and `current_seq` before retrying — the write is idempotent to verify because `object_version` and seq identify it.

### 3.3b Atomic Status Write Path (Spec/Status Split)

In Kubernetes, spec (desired state) and status (observed state) are written by different controllers — e.g., the API server writes spec while a controller writes status. The system supports this with a `WriteStatus` path that mirrors §3.3 except:

1. **Fence against the `'status'` row in `bucket_leases`** (not the `'spec'` row). This gives status writers their own fencing domain — a status writer holds a status lease, a spec writer holds a spec lease, and neither interferes with the other.
2. **UPDATE only touches `status`** — `spec`, `metadata`, and `deletion_timestamp` are unchanged. There is no create path; the object must already exist (`ExpectedVersion > 0`).
3. **Same shared counter and `object_version`.** Both `Write()` and `WriteStatus()` increment the same `gvk_bucket_counters` row and bump the same `object_version` column on `kubernetes_resources`. This ensures watchers see a single gapless, ordered event stream covering both spec and status changes.

```sql
BEGIN;
-- (a) FENCE — against the status row in bucket_leases (independent from spec lease)
SELECT 1 FROM bucket_leases
 WHERE bucket_id = $bucket AND domain = 'status'
   AND holder = $replica_id AND epoch = $lease_epoch
   AND expires_at > now()
 FOR SHARE;

-- (b) SEQUENCE — same shared counter as Write()
INSERT INTO gvk_bucket_counters (bucket_id, gvk, current_seq)
VALUES ($bucket, $gvk, 1)
ON CONFLICT (bucket_id, gvk)
DO UPDATE SET current_seq = gvk_bucket_counters.current_seq + 1
RETURNING current_seq;               -- => $next_seq

-- (c) UPDATE status only — spec, metadata, deletion_timestamp unchanged
UPDATE kubernetes_resources
SET gvk_bucket_seq = $next_seq,
    object_version = object_version + 1,
    status = $status,
    updated_at = now()
WHERE gvk = $gvk AND namespace = $ns AND name = $name
  AND object_version = $expected_version;  -- zero rows => 409 Conflict

-- (d) DOORBELL — same channel as spec writes, empty payload
SELECT pg_notify('resource_changes_b' || $bucket, '');
COMMIT;
```

The fencing guarantees (I4) apply independently per domain: `FOR SHARE` on the `(bucket_id, 'status')` row blocks a status lease grant while a status write is in-flight (R11), just as `FOR SHARE` on the `(bucket_id, 'spec')` row blocks a spec lease grant (R1). A spec writer and a status writer can operate concurrently on the same bucket — the counter's exclusive row lock serializes sequence issuance (I1/I2), but the fence locks are on different rows and do not conflict (R12).

Most controllers own both spec and status for their resources. `BothManager` provides `AcquireBoth`/`RenewBoth`/`ReleaseBoth` — each is a single multi-row statement against `bucket_leases` (atomic without an explicit transaction). For the minority of controllers that split ownership (e.g., API server writes spec, a separate controller writes status), `NewSpecManager` and `NewStatusManager` operate independently.

### 3.3c No-Op Write Suppression

Content-equal writes consume no sequence number, emit no doorbell, and bump no `object_version`. This matches Kubernetes API-server semantics where an update that changes nothing does not advance resourceVersion. The feature is default-on; callers set `ForceWrite: true` to bypass it.

**Mechanism:** after the fence check (step a) and before the counter increment (step b), the writer reads the existing row by primary key within the same transaction:

```sql
SELECT spec, status, metadata, deletion_timestamp, object_version, uid
FROM kubernetes_resources
WHERE gvk = $gvk AND namespace = $ns AND name = $name;
```

If the row exists and all compared fields are equal to the incoming values, the transaction commits immediately (releasing the `FOR SHARE` lock) with no counter increment, no upsert, and no doorbell. The result is `WriteResult{Changed: false, ObjectVersion: existing, UID: existing, Seq: 0}`.

**Field comparison rules:**
- `Write()` compares all four content fields: `spec`, `status`, `metadata`, `deletion_timestamp`. JSONB equality (`=`) is key-order-insensitive; timestamp comparison uses `time.Equal()` to handle timezone normalization.
- `WriteStatus()` compares only `status`.

**Create-path behavior (ExpectedVersion == 0):** if the row already exists and content matches, the write is treated as a replayed create — returns `Changed: false` with the existing row's version and UID. If content differs, returns `ErrAlreadyExists` as before.

**`WriteResult.Changed`** indicates whether the write produced a new state. Callers can use this to skip downstream side-effects (e.g., the DynamoDB bridge skips doorbell emission on `Changed: false`).

**Invariants preserved:**
- **I1 (gapless):** suppressed writes don't touch the counter — no gap possible.
- **I2 (commit order):** no counter change, no ordering concern.
- **I4 (single writer):** the `FOR SHARE` lock is still held for the duration of the transaction, even on suppression. A lease grant blocks until the suppressed transaction commits (RB4g).
- **I5 (exactly-once delivery):** no event emitted for no state change — correct Kubernetes semantics.
- **I8 (optimistic concurrency):** content-equal ⇒ intent satisfied regardless of version.

**Performance:** one additional PK read per write. Under load tests with unique content per write (suppression finds "no match" and proceeds normally), no measurable regression — 1,167 writes/s single-bucket ceiling vs. 1,045 baseline.

**Test hook:** `AfterSuppressionCheck(ctx, tx, suppressed bool)` in the `TxHooks` interface enables deterministic interleaving tests (RB4g).

### 3.4 Lease Fencing — closing the expiry race (hardening added in v4)

The v3 fence checked the lease at transaction start, leaving a window: lease expires (or is reassigned) *after* the check but *before* COMMIT — a paused writer could commit under an epoch that is no longer authoritative.

v4 closes it with lock discipline, not timestamps:

* The writer's fence takes **`FOR SHARE`** on the lease row (`bucket_leases WHERE bucket_id=$b AND domain=$d`) and holds it to COMMIT.
* The coordinator's **grant/steal is an `UPDATE bucket_leases SET holder=$new, epoch=epoch+1 WHERE bucket_id=$b AND domain=$d`** — a row `UPDATE` requires the exclusive lock, which **conflicts with `FOR SHARE`**.

The same mechanism applies to both domains. The two rows are independent: a status lease grant does not block a spec writer, and vice versa — they are different rows with different row-level locks.

Consequence: a new epoch cannot be granted for a bucket while any in-flight fenced write on that bucket is between fence-check and COMMIT. Either the old writer's transaction commits first (it was still the legitimate holder for that write — I4 holds), or the coordinator's grant commits first and the late writer's fence finds the new epoch and aborts. There is no interleaving in which a stale-epoch write commits after a new epoch exists. Lease *renewal* by the current holder updates only `expires_at` and also serializes behind in-flight shares; renewals are cheap and infrequent (10 s cadence) so the contention is negligible.

Time is thus advisory (`expires_at` bounds how long a dead holder blocks reassignment); the epoch + lock conflict is the actual safety mechanism. Clock skew cannot violate I4 — at worst it delays a grant.

Writer regression tripwire (defense in depth for I3): each writer caches its highest committed seq per (GVK, bucket); on reconnect it reads `current_seq` and refuses + alarms if lower. With synchronous Multi-AZ this must never fire; if it fires, halt writes for that bucket and page.

### 3.5 List

Single `REPEATABLE READ` transaction: read `cluster_epoch` + counters (build RV), then live rows via the partial index, COMMIT. Snapshot and RV are the same instant — no skew window (supports I5/I6 handoff into Watch).

### 3.6 Watch — Single-Goroutine Poll-Primary with Doorbell

Polling is the correctness mechanism (**I5**); the doorbell only changes *when* a poll happens.

**Poll cycle** per (GVK, leased bucket): a single **REPEATABLE READ read-only transaction** per poll cycle covers the epoch check, per-bucket compaction horizon checks, and all row queries. This snapshot isolation means mid-poll compaction is invisible (B3 defense). `SELECT ... WHERE gvk=$1 AND bucket_id=$b AND gvk_bucket_seq > $hwm ORDER BY gvk_bucket_seq ASC` (served by `idx_resources_watch`, no sort). Dispatch Added/Modified/Deleted (tombstone ⇒ Deleted); advance the high-water mark per row. Rapid updates to one object coalesce naturally — the delivered sequence numbers are not contiguous under coalescing, which is correct Kubernetes watch semantics (I5).

**Single-goroutine scheduler** — one loop owns all polling, one timer, and local state (`lastPoll`, `doorbellPending`). The listen goroutine only forwards notifications into a 1-buffered channel; it uses a child context that is cancelled when the main loop exits, ensuring prompt shutdown on poll errors. The `hwm` map is never accessed concurrently (R13 defense).

**Scheduling** — three triggers, one timer:
1. **Baseline timer: 5 s** unconditional (liveness backstop; sole guarantee under doorbell loss).
2. **Doorbell:** `LISTEN resource_changes_b{N}`; any notification for a leased bucket requests an early poll.
3. **Debounce floor 100 ms, leading + trailing edge.** If `time.Since(lastPoll) >= DebounceFloor` → leading edge, poll immediately. Otherwise → trailing edge: set `doorbellPending`, reset timer to `lastPoll + DebounceFloor`. Every poll error (including epoch mismatch) terminates `Run` uniformly — no error is silently swallowed (R14 defense).

**Doorbell loss:** on any LISTEN drop (including failover) reconnect, re-LISTEN; the next baseline poll reconciles. No catch-up/stream ordering hazard exists — there is only the poll.

**Bookmarks:** each cycle (even empty) may emit current per-bucket hwm as a progress event so informers advance RV without relist.

### 3.7 Tombstone Compaction

A **single CTE** atomically deletes tombstones and advances the compaction horizon — the horizon must never lag the physical delete, or a watcher could see an unexplained gap (I7):

```sql
WITH deleted AS (
    DELETE FROM kubernetes_resources
    WHERE gvk = $1 AND bucket_id = $2
      AND deletion_timestamp IS NOT NULL
      AND deletion_timestamp < now() - $retention
    RETURNING gvk_bucket_seq
)
INSERT INTO compaction_horizon (bucket_id, gvk, compacted_seq)
VALUES ($2, $1, (SELECT COALESCE(MAX(gvk_bucket_seq), 0) FROM deleted))
ON CONFLICT (bucket_id, gvk)
DO UPDATE SET compacted_seq = GREATEST(
    compaction_horizon.compacted_seq,
    EXCLUDED.compacted_seq
);
```

Default retention: 24 h. Retention must exceed the slowest legitimate watcher resume interval; enforce with an alarm on watcher hwm age approaching retention/2. The `GREATEST` in the upsert ensures the horizon never moves backwards even under concurrent compaction runs.

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
* **R2 — Debounce swallow (latency only, but test anyway).** Doorbell arrives during a poll cycle. *Defense:* single-goroutine scheduler with leading/trailing debounce (§3.6) — a doorbell during a poll sets `doorbellPending`, guaranteeing a trailing poll after DebounceFloor. *Test:* inject a doorbell during the poll snapshot window; assert a trailing poll follows within DebounceFloor. Run under `-race`.
* **R3 — Doorbell loss (I5).** LISTEN connection drops silently; notifications lost. *Defense:* poll-primary. *Test:* proxy kills the LISTEN socket mid-burst without client error; assert every event still delivered within baseline interval, zero dups.
* **R4 — Counter first-write race (I1).** Two txns race the counter's first INSERT. *Defense:* `ON CONFLICT` upsert under the unique PK. *Test:* two sessions insert concurrently; assert seqs are exactly {1, 2}.
* **R5 — Ambiguous commit (I1/I5).** Connection drops during COMMIT; client doesn't know if the write landed. *Defense:* read-back protocol (§3.3). *Test:* proxy drops the connection after COMMIT is sent but before the OK; assert the client's read-back + retry yields exactly one committed state change and no seq is skipped or double-issued.
* **R6 — Lease handover overlap (I4/I5).** Old holder's last write vs. new holder's first write on the same bucket. *Defense:* R1 mechanism + new holder's scoped List starts from post-grant counter state. *Test:* scripted handover under write load; verification watcher asserts a single totally-ordered gapless stream across the handover.
* **R7 — Compaction vs. slow watcher (I7).** Watcher resumes with hwm just below a freshly advanced horizon. *Defense:* horizon advanced transactionally with the delete; boundary check on poll. *Test:* freeze a watcher, compact past its hwm, resume; assert `410 Gone` (never a silent gap). Also the off-by-one: hwm == horizon exactly must succeed.
* **R8 — Failover mid-transaction (I1–I3).** Failover strikes between counter increment and COMMIT. *Defense:* the whole txn aborts atomically; sync standby has all acknowledged commits. *Test:* Phase 6 drill with writes in flight; assert no gap (aborted increment leaves none) and no regression.
* **R9 — RV backwards exposure (I6).** Client presents an RV from a previous timeline epoch after failover. *Defense:* epoch comparison → `410 Gone`, relist. *Test:* replay a pre-failover RV post-failover; assert rejection, never a partial stream.
* **R10 — 409 handling corrupting the stream (I1).** Buggy client retries the upsert inside the same txn after a version conflict. *Defense:* client library makes it structurally impossible (txn helper owns BEGIN/COMMIT); assert in code review + a library test that a conflict always rolls back the counter increment.
* **R11 — Status fence-expiry race (I4).** Mirrors R1 for the status write path: `FOR SHARE` on the `bucket_leases` status row (`domain='status'`) must block a coordinator's grant while a status writer is mid-transaction. *Defense:* same §3.4 share-lock mechanism on the status domain row. *Test:* session A does `WriteStatus()` with hook pausing at BeforeCommit, session B attempts Grant on the status lease — must block; unblock A, verify B completes; A's next `WriteStatus()` with the old epoch returns fence violation.
* **R12 — Concurrent spec/status writes (I1/I2).** holder-A writes spec, holder-B writes status to the same resource. The shared counter must produce a gapless sequence; the watcher must see the correct stream; cross-domain fencing must be enforced (holder-B cannot Write, holder-A cannot WriteStatus). *Defense:* shared `gvk_bucket_counters`, independent `bucket_leases` rows (different `domain` values). *Test:* interleaved spec and status writes produce consecutive seqs {1,2,3,4}; watcher starting from hwm sees correct state; holder-B's Write attempt is fenced; holder-A's WriteStatus attempt is fenced.
* **R13 — Single-goroutine poll serialization (I5).** Rapid doorbell bursts overlapping with the baseline timer must not produce concurrent poll cycles — the `hwm` map is not safe for concurrent access and concurrent polls could deliver events out of order. *Defense:* single-goroutine scheduler (§3.6) — only one goroutine reads `hwm`, the listen goroutine only forwards into a buffered channel. *Test:* fire 10 rapid doorbells while baseline timer is due; assert exactly one poll executes at a time and events arrive in seq order.
* **R14 — Epoch mismatch on doorbell-triggered poll (I6/I7).** A doorbell triggers a poll after the cluster epoch has been bumped. The watcher must terminate with 410 Gone, not silently swallow the error. *Defense:* every poll error (including epoch mismatch) causes `Run()` to return, closing the events channel — the watch adapter sees the channel close and propagates (§3.6). The listen goroutine uses a child context that is cancelled when `Run()` exits, ensuring prompt shutdown. *Test:* start watcher, bump `cluster_epoch`, trigger a doorbell to force a poll; assert the watcher terminates and the events channel closes within 5 s.
* **R15 — Mid-poll compaction (I7).** Compaction runs and advances the horizon while a watcher is mid-poll. The watcher must not see an inconsistent state (some rows deleted, horizon advanced, within a single poll cycle). *Defense:* REPEATABLE READ snapshot isolation — the poll transaction sees the database as of its snapshot instant; the compaction's DELETE and horizon UPDATE are invisible within the poll cycle. *Test:* start a watcher poll (paused via hook), run compaction in a separate session, resume the poll; assert the watcher sees all pre-compaction rows and no unexplained gaps.
* **RB4 — No-op write suppression (I1/I4/I5).** Content-equal writes must be suppressed without violating any invariant. Seven sub-cases:
  - **RB4a** — Identical write suppressed: no seq consumed, no version bump, counter unchanged.
  - **RB4b** — Real change after no-op correctly sequenced (gets next seq, watcher sees exactly one event).
  - **RB4c** — Watcher sees no event for suppressed write (I5).
  - **RB4d** — Replayed create with identical content suppressed (counter unchanged).
  - **RB4e** — WriteStatus suppression (only status field compared).
  - **RB4f** — ForceWrite bypasses suppression.
  - **RB4g** — Suppression under fence interleaving (I4): `AfterSuppressionCheck` hook pauses a suppressed write; grant attempt must block because `FOR SHARE` is held. *Defense:* same §3.4 share-lock — the suppressed transaction holds the share lock for its entire lifetime, even though it performs no counter increment or upsert. *Test:* session A writes identical content (suppressed), paused after suppression check; session B attempts Grant — must block; unblock A, verify A completes as no-op, B completes after A.

## 6. Continuous Invariant Verification (production, not just tests)

Correctness that is only tested pre-GA decays. Run a **verifier** as a permanent, low-priority consumer in every environment:

* Subscribes (via the ordinary poll path) to a sample of buckets — including the hottest — and checks per (GVK, bucket):
  - **I3/I6 — monotonic high-water marks:** `seq > prevHWM` per bucket. Any regression is an invariant violation.
  - **I7 — gaps explained by compaction:** all gaps between delivered seq numbers are below the compaction horizon.
* **No per-event gap checking (I1).** Under coalescing (two writes to the same key between polls), the delivered sequence numbers are not contiguous — only the latest seq per object survives. This is correct Kubernetes watch semantics (I5 permits coalescing). Gap auditing, if needed, must cross-check the table directly (out of scope for the stream-side verifier).
* **Duplicate detection** uses monotonicity: `seq <= prevHWM` is reported as an I3 violation. No per-key map is maintained — verifier state is **O(buckets)**, bounded.
* A second probe writes a synthetic canary object per bucket at low rate and measures write→delivery latency (doorbell health) via a **bounded ring buffer** (1,000 samples) with p99 tracking.
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
