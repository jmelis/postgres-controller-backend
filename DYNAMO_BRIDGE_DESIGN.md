# DynamoDB Streams → PostgreSQL Bridge — Design

**Status:** Proposed · **Depends on:** DESIGN.md v4 (invariants I1–I8, lease fencing, gapless counters)

**Goal:** replicate client writes from DynamoDB into PostgreSQL through the existing transactional write path, preserving every correctness invariant. The bridge consumer IS the lease holder — it inherits all fencing, counter, and concurrency guarantees by construction.

---

## 1. Architecture

```
Client ──► DynamoDB Table
               │
               ▼
         DynamoDB Streams (CDC, ordered per partition key)
               │
               ▼
         Bridge Consumer (Go, long-running)
           ├── Shard discovery & lifecycle
           ├── Per-shard GetRecords polling
           ├── Partition key → bucket_id (deterministic hash)
           ├── Lease holder (spec domain, per assigned bucket)
           ├── Object version cache (from List on startup)
           ├── writer.Write() — same txn as any controller
           └── Checkpoint table in Postgres
               │
               ▼
         PostgreSQL (authoritative for controller-runtime)
           ├── bucket_leases       (consumer holds spec leases)
           ├── gvk_bucket_counters (gapless sequences)
           ├── kubernetes_resources (resource state)
           └── stream_checkpoints  (DynamoDB stream position)  ← new table
```

The consumer is a plain Go process. It holds bucket leases, reads DynamoDB stream shards, and calls `writer.Write()` for each record — the same code path every controller uses. From PostgreSQL's perspective, the consumer is just another writer.

---

## 2. Stream Record → WriteRequest Mapping

### 2.1 Event types

| DynamoDB eventName | Action |
|---|---|
| `INSERT` | `writer.Write()` with `ExpectedVersion = 0` (create) |
| `MODIFY` | `writer.Write()` with `ExpectedVersion = cached version` (update) |
| `REMOVE` | `writer.Write()` with `DeletionTimestamp` set (tombstone) |

The consumer extracts `gvk`, `namespace`, `name`, `spec`, `status`, and `metadata` from the stream record's `NewImage`. The mapping function is caller-provided — the bridge owns the lifecycle and write mechanics, not the schema interpretation.

### 2.2 Bucket assignment

```
bucket_id = fnv32a(partitionKey) % numBuckets
```

Deterministic (same key → same bucket across restarts), stable (no dependency on shard topology), and balanced (FNV-1a distributes well). The consumer must hold leases for every bucket it can produce — for 16 buckets, one consumer holds all 16.

---

## 3. Object Version Tracking

The consumer maintains an in-memory map: `(gvk, namespace, name) → object_version`.

### 3.1 Initialization

After acquiring leases (§5), the consumer calls `reader.List()` for each `(GVK, bucket)` pair and seeds the cache from each resource's `ObjectVersion`.

### 3.2 Steady state

Each successful `writer.Write()` returns a `WriteResult`; the consumer updates its cache:

```
result, err := writer.Write(ctx, req)
// req.ExpectedVersion came from the cache
// result.ObjectVersion goes back into it
versionCache[key] = result.ObjectVersion
```

### 3.3 ErrConflict — version mismatch

If a concurrent status writer (independent lease domain) bumps `object_version` between the consumer's cached version and its write, the write returns `ErrConflict`.

Resolution (bounded retry, max 3):

```
loop:
    resource = readBack(gvk, ns, name)
    versionCache[key] = resource.ObjectVersion
    retry Write with corrected ExpectedVersion
```

Convergence is guaranteed: the consumer holds the spec lease, so no other spec writer can interleave. Only status writes (different domain) can bump the version. The counter's exclusive row lock serializes all writes, so at most one status write lands per retry.

### 3.4 ErrAlreadyExists — replayed create

On replay of a `CREATE` (`ExpectedVersion = 0`), the resource was already created by a prior (successful but uncheckpointed) attempt. Resolution: read the current resource. If it exists, convert to an update — read its `object_version`, set `ExpectedVersion` to that, and write the record's content as an update.

---

## 4. Checkpointing & Replay Tolerance

### 4.1 Checkpoint table

```sql
CREATE TABLE stream_checkpoints (
    stream_arn   TEXT        NOT NULL,
    shard_id     TEXT        NOT NULL,
    last_seq_num TEXT        NOT NULL,
    holder_id    TEXT        NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stream_arn, shard_id)
);
```

### 4.2 Protocol

After each `GetRecords` batch (up to 1,000 records):

1. Process records sequentially through the write path.
2. After the final record commits: update `stream_checkpoints` with the batch's last `SequenceNumber`.

On restart: `GetShardIterator(AFTER_SEQUENCE_NUMBER, last_seq_num)` resumes from the checkpoint. If no checkpoint exists: `TRIM_HORIZON` (start from oldest available record).

### 4.3 Why batch-level checkpointing is safe

On crash mid-batch, the uncheckpointed records replay. Each replayed record hits one of:

- **Write succeeds** — produces a harmless duplicate watch event. Watchers are idempotent (Kubernetes contract). Gapless issuance (I1) is preserved: the extra write consumes a sequence number but leaves no gap.
- **ErrAlreadyExists** — resolved by §3.4 (convert to update).
- **ErrConflict** — resolved by §3.3 (re-read version, retry).

No replay can corrupt the stream or violate an invariant. The worst case is ~1 batch worth of duplicate watch events (~1s of data at steady-state RPS).

### 4.4 Why per-record checkpointing is not worth the cost

Per-record checkpointing adds one Postgres write per DynamoDB record. At 1,870 RPS (50k-cluster burst), this doubles the write load against the database. Batch-level checkpointing adds one write per `GetRecords` call (~2/s per shard) — negligible overhead. The correctness cost (harmless duplicates on crash) is near zero.

---

## 5. Lease Lifecycle

### 5.1 Startup sequence

```
1. Acquire spec lease for each assigned bucket
     epoch[b] = leaseManager.Acquire(ctx, bucketID, ttl)   // ttl = 30s

2. Seed version cache
     for each (gvk, bucket):
         list = reader.List(ctx, conn, gvk, []int{bucket})
         for each resource in list:
             versionCache[key] = resource.ObjectVersion

3. Load checkpoints
     SELECT last_seq_num FROM stream_checkpoints
       WHERE stream_arn = $arn AND shard_id = $shard

4. Start shard iterators from checkpoints (or TRIM_HORIZON)

5. Start lease renewal goroutine (every ttl/3 = 10s)

6. Enter processing loop
```

### 5.2 Renewal

A background goroutine calls `leaseManager.Renew()` every `ttl/3`. On failure:

- **Network error:** retry once, then stop processing (writes would fail anyway).
- **ErrNotHolder:** lease was stolen. Stop processing immediately — the next `Write()` would return `ErrFenceViolation`. This is the correct response; the coordinator reassigned the bucket.

### 5.3 Graceful shutdown

```
1. Stop processing loop (finish in-flight record, not in-flight batch)
2. Checkpoint the last successfully-processed sequence number
3. Release leases: leaseManager.Release(ctx, bucketID)
4. Close Postgres connections
```

Grace period: 10s. After that, leases expire naturally (TTL). The successor consumer acquires after TTL and resumes from the last checkpoint.

---

## 6. Shard Management

### 6.1 Shard properties

- Shards have parent-child lineage. A parent shard must be fully processed before its children.
- Closed shards (both `StartingSequenceNumber` and `EndingSequenceNumber` present) are finite — process to exhaustion, then move to children.
- Open shards are processed continuously (long-poll `GetRecords`).
- Shard iterators expire after 15 minutes of inactivity — re-acquire from last checkpoint on `ExpiredIteratorException`.

### 6.2 Discovery

Every 60s, call `DescribeStream` to discover new shards. Build a DAG from `ParentShardId`. Process in topological order.

### 6.3 Shards vs. buckets

Shards do NOT map to buckets. A single shard contains records for multiple partition keys, which may hash to different buckets. Bucket assignment is per-record (§2.2), not per-shard. The consumer holds leases for all buckets it might write to.

---

## 7. Consumer Core Loop (pseudocode)

```
func (c *Consumer) processShardLoop(ctx, shardID, iterator):
    for ctx is not cancelled:
        records, nextIterator = dynamodb.GetRecords(iterator, limit=1000)
        if error is ExpiredIteratorException:
            iterator = reacquireIterator(shardID, lastCheckpoint)
            continue

        for each record in records:
            writeReq = c.mapper.ToWriteRequest(record)
            writeReq.LeaseHolder = c.holderID
            writeReq.LeaseEpoch = c.epoch[writeReq.BucketID]
            writeReq.ExpectedVersion = c.versionCache.Get(key(writeReq))

            result, err = c.writeWithRetry(ctx, writeReq)
            if err != nil:
                // permanent failure after retries → dead-letter + skip
                c.deadLetter(record, err)
                continue

            c.versionCache.Set(key(writeReq), result.ObjectVersion)
            lastSeqNum = record.SequenceNumber

        if len(records) > 0:
            c.updateCheckpoint(shardID, lastSeqNum)

        if nextIterator == nil:
            break  // closed shard exhausted
        iterator = nextIterator

        if len(records) == 0:
            sleep(250ms)  // backoff on empty reads


func (c *Consumer) writeWithRetry(ctx, req):
    for attempt = 0; attempt < 3; attempt++:
        result, err = c.writer.Write(ctx, req)
        switch err:
            case nil:
                return result, nil
            case ErrConflict:
                resource = c.writer.ReadBack(ctx, req.GVK, req.Namespace, req.Name, 0)
                req.ExpectedVersion = resource.ObjectVersion
                continue
            case ErrAlreadyExists:
                resource = c.writer.ReadBack(ctx, req.GVK, req.Namespace, req.Name, 0)
                req.ExpectedVersion = resource.ObjectVersion
                continue
            case ErrFenceViolation:
                return error  // fatal: lease lost, stop consumer
            default:
                return error  // network error, stop consumer
    return ErrMaxRetries
```

---

## 8. Failure Modes

| Failure | Impact | Recovery |
|---------|--------|----------|
| Consumer crash | Writes stop; DynamoDB stream records buffer (24h retention) | Successor acquires leases, resumes from checkpoint; replayed records handled by §3.3/§3.4 |
| Postgres down (RDS failover) | Writes fail; consumer pauses; records buffer in stream | Consumer retries when Postgres recovers; stream 24h retention exceeds typical RDS failover (~2 min) |
| Lease stolen | `Write()` returns `ErrFenceViolation`; consumer stops | Correct behavior — coordinator reassigned the bucket; successor resumes |
| Two consumers, same bucket | Old consumer's writes fenced by `FOR SHARE` / epoch conflict (§8.1) | No stale-epoch write commits; exactly one consumer writes at any instant |
| DynamoDB Streams outage | No new records; Postgres state freezes | Resume when streams recover; per-partition-key ordering preserved |
| Shard iterator expires (15 min idle) | `ExpiredIteratorException` on `GetRecords` | Re-acquire iterator from last checkpoint (`AFTER_SEQUENCE_NUMBER`) |

### 8.1 Split-brain protection (inherited from DESIGN.md §3.4)

Two consumers might briefly believe they own the same bucket (old consumer hasn't detected reassignment). The existing `FOR SHARE` fencing handles this:

1. Old consumer's `Write()` holds `FOR SHARE` on the `bucket_leases` row until COMMIT.
2. New consumer's `Acquire()` issues `UPDATE bucket_leases SET epoch = epoch+1` — needs exclusive lock, **blocked** by the share lock.
3. Old consumer's transaction completes → share lock released.
4. New consumer's `Acquire()` commits → epoch incremented.
5. Old consumer's next `Write()` finds epoch mismatch → `ErrFenceViolation` → stops.

No interleaving exists in which a stale-epoch write commits after a new epoch. The consumer inherits this guarantee by using `writer.Write()`.

---

## 9. Correctness Analysis

| Invariant | How the consumer preserves it |
|-----------|-------------------------------|
| **I1 — Gapless issuance** | Uses `writer.Write()`, which increments the counter in the same transaction as the upsert. Aborted writes leave no gap. Replays consume extra sequence numbers but never create gaps. |
| **I2 — Commit order = sequence order** | The counter's exclusive row lock serializes all writes in the same `(GVK, bucket)`. The consumer's writes serialize identically to any other writer. |
| **I3 — No regression** | Synchronous Multi-AZ + writer tripwire — inherited unchanged. |
| **I4 — Single writer** | Consumer holds the spec lease. `writer.Write()` performs the `FOR SHARE` fence check on every transaction. Split-brain resolved by §8.1. |
| **I5 — Exactly-once delivery** | Watch path is entirely unchanged — poll-primary, single-goroutine scheduler, snapshot isolation. The consumer's writes are just writes from the watcher's perspective. Replayed records may produce duplicate watch events; watchers tolerate this (Kubernetes contract). |
| **I6 — RV monotonicity** | Timeline epochs are checked in every poll cycle. The consumer's writes carry the current epoch via the normal write path. |
| **I7 — Compaction safety** | Compaction CTE + horizon check are unchanged. Compaction operates on `kubernetes_resources` regardless of the write's origin. |
| **I8 — Optimistic concurrency** | Consumer tracks `object_version` in its cache (§3). Conflicts from concurrent status writers are resolved by bounded retry (§3.3). No lost updates. |

### 9.1 New invariant

**I9 — Stream completeness.** Every DynamoDB stream record for a leased bucket is eventually written to Postgres, absent permanent failure of DynamoDB Streams or Postgres.

Upheld by:
- DynamoDB Streams: at-least-once delivery, 24h retention.
- Checkpoint protocol (§4): no record is permanently skipped.
- Replay tolerance (§3.3/§3.4): replayed records converge.
- Lease lifecycle (§5): continuous processing while leased; successor resumes on handover.

### 9.2 Ordering semantics

DynamoDB Streams guarantees per-partition-key ordering within a shard (parents before children). The consumer processes shards in topological order (§6.2), so for the **same resource**, the Postgres commit order matches the DynamoDB write order.

For **different resources**, the Postgres sequence order reflects the consumer's processing order, not the DynamoDB write order. This is acceptable: the watcher sees events in commit order (I2), which is a valid total order. No invariant requires that the Postgres order mirrors an external system's order.

---

## 10. Performance

### 10.1 Throughput budget

| Tier | Steady RPS | Per-bucket (16 buckets) | Bucket ceiling | Headroom |
|------|-----------|------------------------|----------------|----------|
| 5,000 clusters | 187 | ~12 writes/s | 1,045 writes/s | 87× |
| 50,000 clusters | 1,870 | ~117 writes/s | 1,045 writes/s | 9× |

DynamoDB Streams: `GetRecords` returns up to 1,000 records, at most 2 calls/s per shard. With 4 shards, theoretical ceiling is ~8,000 records/s — not the bottleneck.

### 10.2 End-to-end latency

| Segment | Latency |
|---------|---------|
| DynamoDB Streams propagation | ~100ms–1s |
| Consumer processing (map + write) | ~15–33ms (p50–p99 from load tests) |
| **Total** | **~200ms–2s** |

### 10.3 Postgres connection usage

Per consumer instance:
- 1 `*pgx.Conn` for writes (reused across all records; reconnect on error)
- 1 `*pgx.Conn` for lease management / checkpoints
- 1 `*pgx.Conn` for `reader.List()` (startup only, then closed)

The consumer uses `writer.Writer` directly (not `crbridge.Client`) because:
- Long-lived connections — no per-call connection factory overhead
- Fine-grained control over retry and error handling
- `crbridge.Client` is shaped for controller-runtime reconcilers, not CDC consumers

---

## 11. Schema Addition

One new table. No changes to existing tables.

```sql
CREATE TABLE stream_checkpoints (
    stream_arn   TEXT        NOT NULL,
    shard_id     TEXT        NOT NULL,
    last_seq_num TEXT        NOT NULL,
    holder_id    TEXT        NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stream_arn, shard_id)
);
```

---

## 12. Open Questions

1. **DynamoDB item schema**: the consumer needs a mapping function `StreamRecord → WriteRequest`. This depends on how resources are stored in DynamoDB (partition key layout, attribute names). The bridge provides the lifecycle; the caller provides the mapper.

2. **Status ownership**: if the DynamoDB path carries both spec and status, the consumer writes both via `Write()`. If status is owned by a separate controller (Assumption 4 in README), the consumer writes spec only and the status controller holds its own lease independently. Which pattern applies?

3. **Consumer fleet size**: at 16 buckets, one consumer holds all leases. At higher bucket counts, partition the bucket space across consumer instances — each holds a disjoint subset of leases. The existing lease mechanism handles coordination (no external orchestrator needed).

4. **Dead letter policy**: records that fail after retries (§7 pseudocode) should be sent to a dead-letter destination (SQS queue or Postgres error table) rather than blocking the shard. Define max retries (default: 3) and DLQ target.

5. **Deletion & compaction alignment**: DynamoDB `REMOVE` → tombstone write. The compaction retention (24h) must exceed the DynamoDB stream replay window (also 24h). If compaction runs on a tombstone before its stream record could possibly replay, a replayed `REMOVE` would find nothing to tombstone. Consider `retention ≥ 48h` or detect-and-skip missing tombstone targets.

6. **Monitoring**: expose metrics for DynamoDB iterator age, write success/failure rate, checkpoint lag, version cache hit rate, and lease renewal health. Alert if iterator age > 5 minutes (consumer falling behind) or lease renewal failure.
