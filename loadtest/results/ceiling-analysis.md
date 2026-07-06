# Write Ceiling Analysis — postgres-controller-backend

**Date:** 2026-07-05
**Region:** us-east-1
**Engine:** PostgreSQL 16.4
**Storage:** gp3, 100 GB (baseline 3,000 IOPS / 125 MiBps)
**Write path:** fence check (FOR SHARE) -> no-op suppression -> counter increment -> UPSERT -> pg_notify -> sync COMMIT (5 round-trips + 1 sync commit per write)
**Workers per bucket:** 1 (matches production: exactly 1 lease holder per bucket)

## Raw Results

### Test 1: db.r6g.large (2 vCPU, 16 GB) — Multi-AZ

| Buckets | RPS   | p50     | p99      |
|---------|-------|---------|----------|
| 1       | 363.4 | 2.6ms   | 4.2ms    |
| 4       | 679.5 | 5.7ms   | 10.3ms   |
| 8       | 682.8 | 11.4ms  | 18.4ms   |
| 16      | 685.6 | 22.9ms  | 33.3ms   |
| 32      | 654.3 | 46.1ms  | 99.9ms   |
| 64      | 655.5 | 92.9ms  | 148.2ms  |

**Ceiling: ~685 w/s (reached at 4 buckets)**

### Test 2: db.m6g.2xlarge (8 vCPU, 32 GB) — Multi-AZ

| Buckets | RPS   | p50     | p99      |
|---------|-------|---------|----------|
| 1       | 209.5 | 4.7ms   | 5.4ms    |
| 4       | 705.9 | 5.7ms   | 6.9ms    |
| 8       | 694.5 | 11.4ms  | 14.1ms   |
| 16      | 685.0 | 23.0ms  | 27.4ms   |
| 32      | 677.5 | 46.7ms  | 54.0ms   |
| 64      | 683.7 | 92.6ms  | 103.6ms  |

**Ceiling: ~706 w/s (reached at 4 buckets)**

### Test 3: db.r6g.large (2 vCPU, 16 GB) — Single-AZ

| Buckets | RPS   | p50     | p99      |
|---------|-------|---------|----------|
| 1       | 243.8 | 4.0ms   | 5.0ms    |
| 4       | 914.0 | 4.2ms   | 6.7ms    |
| 8       | 971.4 | 8.0ms   | 13.1ms   |
| 16      | 961.8 | 16.2ms  | 24.9ms   |
| 32      | 868.6 | 32.6ms  | 154.4ms  |
| 64      | 934.1 | 65.8ms  | 95.2ms   |

**Ceiling: ~970 w/s (reached at 8 buckets)**

### Local reference (MacBook, no network)

| Buckets | Workers | RPS    |
|---------|---------|--------|
| 1       | 10      | 1,384  |
| 16      | 48      | 2,754  |

## Analysis

### The bottleneck stack

Three ceilings constrain write throughput, in order of dominance:

1. **Multi-AZ synchronous replication (~2-3ms per commit)**
   Each sync COMMIT waits for the standby in another AZ to confirm WAL receipt.
   This is a fixed network round-trip that cannot be parallelized — it's per-transaction.
   Evidence: 4x-ing CPU (2 -> 8 vCPU) gave only 3% improvement (685 -> 706 w/s).

2. **CPU (2 vCPU on r6g.large)**
   Once sync replication is removed (Single-AZ), throughput jumps 42% (685 -> 970 w/s)
   but doesn't reach local-test levels (2,754 w/s). The 2 CPUs are now saturated handling
   the write transactions and WAL processing.

3. **Local WAL fsync to gp3 (~0.5-1ms per commit)**
   Even on Single-AZ, each commit must fsync to durable storage. gp3 baseline gives
   3,000 IOPS — at ~970 w/s with 5 round-trips each, we're approaching that.

### Why adding buckets doesn't help past 4

Each bucket has exactly 1 writer (the lease holder). Adding buckets adds parallelism —
but the shared resource (CPU / replication / storage IOPS) saturates quickly.

- At 1 bucket: single-threaded, one CPU core busy, the other idle
- At 4 buckets: both CPUs saturated, ceiling reached
- At 8+ buckets: same throughput, but latency rises linearly (more connections contending
  for the same CPU time)

### Why latency rises linearly with bucket count

With N buckets at the ceiling, each write waits in a queue for CPU time.
Average wait = (N-1) * per-write-time / 2. This is visible in the data:
- 4 buckets: p50 = 5.7ms
- 8 buckets: p50 = 11.4ms (2x)
- 16 buckets: p50 = 22.9ms (4x)
- 32 buckets: p50 = 46.1ms (8x)

Linear scaling = classic CPU queueing, not lock contention.

## Production Implications

### Fleet sizing (Multi-AZ, which production requires)

| Metric | Value |
|--------|-------|
| Write ceiling (Multi-AZ) | ~685 w/s |
| Writes per reconcile | 1 |
| Reconcile interval | 10 min = 600s |
| Writes per cluster per second | 2 GVKs / 600s = 0.0033 |
| Steady-state bursts (scale-up, etc.) | ~10x = 0.037 w/s per cluster |
| Max clusters at 100% utilization | 685 / 0.037 = 18,500 |
| Max clusters at 60% headroom | **~11,100** |
| Max clusters at 40% headroom | **~7,400** |

### Instance class doesn't matter (for writes)

The Multi-AZ sync commit dominates. db.r6g.large (2 vCPU, ~$0.43/hr) gives the same
write throughput as db.m6g.2xlarge (8 vCPU, ~$0.87/hr). Upsizing the RDS instance
does NOT increase write throughput.

A larger instance only helps for:
- Read throughput (LIST/WATCH queries, which are not write-path bound)
- More connections (pgbouncer is better for this)
- More memory for shared_buffers / OS page cache

### What WOULD increase the write ceiling

| Option | Expected improvement | Trade-off |
|--------|---------------------|-----------|
| **Disable Multi-AZ** | +42% (685 -> 970 w/s) | No HA — failover requires manual intervention or restore from backup |
| **Batch writes** | 2-5x | Requires application-level changes to group multiple writes per COMMIT |
| **Async commit** | ~2x | Risk of losing last ~100ms of committed data on crash |
| **Reduce round-trips** | ~30-50% | Collapse fence+counter+upsert into a single stored procedure |
| **Horizontal sharding** | Linear | Multiple RDS instances, each owning a subset of buckets — adds operational complexity |
| **Switch to io2 storage** | Marginal | Lower fsync latency, but replication still dominates in Multi-AZ |

### Recommendation

**db.r6g.large with Multi-AZ is the right production config.** At ~685 w/s it supports
~11,000 clusters at 60% headroom. That's well beyond current fleet sizes. The write
ceiling is architecturally determined by the gapless-sequence design (I4 invariant) +
Multi-AZ sync replication, not by the instance class. Spending more on a bigger instance
won't help writes.

If write throughput ever becomes a bottleneck, the highest-ROI change is **collapsing the
5 round-trips into a stored procedure** (single round-trip per write), which would roughly
triple single-bucket throughput and raise the ceiling proportionally.
