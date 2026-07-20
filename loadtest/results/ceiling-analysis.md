# Write Ceiling Analysis — postgres-controller-backend

**Date:** 2026-07-05 (pre-stored-procedure baseline), updated 2026-07-06 (post-stored-procedure)
**Region:** us-east-1
**Engine:** PostgreSQL 16.4
**Storage:** gp3, 100 GB (baseline 3,000 IOPS / 125 MiBps)
**Concurrent workers:** 1 per test step (sweep varies)

> **Note:** Tests 1–3 below were run with the old multi-statement write path (5 SQL round-trips + pg_notify inside the transaction). The stored procedure optimization recommended at the bottom has since been implemented — see the [loadtest README](../README.md) for current results (up to 15,322 w/s with small payloads; 3,932 w/s with realistic 15-20KB payloads on Aurora db.r6g.8xlarge).

## Raw Results

### Test 1: db.r6g.large (2 vCPU, 16 GB) — Multi-AZ

| Workers | RPS   | p50    | p99     |
| ------- | ----- | ------ | ------- |
| 1       | 363.4 | 2.6ms  | 4.2ms   |
| 4       | 679.5 | 5.7ms  | 10.3ms  |
| 8       | 682.8 | 11.4ms | 18.4ms  |
| 16      | 685.6 | 22.9ms | 33.3ms  |
| 32      | 654.3 | 46.1ms | 99.9ms  |
| 64      | 655.5 | 92.9ms | 148.2ms |

**Ceiling: ~685 w/s (reached at 4 workers)**

### Test 2: db.m6g.2xlarge (8 vCPU, 32 GB) — Multi-AZ

| Workers | RPS   | p50    | p99     |
| ------- | ----- | ------ | ------- |
| 1       | 209.5 | 4.7ms  | 5.4ms   |
| 4       | 705.9 | 5.7ms  | 6.9ms   |
| 8       | 694.5 | 11.4ms | 14.1ms  |
| 16      | 685.0 | 23.0ms | 27.4ms  |
| 32      | 677.5 | 46.7ms | 54.0ms  |
| 64      | 683.7 | 92.6ms | 103.6ms |

**Ceiling: ~706 w/s (reached at 4 workers)**

### Test 3: db.r6g.large (2 vCPU, 16 GB) — Single-AZ

| Workers | RPS   | p50    | p99     |
| ------- | ----- | ------ | ------- |
| 1       | 243.8 | 4.0ms  | 5.0ms   |
| 4       | 914.0 | 4.2ms  | 6.7ms   |
| 8       | 971.4 | 8.0ms  | 13.1ms  |
| 16      | 961.8 | 16.2ms | 24.9ms  |
| 32      | 868.6 | 32.6ms | 154.4ms |
| 64      | 934.1 | 65.8ms | 95.2ms  |

**Ceiling: ~970 w/s (reached at 8 workers)**

### Local reference (MacBook, no network)

| Workers | Workers | RPS   |
| ------- | ------- | ----- |
| 1       | 10      | 1,384 |
| 16      | 48      | 2,754 |

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

### Why adding workers didn't help past 4 (old multi-statement path)

With the old 5-round-trip write path, each worker ran its own write transaction but the
shared resources (CPU + `pg_notify` global lock) saturated at 4 workers on r6g.large. The
`pg_notify` inside the transaction acquired a global exclusive lock on the notification
queue, serializing all concurrent commits regardless of worker count.

**This is no longer the case.** The stored procedure eliminated the extra round-trips
and moving `pg_notify` outside the transaction (via a debounced doorbell) removed the
global lock. With the current code, throughput reaches 15,322 w/s (RDS, small payloads)
and 3,932 w/s (Aurora, realistic 15-20KB payloads) at 48 workers on 8xlarge. The
remaining bottleneck is WAL volume per commit — payload size is now the primary variable.

## Production Implications

### Fleet sizing (Multi-AZ, which production requires)

Pre-stored-procedure (old baseline for reference):

| Metric                                    | Old (multi-statement) | Current (stored proc + debounced doorbell, 48 workers, 8xlarge) |
| ----------------------------------------- | --------------------- | --------------------------------------------------------------- |
| Write ceiling (RDS, 50B)                  | ~685 w/s              | **15,322 w/s**                                                  |
| Write ceiling (Aurora, 15-20KB)           | —                     | **3,932 w/s**                                                   |
| Burst rate per cluster                    | 0.0748 w/s            | 0.0748 w/s                                                      |
| Max clusters at burst (Aurora, realistic) | ~9,200                | **~53,000**                                                     |

### Payload size is the primary variable

With the stored procedure, the old per-commit round-trip bottleneck is eliminated. The
dominant factor is now **WAL volume per commit** — realistic 15-20KB payloads reduce
throughput 3-4x compared to small payloads. Aurora handles large payloads better than
RDS Multi-AZ (3,932 vs 1,728 w/s), though RDS is faster with small payloads (15,322 vs
11,061 w/s).

### What WOULD increase the write ceiling

| Option                  | Expected improvement                 | Trade-off                                                               | Status                                                           |
| ----------------------- | ------------------------------------ | ----------------------------------------------------------------------- | ---------------------------------------------------------------- |
| **Reduce round-trips**  | ~22x (685 -> 15,322 w/s, RDS small)  | —                                                                       | **Done.** `pgctl_write()` stored procedure + debounced doorbell. |
| **Reduce payload size** | ~4x (3,932 -> 15,322 w/s equivalent) | Requires application-level changes to reduce spec/status/metadata sizes | Application-dependent                                            |
| **Async commit**        | ~2x                                  | Risk of losing last ~100ms of committed data on crash                   | Next lever if needed                                             |
| **Batch writes**        | 2-5x                                 | Requires application-level changes to group multiple writes per COMMIT  | Not needed at current ceiling                                    |
| **Horizontal sharding** | Linear                               | Multiple RDS instances — adds operational complexity                    | Not needed                                                       |

### Recommendation (updated)

After implementing the stored procedure (`pgctl_write()`), autocommit, and the debounced doorbell, the ceiling rose from ~685 w/s to **15,322 w/s** (RDS, small payloads) and **3,932 w/s** (Aurora, realistic 15-20KB payloads). The primary bottleneck is now WAL volume per commit, not round-trips or CPU.

For production with realistic payloads on Aurora, the 5,000-cluster tier (374 burst w/s) has over 10x headroom. The 50,000-cluster tier (3,740 burst w/s) is within the measured ceiling on a single db.r6g.8xlarge.

The next optimization lever, if needed, is `synchronous_commit = off` (async commit) which would remove the WAL sync bottleneck. See [loadtest/README.md](../README.md) for the full current results.
