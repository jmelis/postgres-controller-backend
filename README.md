# postgres-controller-backend

A PostgreSQL-backed storage backend for Kubernetes controllers that manage fleet resources (5,000–50,000 managed clusters). Not a general-purpose etcd replacement — see Assumptions below.

See [DESIGN.md](DESIGN.md) for the full design, [WALKTHROUGH.md](WALKTHROUGH.md) for the narrative explanation, and [METRICS.md](METRICS.md) for Prometheus instrumentation.

## Assumptions

This design exploits constraints that a fleet controller satisfies but a general-purpose API server does not. If your use case doesn't match these, use [kine](https://github.com/k3s-io/kine).

1. **The controller owns the write path.** Writes come from controller-runtime reconcilers, not from arbitrary API clients. The controller knows which resources it manages and can accept bucket-level lease assignment.

2. **Fixed bucket count.** The number of buckets is set at deployment time. Each (GVK, bucket) pair gets its own sequence counter created on first use, so there is no single global sequence bottleneck. Changing the bucket count is an epoch-bump migration (all watchers 410 + relist).

3. **Single writer per bucket per sub-resource.** Each bucket has at most one spec writer and one status writer at any time, enforced by lease-based fencing. This replaces etcd's raft consensus with a cheaper mechanism — row-level `FOR SHARE` locks on lease tables.

4. **Spec/status ownership is decided at deployment time.** Most controllers own both spec and status for their resources — a single controller acquires both leases and calls `Write()` and `WriteStatus()`. Some controllers split ownership: an API server writes spec while a separate controller writes status, each holding its own lease on the same bucket independently. Both patterns are first-class; the lease rows (`bucket_leases` with `domain IN ('spec','status')`) are fully independent.

5. **Regional, single-primary database.** AWS RDS PostgreSQL 16+ Multi-AZ with synchronous standby. No multi-region, no multi-writer. Synchronous replication guarantees that failover never loses an acknowledged commit.

## Architecture

The system uses PostgreSQL 16 as the authoritative store for Kubernetes resources, with:

- **Server-side stored procedure (`pgctl_write()`)** that performs fence check, no-op suppression, counter increment, and upsert in a single server-side call, with `pg_notify` doorbell fired after commit to avoid the global notification-queue lock
- **Per-(GVK, bucket) gapless sequence counters** for commit-ordered event streams
- **Independent spec/status fencing** — single `bucket_leases` table with `domain` column (`'spec'`/`'status'`), each row fenced independently with `FOR SHARE` lock for single-writer semantics per sub-resource
- **No-op write suppression** — content-equal writes consume no sequence number, emit no doorbell, bump no `object_version`. Default on; `ForceWrite` opt-out. Matches Kubernetes API-server semantics where an update that changes nothing does not advance resourceVersion
- **Single-goroutine poll-primary watch** with LISTEN/NOTIFY doorbell as a latency-only optimization; all polling in one goroutine with snapshot-isolated (REPEATABLE READ) poll cycles; automatic LISTEN reconnection via `ListenConnFactory` with exponential backoff
- **Tombstone compaction** via single CTE (atomic delete + horizon advancement)
- **Timeline epochs** for failover detection
- **Prometheus instrumentation** across writer, watcher, verifier, and lease paths (see [METRICS.md](METRICS.md))

All mechanisms are justified by 8 named correctness invariants (I1–I8) defined in DESIGN.md §2.

## Correctness Guarantees

Every invariant has a corresponding race/failure scenario and a deterministic test that forces the interleaving:

| Race | Invariant | What it proves |
|------|-----------|----------------|
| R1 | I4 | `FOR SHARE` blocks lease epoch bump while writer is in-flight |
| R2 | I5 | Single-goroutine scheduler with leading/trailing debounce prevents event swallowing |
| R3 | I5 | Poll-primary delivers all events even with total doorbell loss |
| R4 | I1 | Concurrent first-write `ON CONFLICT` upsert yields {1, 2} |
| R5 | I1/I5 | Ambiguous commit resolved by read-back protocol |
| R6 | I4/I5 | Gapless stream across lease handover |
| R7 | I7 | Watcher gets 410 Gone when hwm < compaction horizon |
| R9 | I6 | Stale timeline epoch rejected with 410 Gone |
| R10 | I1 | 409 conflict rolls back counter increment |
| R11 | I4 | `FOR SHARE` on `bucket_leases` status row blocks status lease epoch bump while status writer is in-flight |
| R12 | I1/I2 | Concurrent spec (holder-A) and status (holder-B) writes share gapless counter, watcher sees correct stream, cross-domain fencing is enforced |
| R13 | I5 | Single-goroutine scheduler: no concurrent polls under rapid doorbell + baseline overlap |
| R14 | I6/I7 | Epoch mismatch on doorbell-triggered poll terminates watcher with 410 Gone (not swallowed) |
| R15 | I7 | Mid-poll compaction: snapshot isolation makes compaction invisible within a poll cycle |
| RB4a | I1/I5 | Identical write suppressed: no seq consumed, no version bump, counter unchanged |
| RB4b | I1/I2 | Real change after no-op correctly sequenced (gets next seq) |
| RB4c | I5 | Watcher sees no event for suppressed write |
| RB4d | I1 | Replayed create with identical content suppressed |
| RB4e | I1/I5 | WriteStatus suppression |
| RB4f | — | ForceWrite bypasses suppression |
| RB4g | I4 | Suppression holds FOR SHARE — grant blocks until suppressed txn commits |
| R16 | I5 | Debounce suppression: 30 rapid doorbells coalesce to ≤2 polls, trailing edge fires within floor window, no event loss |
| R17 | I5/I7 | Multi-bucket: interleaved delivery with per-bucket ascending seqs, per-channel doorbell, partial 410 on single-bucket compaction |
| R18 | I5 | Watcher resume: cancel + restart from HWM delivers exactly the missed events, union is complete, no duplicates |

Additionally, R3 and R5 have Toxiproxy-enhanced variants that inject network-level faults (TCP RST) via a proxy container, including a reconnect test that verifies `ListenConnFactory` restores the doorbell fast path after a connection kill.

## Continuous Invariant Verifier (§6)

The `internal/verifier` package implements the production verifier from DESIGN.md §6. It subscribes via the ordinary poll path and continuously checks:

- **I3/I6**: monotonic high-water marks (seq > prevHWM per bucket)
- **I7**: all gaps explained by compaction horizon

Stream-side gap checking (I1) is deliberately omitted: under coalescing (two writes to the same key between polls), the delivered sequence numbers are not contiguous — only the latest seq per object survives. This is correct Kubernetes watch semantics (I5 permits coalescing). Gap auditing, if needed, must cross-check the table (out of scope).

Duplicate detection uses monotonicity: `seq <= prevHWM` is reported as an I3 violation. No per-key map is maintained — verifier state is O(buckets), bounded.

An optional canary writer measures write-to-delivery latency — the wall-clock time from `Write` returning to the event appearing on the watcher channel — with p99 tracking via a bounded ring buffer (1,000 samples) and the `pgctl_verifier_canary_delivery_seconds` Prometheus histogram. The same code serves as the acceptance oracle for load tests and (later) production monitoring.

## Spec/Status Split

Both write paths (`Write` for spec, `WriteStatus` for status) share the same `gvk_bucket_counters` sequence and `object_version`, so watchers see a single ordered event stream covering both spec and status changes. Both paths support no-op write suppression — `Write` compares all four content fields (spec, status, metadata, deletion_timestamp), `WriteStatus` compares only the status field. `WriteResult.Changed` indicates whether the write produced a new state; callers can use this to skip downstream side-effects on no-ops.

Lease management matches the ownership pattern (see Assumption 4):

- **Single owner:** `BothManager` provides atomic `AcquireBoth`/`RenewBoth`/`ReleaseBoth` — single multi-row statements against the `bucket_leases` table (no explicit transaction needed).
- **Split ownership:** `NewSpecManager` and `NewStatusManager` operate independently. A spec writer and a status writer hold leases on the same bucket without interfering — the fence locks are on different rows (`domain='spec'` vs `domain='status'`).

## Performance

### Write throughput — RDS ceiling hunt

Results on AWS RDS Multi-AZ (synchronous commit), using the stored procedure write path (`pgctl_write()`) with doorbell outside transaction. One writer per bucket, 120s duration, 15s warm-up.

**db.m6g.2xlarge (8 vCPU):**

| Buckets | RPS | p50 | p99 |
|---------|-----|-----|-----|
| 1 | 317 | 3.1ms | 3.7ms |
| 16 | 3,803 | 4.1ms | 5.6ms |
| 64 | **9,622** | 6.1ms | 13.2ms |

**db.m6g.8xlarge (32 vCPU):** 11,728 w/s @ 64 buckets — only +22% over 2xlarge; the bottleneck is WAL sync (Multi-AZ synchronous replication round-trip), not CPU.

All runs: zero serialization failures, zero verifier violations. Near-linear scaling with bucket count (~150 w/s per bucket). Fleet capacity at burst (0.0748 w/s per cluster): ~128k clusters on 2xlarge, ~157k on 8xlarge.

Against DESIGN.md §4 sizing tiers:

| Tier | Steady RPS | Burst RPS | Buckets needed (RDS 2xlarge) |
|------|-----------|-----------|------------------------------|
| 5,000 clusters | 187 | 374 | 1 |
| 50,000 clusters | 1,870 | 3,740 | ~4 |

Bucket count caps the maximum controller replicas. The recommended default is **64 buckets**, expandable via epoch-bump migration (same mechanism as failover — all watchers 410 + relist).

The bottleneck is WAL sync (Multi-AZ synchronous replication round-trip), not CPU — a 4x larger instance (db.m6g.8xlarge) only adds +22% at 64 buckets. The 2xlarge is the right production choice.

For the full perfscale suite (Phase 0–7 certification, scaling analysis, infrastructure setup), see [`loadtest/README.md`](loadtest/README.md).

### Poll cost & delivery latency

Local podman measurements (4 buckets, 2,000 seeded resources, 10 watchers):

| Metric | p50 | p99 |
|--------|-----|-----|
| Idle poll cycle | 4.1 ms | 9.5 ms |
| Doorbell write-to-delivery | 25 ms | 62 ms |
| Baseline-only (notify-loss drill) | 527 ms | 996 ms |

All 1,000 events delivered under notify-loss (no doorbell), verifier silent. The baseline-only latency is bounded by the 1s polling interval used in the drill — in production the default 5s baseline is the worst case, but doorbells keep typical delivery under 100ms.

## Read Model: Direct Reads vs. Cached Reads

`Client.Get()` reads directly from PostgreSQL on every call — no in-memory cache. This differs from standard controller-runtime, where `Get()` inside a `Reconcile` reads from an informer cache populated by the List/Watch stream.

**Why direct reads are the default:** A fleet controller has low read rates (reconcilers read a handful of objects per cycle, not thousands). Direct reads are simpler, always return committed state, and avoid a class of staleness bugs. The `ListerWatcher` already feeds the watch stream for event-driven reconciliation; `Get()` is a point-read for the current object, not a scan.

**When to consider a cached model:** If reconcilers perform many `Get()` calls per cycle, or if multiple informers share the same `ListerWatcher`, wiring it into controller-runtime's standard cache reduces DB load and read latency. The `ListerWatcher` already implements the List/Watch contract, so the integration is mechanical — the hard part (gapless, ordered, exactly-once event stream) is already done.

**Trade-offs:**

| | Direct reads (current) | Cached reads |
|---|---|---|
| Read latency | ~1–5ms (Postgres round-trip) | ~0ms (memory) |
| Freshness | Always committed state | Up to one poll interval stale (5s worst case, ~100ms typical) |
| DB load | One query per `Get()` | Zero read queries from reconcilers |
| Memory | None beyond the connection | Full working set in memory per controller |
| Complexity | Simpler — no cache coherence concerns | Requires trusting the watch stream entirely |

For conflict resolution and ambiguous-commit read-back, the direct `Get()` (or `ReadBack`) is always needed regardless of the read model — those paths require the live database value.

## Examples

The [`examples/`](examples/) directory contains the same controller implemented twice — once against etcd, once against PostgreSQL — showing exactly what changes when migrating from one to the other. See [`examples/README.md`](examples/README.md) for the full migration guide, a line-count breakdown, and a step-by-step checklist.

## Documentation

- [DESIGN.md](DESIGN.md) — full design, invariant catalog (I1–I8), race catalog (R1–R18), and certification plan
- [WALKTHROUGH.md](WALKTHROUGH.md) — narrative explanation of why each mechanism exists and how the pieces fit together
- [METRICS.md](METRICS.md) — Prometheus metrics reference (all `pgctl_*` metrics, labels, integration guide)
- [loadtest/README.md](loadtest/README.md) — RDS perfscale suite: ceiling hunt, Phase 0–7 certification, Terraform infrastructure, and scaling analysis

## Running Tests

Requires: Go 1.22+, podman with a running machine (`podman machine start`).

```bash
make test-unit          # Pure unit tests (resourceversion)
make test-integration   # All DB-backed tests (schema, lease, writer, reader, compaction, verifier)
make test-race          # Race catalog R1–R18, RB4a–g under -race
make test-toxirace      # Toxiproxy-enhanced R3/R5 + doorbell reconnect
make test-load          # Phase 1 + Phase 5 load tests
make test               # Unit + integration + race
```

Stress mode for timing-sensitive race tests:

```bash
make test-race-stress   # 100x repetition under -race
```
