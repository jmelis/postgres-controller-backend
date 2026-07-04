# postgres-controller-backend

A PostgreSQL-backed storage backend for Kubernetes controllers that manage fleet resources (5,000–50,000 managed clusters). Not a general-purpose etcd replacement — see Assumptions below.

See [DESIGN.md](DESIGN.md) for the full design, invariant catalog, and certification plan.

## Assumptions

This design exploits constraints that a fleet controller satisfies but a general-purpose API server does not. If your use case doesn't match these, use [kine](https://github.com/k3s-io/kine).

1. **The controller owns the write path.** Writes come from controller-runtime reconcilers, not from arbitrary API clients. The controller knows which resources it manages and can accept bucket-level lease assignment.

2. **Fixed bucket count.** The number of buckets is set at deployment time. Each (GVK, bucket) pair gets its own sequence counter created on first use, so there is no single global sequence bottleneck. Changing the bucket count is an epoch-bump migration (all watchers 410 + relist).

3. **Single writer per bucket per sub-resource.** Each bucket has at most one spec writer and one status writer at any time, enforced by lease-based fencing. This replaces etcd's raft consensus with a cheaper mechanism — row-level `FOR SHARE` locks on lease tables.

4. **Spec/status ownership is decided at deployment time.** Most controllers own both spec and status for their resources — a single controller acquires both leases and calls `Write()` and `WriteStatus()`. Some controllers split ownership: an API server writes spec while a separate controller writes status, each holding its own lease on the same bucket independently. Both patterns are first-class; the lease rows (`bucket_leases` with `domain IN ('spec','status')`) are fully independent.

5. **Regional, single-primary database.** AWS RDS PostgreSQL 16+ Multi-AZ with synchronous standby. No multi-region, no multi-writer. Synchronous replication guarantees that failover never loses an acknowledged commit.

## Architecture

The system uses PostgreSQL 16 as the authoritative store for Kubernetes resources, with:

- **Per-(GVK, bucket) gapless sequence counters** for commit-ordered event streams
- **Independent spec/status fencing** — single `bucket_leases` table with `domain` column (`'spec'`/`'status'`), each row fenced independently with `FOR SHARE` lock for single-writer semantics per sub-resource
- **Single-goroutine poll-primary watch** with LISTEN/NOTIFY doorbell as a latency-only optimization; all polling in one goroutine with snapshot-isolated (REPEATABLE READ) poll cycles
- **Tombstone compaction** via single CTE (atomic delete + horizon advancement)
- **Timeline epochs** for failover detection

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

Additionally, R3 and R5 have Toxiproxy-enhanced variants that inject network-level faults (TCP RST) via a proxy container.

## Continuous Invariant Verifier (§6)

The `internal/verifier` package implements the production verifier from DESIGN.md §6. It subscribes via the ordinary poll path and continuously checks:

- **I3/I6**: monotonic high-water marks (seq > prevHWM per bucket)
- **I7**: all gaps explained by compaction horizon

Stream-side gap checking (I1) is deliberately omitted: under coalescing (two writes to the same key between polls), the delivered sequence numbers are not contiguous — only the latest seq per object survives. This is correct Kubernetes watch semantics (I5 permits coalescing). Gap auditing, if needed, must cross-check the table (out of scope).

Duplicate detection uses monotonicity: `seq <= prevHWM` is reported as an I3 violation. No per-key map is maintained — verifier state is O(buckets), bounded.

An optional canary writer measures write-to-delivery latency with p99 tracking via a bounded ring buffer (1,000 samples). The same code serves as the acceptance oracle for load tests and (later) production monitoring.

## Spec/Status Split

Both write paths (`Write` for spec, `WriteStatus` for status) share the same `gvk_bucket_counters` sequence and `object_version`, so watchers see a single ordered event stream covering both spec and status changes.

Lease management matches the ownership pattern (see Assumption 4):

- **Single owner:** `BothManager` provides atomic `AcquireBoth`/`RenewBoth`/`ReleaseBoth` — single multi-row statements against the `bucket_leases` table (no explicit transaction needed).
- **Split ownership:** `NewSpecManager` and `NewStatusManager` operate independently. A spec writer and a status writer hold leases on the same bucket without interfering — the fence locks are on different rows (`domain='spec'` vs `domain='status'`).

## Performance

Phase 1 load test results on a local podman Postgres 16 container (macOS ARM64, 8 CPU, 3.8 GB RAM):

**Per-bucket ceiling** (50 workers, 1 bucket): **1,045 writes/s**, p50=33ms, p99=231ms

**16-bucket scaling** (48 workers total): **2,548 writes/s**, p50=18ms, p99=45ms

All runs: zero serialization failures, zero fencing false-positives, zero invariant violations.

Against DESIGN.md §4 sizing tiers:

| Tier | Steady RPS | Burst RPS | Buckets needed (local) |
|------|-----------|-----------|------------------------|
| 5,000 clusters | 187 | 374 | 1 |
| 50,000 clusters | 1,870 | 3,740 | 4–8 |

Bucket count caps the maximum controller replicas. The recommended default is **16 buckets**, expandable via epoch-bump migration (same mechanism as failover — all watchers 410 + relist).

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

## Project Structure

```
internal/
  schema/       Schema migrations (5 tables: bucket_leases, ...)
  model/        Core types (Resource, WriteRequest, StatusWriteRequest, WriteResult)
  lease/        Lease acquire/renew/release/grant + BothManager for atomic spec+status
  writer/       Atomic write path (Write + WriteStatus) with TxHooks for test injection
  reader/       List + single-goroutine poll-primary Watcher with snapshot polls
  compaction/   Tombstone compaction (single CTE: delete + horizon atomically)
  resourceversion/  Composite RV parse/serialize
  verifier/     Continuous invariant verifier (§6) — O(buckets) state, bounded ring buffer

pkg/
  crbridge/     controller-runtime-shaped adapter (Client, ListerWatcher, WatchInterface)

test/
  testinfra/    Postgres + Toxiproxy container helpers (podman)
  race/         Race catalog tests R1–R15
  toxirace/     Toxiproxy-enhanced R3/R5
  loadtest/     Phase 1 counter ceiling + bucket scaling
```

## Running Tests

Requires: Go 1.22+, podman with a running machine (`podman machine start`).

```bash
make test-unit          # Pure unit tests (resourceversion)
make test-integration   # All DB-backed tests (schema, lease, writer, reader, compaction, verifier)
make test-race          # Race catalog R1–R15 under -race
make test-toxirace      # Toxiproxy-enhanced R3/R5 (starts extra containers)
make test-load          # Phase 1 load test (10s sustained, 50 workers)
make test               # Unit + integration + race
```

Stress mode for timing-sensitive race tests:

```bash
make test-race-stress   # 100x repetition under -race
```
