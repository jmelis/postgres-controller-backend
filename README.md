# postgres-controller-backend

A PostgreSQL-backed Kubernetes control plane storage backend, replacing etcd for fleet management at 5,000–50,000 managed clusters. See [DESIGN.md](DESIGN.md) for the full design, invariant catalog, and certification plan.

## Architecture

The system uses PostgreSQL 16 as the authoritative store for Kubernetes resources, with:

- **Per-(GVK, bucket) gapless sequence counters** for commit-ordered event streams
- **Independent spec/status fencing** — `bucket_spec_leases` for spec writers, `bucket_status_leases` for status writers, each with `FOR SHARE` lock for single-writer semantics per sub-resource
- **Poll-primary watch** with LISTEN/NOTIFY doorbell as a latency-only optimization
- **Tombstone compaction** with transactional horizon advancement
- **Timeline epochs** for failover detection

All mechanisms are justified by 8 named correctness invariants (I1–I8) defined in DESIGN.md §2.

## Correctness Guarantees

Every invariant has a corresponding race/failure scenario and a deterministic test that forces the interleaving:

| Race | Invariant | What it proves |
|------|-----------|----------------|
| R1 | I4 | `FOR SHARE` blocks lease epoch bump while writer is in-flight |
| R2 | I5 | Dirty-flag clear-before-snapshot prevents event swallowing |
| R3 | I5 | Poll-primary delivers all events even with total doorbell loss |
| R4 | I1 | Concurrent first-write `ON CONFLICT` upsert yields {1, 2} |
| R5 | I1/I5 | Ambiguous commit resolved by read-back protocol |
| R6 | I4/I5 | Gapless stream across lease handover |
| R7 | I7 | Watcher gets 410 Gone when hwm < compaction horizon |
| R9 | I6 | Stale timeline epoch rejected with 410 Gone |
| R10 | I1 | 409 conflict rolls back counter increment |
| R11 | I4 | `FOR SHARE` on `bucket_status_leases` blocks status lease epoch bump while status writer is in-flight |
| R12 | I1/I2 | Concurrent spec (holder-A) and status (holder-B) writes share gapless counter, watcher sees correct stream, cross-domain fencing is enforced |

Additionally, R3 and R5 have Toxiproxy-enhanced variants that inject network-level faults (TCP RST) via a proxy container.

## Continuous Invariant Verifier (§6)

The `internal/verifier` package implements the production verifier from DESIGN.md §6. It subscribes via the ordinary poll path and continuously checks:

- **I1**: seq contiguity (no unexplained gaps)
- **I3/I6**: monotonic high-water marks
- **I5**: no duplicate deliveries
- **I7**: all gaps explained by compaction horizon

An optional canary writer measures write-to-delivery latency with p99 tracking. The same code serves as the acceptance oracle for load tests and (later) production monitoring.

## Spec/Status Split

Kubernetes controllers write spec (desired state) and status (observed state) independently. This backend supports that with separate fencing domains:

- **`bucket_spec_leases`** — fences `Write()` (spec + metadata + deletion)
- **`bucket_status_leases`** — fences `WriteStatus()` (status only)

Both write paths share the same `gvk_bucket_counters` sequence and `object_version`, so watchers see a single ordered event stream covering both spec and status changes. A spec writer (e.g., API server) and a status writer (e.g., controller) can hold leases on the same bucket independently — neither blocks the other's fencing.

Most controllers own both sub-resources. `BothManager` provides transactional `AcquireBoth`/`RenewBoth`/`ReleaseBoth` — a single BEGIN/COMMIT acquires or renews both lease rows atomically. For the minority of controllers that split ownership, `NewSpecManager` and `NewStatusManager` operate independently.

## Performance

Phase 1 load test results on a local podman Postgres 16 container (macOS ARM64, 8 CPU, 3.8 GB RAM):

**Per-bucket ceiling** (50 workers, 1 bucket): **1,035 writes/s**, p50=32ms, p99=225ms

**16-bucket scaling** (48 workers total): **2,482 writes/s**, p50=19ms, p99=45ms

All runs: zero serialization failures, zero fencing false-positives, zero invariant violations.

Against DESIGN.md §4 sizing tiers:

| Tier | Steady RPS | Burst RPS | Buckets needed (local) |
|------|-----------|-----------|------------------------|
| 5,000 clusters | 187 | 374 | 1 |
| 50,000 clusters | 1,870 | 3,740 | 4–8 |

Bucket count caps the maximum controller replicas. The recommended default is **16 buckets**, expandable via epoch-bump migration (same mechanism as failover — all watchers 410 + relist).

## Project Structure

```
internal/
  schema/       Schema migrations (6 tables: bucket_spec_leases, bucket_status_leases, ...)
  model/        Core types (Resource, WriteRequest, WriteResult)
  lease/        Lease acquire/renew/release/grant + BothManager for transactional spec+status
  writer/       Atomic write path (Write + WriteStatus) with TxHooks for test injection
  reader/       List + poll-primary Watcher
  compaction/   Tombstone compaction with horizon advancement
  resourceversion/  Composite RV parse/serialize
  verifier/     Continuous invariant verifier (§6)

test/
  testinfra/    Postgres + Toxiproxy container helpers (podman)
  race/         Race catalog tests R1–R12
  toxirace/     Toxiproxy-enhanced R3/R5
  loadtest/     Phase 1 counter ceiling + bucket scaling
```

## Running Tests

Requires: Go 1.22+, podman with a running machine (`podman machine start`).

```bash
make test-unit          # Pure unit tests (resourceversion)
make test-integration   # All DB-backed tests (schema, lease, writer, reader, compaction, verifier)
make test-race          # Race catalog R1–R12 under -race
make test-toxirace      # Toxiproxy-enhanced R3/R5 (starts extra containers)
make test-load          # Phase 1 load test (10s sustained, 50 workers)
make test               # Unit + integration + race
```

Stress mode for timing-sensitive race tests:

```bash
make test-race-stress   # 100x repetition under -race
```
