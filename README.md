# postgres-controller-backend

Runs the controller-runtime controllers against plain PostgreSQL instead of kube-apiserver + etcd — same reconcile loops, one commodity managed database.

This works because the library re-implements the Kubernetes List/Watch contract — commit-ordered event streams with `resourceVersion` semantics — on ordinary Postgres tables. Informers, reconcile loops, and optimistic concurrency behave as they always have; underneath, writers (controllers, or an API server fronting the database) and watchers talk to Postgres directly, with no etcd protocol, no kube-apiserver, and no consensus layer in the path.

The motivation is operational. At fleet scale, etcd becomes the component you engineer around, and colocating application state in the cluster's own etcd ties your data's disaster-recovery story to the cluster's. One managed Postgres instance replaces it with a database your team already knows how to run — independent backup/restore, standard failover — and gives up nothing on throughput: up to ~15,000 writes/s with small payloads, ~4,000 with realistic 15-20KB payloads (db.m6g.8xlarge), with correctness enforced by PostgreSQL's native transaction IDs.

**This is not a general-purpose etcd replacement.** It targets deployments where you own every writer and all writes go through this library. Check [Is this for you?](#is-this-for-you) to see if your use case matches the assumptions. If not, use [kine](https://github.com/k3s-io/kine).

## Is this for you?

Only use if you can satisfy both assumptions:

1. **You own every writer, and every writer uses this library.** Writers can be controller-runtime reconcilers, an API server fronting the database, or any other component — as long as all writes go through this library. Nothing server-side validates a writer's behavior, so the guarantees hold only because every writer is yours.

2. **Single-primary PostgreSQL 16+.** Synchronous replication to a standby is required for the claim that failover never loses an acknowledged commit. AWS RDS Multi-AZ is the reference deployment (and where the performance numbers come from), not a requirement.

Otherwise, use `etcd` or `kine`, for example.

## The mental model

Three ideas carry the whole design:

- **Transaction-ID ordering.** Each write is stamped with `pg_current_xact_id()` — PostgreSQL's native 64-bit transaction ID. Watchers use `pg_snapshot_xmin()` as a safe watermark: everything below `xmin` is committed, forming a complete prefix. No shared counter, no exclusive row locks, no write serialization.
- **Poll-primary watch.** Watchers _pull_ events from the table; the LISTEN/NOTIFY doorbell is a latency-only optimization. Total notification loss costs latency (bounded by the baseline poll, 5s default), never events.
- **Failover safety.** RDS Multi-AZ synchronous replication guarantees no committed transaction is ever lost across failover. On database restore from backup, restarting controller pods forces a clean relist.

[WALKTHROUGH.md](WALKTHROUGH.md) develops each of these in narrative form; [DESIGN.md](DESIGN.md) is the full specification.

## Getting started

The [`examples/`](examples/) directory contains the same controller implemented twice — once with controller-runtime against etcd, once with [`pkg/pgruntime`](pkg/pgruntime/) against PostgreSQL — showing exactly what changes when migrating. The postgres wiring looks like:

```go
mgr, _ := pgruntime.NewManager(pgruntime.Options{
    Scheme:   scheme,
    DSN:      dsn,
    Logger:   log,
})

(&GreetingReconciler{Client: mgr.GetClient()}).SetupWithManager(mgr)
mgr.Start(ctx)
```

`pgruntime.NewManager` handles connection pooling and schema migration internally — the caller provides a DSN and a scheme, and gets back a standard `manager.Manager`. See [`examples/README.md`](examples/README.md) for the full migration guide, a line-count breakdown, and a step-by-step checklist.

## Architecture

PostgreSQL 16 is the authoritative store, with:

- **Server-side stored procedure (`pgctl_write()`)** — no-op suppression and upsert in a single server-side call, run in autocommit mode (no explicit BEGIN/COMMIT). Each row stamped with `pg_current_xact_id()` — no shared counter, no write contention
- **xid8 + snapshot-xmin watermark** for commit-ordered event streams — watchers use `pg_snapshot_xmin()` as a safe high-water mark
- **No-op write suppression** — content-equal writes consume no sequence number, emit no doorbell, and bump no `object_version`, matching Kubernetes API-server semantics where an update that changes nothing does not advance resourceVersion. Default on; `ForceWrite` opts out; `WriteResult.Changed` lets callers skip downstream side-effects
- **Single-goroutine poll-primary watch** with LISTEN/NOTIFY doorbell as a latency-only optimization; all polling in one goroutine with snapshot-isolated (`REPEATABLE READ`) poll cycles; automatic LISTEN reconnection via `ListenConnFactory` with exponential backoff
- **Tombstone compaction** via a single CTE (atomic delete + horizon advancement) with finalizer guard — only fully-deleted objects (no active finalizers) are compacted; dying objects with finalizers survive past retention
- **Failover safety** via RDS Multi-AZ synchronous replication (no committed-write loss); on restore, pod restart forces relist
- **Prometheus instrumentation** across writer, watcher, and verifier paths ([METRICS.md](METRICS.md))

```mermaid
flowchart LR
    subgraph Controllers
        C1[Controller A]
        C2[Controller B]
        C3[Controller C]
    end

    subgraph "writer.Write()"
        W["pgctl_write()
        ─────────────
        1. Suppress if no-op
        2. UPSERT + xid8 stamp"]
    end

    subgraph PostgreSQL
        direction TB
        PG["kubernetes_resources\n txid_stamp ordering"]
    end

    subgraph Watchers
        WA["Poll loop\n txid > hwm"]
    end

    subgraph Debouncer
        DB["Ring(gvk)\n → pg_notify\n every 50ms"]
    end

    C1 --> W
    C2 --> W
    C3 --> W
    W --> PG
    W -.-> DB
    DB -.->|pg_notify| WA
    WA -->|"SELECT\n txid > hwm"| PostgreSQL
```

## Correctness

Every mechanism is justified by one of 6 named invariants (I1–I6, DESIGN.md §2) — commit-ordered sequences, no regression across failover, exactly-once watch delivery, resourceVersion monotonicity, compaction safety, optimistic concurrency.

Every invariant has a corresponding race or failure scenario and a **deterministic test that forces the interleaving** — 21 tests in total (R2–R5, R7, R10, R12–R13, R15–R21, RB4a–f; full catalog in DESIGN.md §5):

| Theme                  | Tests                      | What they prove                                                                                                                                           |
| ---------------------- | -------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Sequence integrity     | R4, R5, R10, R12           | Concurrent first writes, ambiguous commits, 409 rollbacks, and concurrent spec/status writes never create duplicates or ordering violations               |
| Watch delivery         | R2, R3, R13, R16, R17, R18 | No event is swallowed by debouncing, doorbell loss, rapid-doorbell coalescing, or cancel/resume from the high-water mark                                  |
| Compaction             | R7, R15                    | Watchers behind a compaction horizon get `410 Gone` (never a silent skip); mid-poll compaction is invisible under snapshot isolation                      |
| Optimistic concurrency | R19, R20, R21              | Concurrent writers to the same object, concurrent spec/status writers, and tombstone revival all respect `object_version` (409 on stale, no lost updates) |
| No-op suppression      | RB4a–f                     | Suppressed writes consume no sequence and emit no event; real changes after a no-op sequence correctly                                                    |

R3 and R5 additionally have Toxiproxy variants that inject network-level faults (TCP RST), including a test that verifies the doorbell fast path recovers after a connection kill.

Beyond tests, the [`internal/verifier`](internal/verifier/) package runs the same checks continuously in production (DESIGN.md §6): it subscribes via the ordinary poll path and verifies monotonic watermarks (I2/I4) and that all gaps are explained by the compaction horizon (I5). An optional canary writer measures write-to-delivery latency (p99 via bounded ring buffer, exported as `pgctl_verifier_canary_delivery_seconds`). The same code is the acceptance oracle for load tests.

## Spec/Status Split

All three write paths (`Write` for full writes, `WriteObject` for spec + metadata only, `WriteStatus` for status only) share the same `object_version` and `txid_stamp`, so watchers see a single event stream covering both spec and status changes. All paths support no-op write suppression — `Write` compares all four content fields (spec, status, metadata, deletion_timestamp), `WriteObject` compares spec, metadata, and deletion_timestamp (not status), `WriteStatus` compares only the status field. `WriteObject` passes null status to the stored procedure, which uses `COALESCE(p_status, status)` to preserve the existing status column — this matches the Kubernetes API server's `Update` behavior where spec and status are independent subresources. `WriteResult.Changed` indicates whether the write produced a new state; callers can use this to skip downstream side-effects on no-ops.

## Performance

db.m6g/r6g.8xlarge (32 vCPU), 48 concurrent workers, synchronous commit, 10 GVKs:

| Engine | Payloads | Writes/s | p50   | p99   |
| ------ | -------- | -------- | ----- | ----- |
| RDS    | 50B      | 15,322   | 3.0ms | 4.3ms |
| Aurora | 50B      | 11,061   | 4.3ms | 7.4ms |
| Aurora | 15-20KB  | 3,932    | 11ms  | 26ms  |
| RDS    | 15-20KB  | 1,728    | 8ms   | 1.4s  |

Payload size is the primary throughput variable: realistic 15-20KB payloads (matching production GVK sizes) reduce throughput 3-4x compared to minimal payloads due to WAL volume. Aurora handles large payloads substantially better than RDS Multi-AZ (3,932 vs 1,728 w/s), though RDS is faster with small payloads.

All correctness invariants (I1–I6) verified under load: zero serialization failures, zero verifier violations across all runs.

Full perfscale suite: [`loadtest/README.md`](loadtest/README.md).

### Poll cost & delivery latency (Phase 5)

2,000 seeded resources, 10 watchers:

| Metric                     | p50    | p99    |
| -------------------------- | ------ | ------ |
| Idle poll cycle            | 4.1 ms | 9.5 ms |
| Doorbell write-to-delivery | 25 ms  | 62 ms  |

All 1,000 events delivered under notify-loss (no doorbell), verifier silent. The baseline-only latency is bounded by the 1s polling interval used in the drill — in production the default 5s baseline is the worst case, but doorbells keep typical delivery under 100ms.

## Examples

The [`examples/`](examples/) directory contains the same controller implemented twice — once against etcd, once against PostgreSQL — showing exactly what changes when migrating from one to the other. See [`examples/README.md`](examples/README.md) for the full migration guide, a line-count breakdown, and a step-by-step checklist.

## Documentation

- [DESIGN.md](DESIGN.md) — full design: invariant catalog (I1–I6), race catalog (R2–R5, R7, R10, R12–R13, R15–R21), sizing, certification plan
- [WALKTHROUGH.md](WALKTHROUGH.md) — narrative explanation of why each mechanism exists and how the pieces fit together
- [COMPATIBILITY.md](COMPATIBILITY.md) — controller-runtime compatibility matrix: what works, what doesn't, what silently differs
- [ARCHITECTURE_COMPARISON.md](ARCHITECTURE_COMPARISON.md) — this direct-to-Postgres design vs. an intermediated REST-API architecture (reliability, consistency, operational surface)
- [METRICS.md](METRICS.md) — Prometheus metrics reference (all `pgctl_*` metrics, labels, integration guide)
- [loadtest/README.md](loadtest/README.md) — RDS perfscale suite: ceiling hunt, Phase 0–7 certification, Terraform infrastructure, scaling analysis
- [examples/README.md](examples/README.md) — etcd → postgres migration guide with side-by-side controller implementations

## Running tests

Requires: Go 1.26+, podman with a running machine (`podman machine start`).

```bash
make test-unit          # Pure unit tests (resourceversion)
make test-integration   # All DB-backed tests (schema, writer, reader, compaction, verifier)
make test-race          # Race catalog R2–R5, R7, R10, R12–R13, R15–R21, RB4a–f under -race
make test-toxirace      # Toxiproxy-enhanced R3/R5 + doorbell reconnect
make test-load          # Phase 1 + Phase 5 load tests
make test               # Unit + integration + race
```

Stress mode for timing-sensitive race tests:

```bash
make test-race-stress   # 100x repetition under -race
```
