# Load Testing the Storage Library

## What we're testing

This harness tests the **Go library packages** that implement Postgres-backed Kubernetes controller storage. It does not run controllers — it exercises the library directly against a real RDS PostgreSQL instance under load, proving correctness and finding performance boundaries that a local Postgres container cannot reveal.

### Packages under test

| Package | What it does | How the harness uses it |
|---------|-------------|------------------------|
| `internal/writer` | Fenced atomic writes: lease check (FOR SHARE) &rarr; counter increment &rarr; UPSERT &rarr; pg_notify, all in one transaction. Enforces single-writer semantics (I4), gapless sequences (I1), commit ordering (I2), and optimistic concurrency (I8). Includes no-op write suppression. | Every phase creates `writer.New(conn, nil).WithMetrics(m)` instances and calls `Write()` at varying rates, concurrency, and bucket distributions. |
| `internal/reader` | Poll-primary watcher with optional doorbell (LISTEN/NOTIFY). Delivers gap-free event streams per (GVK, bucket) regardless of doorbell health. Single-goroutine scheduler with debounce. | Phase 5 creates `reader.NewWatcher(...)` instances to measure idle poll cost, doorbell delivery latency, and baseline-only fallback. |
| `internal/lease` | Per-bucket lease acquisition, renewal, and release. The epoch-based fencing mechanism that prevents stale writers from committing. | All phases acquire leases via `lease.NewSpecManager(conn, holder).WithMetrics(m)`. Phase 3 specifically tests lease handover and zombie fencing. |
| `internal/verifier` | Continuous invariant checker — subscribes to the poll stream and verifies monotonic high-water marks (I3/I6), gap-vs-compaction-horizon checks (I7), and canary write-to-delivery latency. | Runs alongside every write phase. Any violation fails the test. Same code used in production. |
| `internal/compaction` | Tombstone compaction: deletes old tombstones and advances the compaction horizon atomically in a single CTE (I7). | Background goroutine runs `compaction.Compact()` every 5 minutes during long tests to keep table size bounded. |
| `internal/schema` | DDL migration — creates all tables and indexes. | Runs once at startup before seeding. |
| `internal/metrics` | Prometheus metrics for all of the above: write duration/count, poll duration, delivery latency, lease operations, verifier violations. | All library instances are wired with `.WithMetrics(...)`. Metrics are scraped by CloudWatch Agent and pushed to a CloudWatch dashboard alongside native RDS metrics. |

### Invariants validated

The harness validates the correctness invariants from [DESIGN.md §2](../DESIGN.md):

| Invariant | What it guarantees | How the harness tests it |
|-----------|-------------------|--------------------------|
| **I1 — Gapless issuance** | Committed sequence numbers are exactly 1, 2, 3, ... with no holes per (GVK, bucket). | The verifier runs during every write phase and checks the stream. Concurrent writers in Phase 1 stress the counter's INSERT ON CONFLICT path. |
| **I2 — Commit order = sequence order** | If seq(A) < seq(B), then A committed before B became visible. | The verifier confirms monotonic high-water marks; watchers in Phase 5 verify delivery order. |
| **I3 — No regression** | `current_seq` never decreases, even across failover. | The verifier's HWM check catches any regression. Phase 6 (manual) tests this across RDS failover. |
| **I4 — Single writer** | A replica holding a stale lease epoch cannot commit. | Phase 3 starts zombie writers with stale epochs — every zombie write must fail with `ErrFenceViolation`. The verifier confirms the stream has no interleaved stale-epoch data. |
| **I5 — Exactly-once delivery** | Watchers receive every state change exactly once (coalescing permitted), with no duplicates or losses, regardless of doorbell. | Phase 5's notify-loss drill disables the LISTEN connection; watchers must still deliver every event via poll-only fallback within the baseline interval. |
| **I7 — Compaction safety** | A watcher can never silently skip a compacted event. | The compaction goroutine runs during long phases; the verifier checks that gaps are only below the compaction horizon. |
| **I8 — Optimistic concurrency** | An update with a stale `object_version` is rejected (409). | The writer library handles this internally; the harness measures resulting error rates. |

### Why local Postgres isn't enough

| Concern | What RDS adds |
|---------|--------------|
| **Write latency** | Synchronous standby in Multi-AZ adds 1-3ms per commit — local has no sync standby, so latency numbers are meaninglessly optimistic. |
| **IOPS ceiling** | gp3 storage has provisioned IOPS limits; local NVMe has effectively unlimited IOPS. The ceiling test (Phase 1) and steady-state test (Phase 2) are only meaningful against real storage. |
| **Autovacuum under load** | Sustained write load generates dead tuples; autovacuum must keep up or the counter table bloats and HOT updates degrade. Only visible over hours of sustained load. |
| **WAL volume** | Large JSONB payloads (8-20KB) generate significant WAL. Only measurable with real gp3 throughput limits. |
| **Connection behavior** | Real network between the harness and RDS exercises connection drops, latency jitter, and TCP keepalive — none of which exist with localhost. |
| **Failover** | RDS Multi-AZ failover (timeline epoch bump, connection drop, promotion) can only be tested on a real cluster. |

## Certification phases

Each phase targets a specific failure mode or performance boundary from [DESIGN.md §7](../DESIGN.md):

### Phase 1 — Counter ceiling

Saturates buckets with N concurrent writers to find the maximum writes/s before serialization failures or latency degradation. This is the fundamental capacity number that all sizing math depends on.

**Library code exercised:** `writer.Write()` under maximum concurrency, `lease.Acquire()` + `lease.Renew()`, `verifier.Run()`.

**Pass criteria:** meets target RPS, p99 below threshold, zero serialization failures, zero fence violations, verifier silent.

### Phase 2 — Steady state

Sustains a target RPS for an extended period (hours to a week). Validates that autovacuum keeps up, IOPS stays within provisioned limits, and the write path remains stable. Periodic bursts test headroom.

**Library code exercised:** `writer.Write()` at sustained rate with burst periods, `lease.Renew()` over the full duration, `verifier.Run()` continuously, `compaction.Compact()` periodically.

**Pass criteria:** sustained RPS within 5% of target, p50 below threshold, verifier silent.

### Phase 2b — Hot-bucket skew

Zipfian distribution: one bucket gets 80% of writes while the rest are cold. Proves cold buckets don't starve.

**Library code exercised:** same as Phase 2 but with skewed bucket selection. Exposes whether the counter table's `fillfactor=50` and HOT updates work under uneven load.

**Pass criteria:** cold-bucket p99 below configured threshold, verifier silent.

### Phase 3 — Avalanche / kill writers

Kills a fraction of writers mid-stream, then starts zombie writers holding stale lease epochs. The zombies must be fenced. New writers acquire leases and take over. The verifier confirms the event stream remains gapless across handover.

**Library code exercised:** `lease.Acquire()` (contested, after previous holder's TTL expires), `writer.Write()` with stale epoch (must return `ErrFenceViolation`), `verifier.Run()` across the handover boundary.

**Pass criteria:** all zombie writes fenced, verifier detects no gaps or violations across the handover.

### Phase 5 — Poll and delivery latency

Three sub-phases:

- **5A — Idle poll cost.** No writes occurring; measures the poll cycle duration of `reader.NewWatcher()` under idle conditions.
- **5B — Doorbell delivery latency.** Writes at a configured rate with LISTEN connections active; measures write-to-delivery latency through the doorbell path.
- **5C — Notify-loss drill.** Watchers created with `nil` listen connections (simulating LISTEN failure); must still deliver every event via poll-only fallback within the baseline interval.

**Library code exercised:** `reader.NewWatcher()` with and without listen connections, `writer.Write()` for stimulus, the doorbell (`pg_notify`) path end-to-end.

**Pass criteria:** doorbell delivery p99 below threshold, notify-loss drill delivers all events.

### Phase 6 — Failover drills (manual trigger)

Force an RDS failover under load. Measure RTO, verify zero event loss, confirm timeline epoch bump.

### Phase 7 — Backup/restore (manual trigger)

Restore from snapshot, verify epoch bump forces client relist.

## Data seeding

Before running phases, the harness populates the database with realistic data. The YAML spec controls:

- **GVK types** with configurable spec/status/metadata payload sizes (e.g., 8KB spec + 12KB status for HostedCluster objects)
- **Objects per bucket** — determines total row count and index depth
- **Bucket count** — determines sharding fan-out

Large JSONB payloads matter because they exercise:
- No-op write suppression (JSONB `=` comparison on multi-KB payloads)
- Poll cycle cost (more bytes transferred per watcher poll)
- WAL volume (larger rows = more WAL per write)

## Architecture

```
┌─────────────────────────────────────┐
│            EKS Cluster              │
│                                     │
│  ┌──────────────┬────────────────┐  │
│  │  loadtest    │  CloudWatch    │  │
│  │  harness     │  Agent sidecar │  │
│  │  /metrics ──>│  (scrapes &    │  │
│  │              │   pushes)      │  │
│  └──────┬───────┴───────┬────────┘  │
│         │               │           │
└─────────┼───────────────┼───────────┘
          │               │
          │ pgx           │ PutMetricData
          ▼               ▼
┌─────────────────┐   ┌──────────────┐
│  RDS PostgreSQL │   │  CloudWatch  │
│  16 Multi-AZ    │   │  (metrics +  │
│  (private, gp3) │   │   dashboard) │
└─────────────────┘   └──────────────┘
```

The harness is a standalone Go binary that imports the library packages directly — no RPC, no controllers, no Kubernetes API server. It connects to Postgres via `pgx`, runs the same code paths that controllers would, and exposes the same Prometheus metrics the library emits.

**CloudWatch Agent sidecar** scrapes the harness's `/metrics` endpoint and pushes `pgctl_*` + `loadtest_*` metrics to CloudWatch. RDS metrics (CPU, IOPS, connections) are already there natively. One dashboard covers everything.

**Compaction** runs in a background goroutine (every 5 minutes, 1-hour retention) to keep table size bounded during long tests.

**Checkpoints** are written periodically during long-running tests (days/weeks) with completed phase results and current phase progress. Fetch anytime with `./run.sh check`.

## YAML-driven test specs

A single YAML file controls everything: bucket count, GVK payload sizes, which phases to run, target RPS, duration, pass/fail thresholds, checkpoint interval.

| Spec | Scenario | Duration |
|------|----------|----------|
| `specs/5k-baseline.yaml` | 5,000-cluster tier — 16 buckets, Phase 1+2+5 | ~2h |
| `specs/50k-stress.yaml` | 50,000-cluster tier — 64 buckets, all phases | ~50h |
| `specs/ceiling-hunt.yaml` | Max RPS discovery across bucket counts and GVK sizes | ~30min |
| `specs/custom.yaml.example` | Fully commented template | varies |

## Quick start

```bash
# Prerequisites: AWS CLI, Terraform >= 1.5, kubectl, Podman, Go 1.22+

# 1. Provision infrastructure and run the 5k baseline test
./run.sh specs/5k-baseline.yaml all

# 2. Check the CloudWatch dashboard (URL printed by setup)
./run.sh status

# 3. For long-running tests, check progress anytime
./run.sh check

# 4. Fetch final results
./run.sh results

# 5. Tear down when done
./run.sh teardown
```

## Commands

```bash
./run.sh [SPEC_FILE] [COMMAND]

setup       # Terraform apply + K8s manifests
run         # Build image + start Job
check       # Fetch current checkpoint (mid-run progress)
status      # Show Job/pod status + CloudWatch dashboard URL
results     # Fetch final results JSON from completed Job
teardown    # Destroy everything
all         # setup + run (default)
```

## Development

Build and run locally against any Postgres:

```bash
go build -o lt ./loadtest/cmd/loadtest/
./lt --spec=loadtest/specs/5k-baseline.yaml --dsn="postgres://user:pass@localhost:5432/pgctl"
```

## Directory structure

```
loadtest/
├── README.md                   # This file
├── Containerfile               # Multi-stage build for the Go harness
├── run.sh                      # Wrapper script for the full lifecycle
├── terraform/                  # EKS + RDS + VPC + CloudWatch dashboard
├── k8s/                        # Job, ServiceAccount, CW Agent config
├── specs/                      # YAML test specifications
└── cmd/loadtest/               # Go harness source
```
