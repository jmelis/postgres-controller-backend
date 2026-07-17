# Load Testing the Storage Library

## What we're testing

This harness tests the **Go library packages** that implement Postgres-backed Kubernetes controller storage. It does not run controllers — it exercises the library directly against a real RDS PostgreSQL instance under load, proving correctness and finding performance boundaries that a local Postgres container cannot reveal.

### Packages under test

| Package               | What it does                                                                                                                                                                                                                                                                | How the harness uses it                                                                                                                                              |
| --------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `internal/writer`     | Atomic writes via `pgctl_write()` stored procedure: no-op suppression &rarr; UPSERT + `pg_current_xact_id()`, all in one server-side call in autocommit mode. Debounced doorbell via `doorbell.Debouncer`. Enforces commit ordering (I1) and optimistic concurrency (I6). | Every phase creates `writer.New(conn, nil).WithMetrics(m)` instances and calls `Write()` at varying rates and concurrency levels.                                    |
| `internal/reader`     | Poll-primary watcher with optional doorbell (LISTEN/NOTIFY). Delivers commit-ordered event streams per GVK using xid8 + snapshot-xmin watermark, regardless of doorbell health. Single-goroutine scheduler with debounce.                                                   | Phase 5 creates `reader.NewWatcher(...)` instances to measure idle poll cost, doorbell delivery latency, and baseline-only fallback.                                 |
| `internal/verifier`   | Continuous invariant checker — subscribes to the poll stream and verifies monotonic high-water marks (I2/I4), gap-vs-compaction-horizon checks (I5), and canary write-to-delivery latency.                                                                                  | Runs alongside every write phase. Any violation fails the test. Same code used in production.                                                                        |
| `internal/compaction` | Tombstone compaction: deletes fully-deleted tombstones (deletion_timestamp set, no active finalizers) and advances the compaction horizon atomically in a single CTE (I5). Dying objects with finalizers are preserved.                                                     | Background goroutine runs `compaction.Compact()` every 5 minutes during long tests to keep table size bounded.                                                       |
| `internal/schema`     | DDL migration — creates all tables and indexes.                                                                                                                                                                                                                             | Runs once at startup before seeding.                                                                                                                                 |
| `internal/metrics`    | Prometheus metrics for all of the above: write duration/count, poll duration, delivery latency, verifier violations.                                                                                                                                                        | All library instances are wired with `.WithMetrics(...)`. Metrics are scraped by CloudWatch Agent and pushed to a CloudWatch dashboard alongside native RDS metrics. |

### Invariants validated

The harness validates the correctness invariants from [DESIGN.md §2](../DESIGN.md):

| Invariant                              | What it guarantees                                                                                                             | How the harness tests it                                                                                                                                 |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **I1 — Commit order = sequence order** | If seq(A) < seq(B), then A committed before B became visible.                                                                  | The verifier confirms monotonic high-water marks; watchers in Phase 5 verify delivery order.                                                             |
| **I2 — No regression**                 | The watermark (`pg_snapshot_xmin()`) never decreases, even across failover.                                                    | The verifier's watermark check catches any regression. Phase 6 (manual) tests this across RDS failover.                                                  |
| **I3 — Exactly-once delivery**         | Watchers receive every state change exactly once (coalescing permitted), with no duplicates or losses, regardless of doorbell. | Phase 5's notify-loss drill disables the LISTEN connection; watchers must still deliver every event via poll-only fallback within the baseline interval. |
| **I5 — Compaction safety**             | A watcher can never silently skip a compacted event.                                                                           | The compaction goroutine runs during long phases; the verifier checks that gaps are only below the compaction horizon.                                   |
| **I6 — Optimistic concurrency**        | An update with a stale `object_version` is rejected (409).                                                                     | The writer library handles this internally; the harness measures resulting error rates.                                                                  |

### Why local Postgres isn't enough

| Concern                   | What RDS adds                                                                                                                                                                            |
| ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Write latency**         | Synchronous standby in Multi-AZ adds 1-3ms per commit — local has no sync standby, so latency numbers are meaninglessly optimistic.                                                      |
| **IOPS ceiling**          | gp3 storage has provisioned IOPS limits; local NVMe has effectively unlimited IOPS. The ceiling test (Phase 1) and steady-state test (Phase 2) are only meaningful against real storage. |
| **Autovacuum under load** | Sustained write load generates dead tuples; autovacuum must keep up or the counter table bloats and HOT updates degrade. Only visible over hours of sustained load.                      |
| **WAL volume**            | Large JSONB payloads (8-20KB) generate significant WAL. Only measurable with real gp3 throughput limits.                                                                                 |
| **Connection behavior**   | Real network between the harness and RDS exercises connection drops, latency jitter, and TCP keepalive — none of which exist with localhost.                                             |
| **Failover**              | RDS Multi-AZ failover (connection drop, promotion) can only be tested on a real cluster.                                                                                                 |

## Certification phases

Each phase targets a specific failure mode or performance boundary from [DESIGN.md §7](../DESIGN.md):

### Phase 1 — Counter ceiling

Saturates with N concurrent writers to find the maximum writes/s before latency degradation. This is the fundamental capacity number that all sizing math depends on.

**Library code exercised:** `writer.Write()` under maximum concurrency, `verifier.Run()`.

**Pass criteria:** meets target RPS, p99 below threshold, zero serialization failures, verifier silent.

### Phase 2 — Steady state

Sustains a target RPS for an extended period (hours to a week). Validates that autovacuum keeps up, IOPS stays within provisioned limits, and the write path remains stable. Periodic bursts test headroom.

**Library code exercised:** `writer.Write()` at sustained rate with burst periods, `verifier.Run()` continuously, `compaction.Compact()` periodically.

**Pass criteria:** sustained RPS within 5% of target, p50 below threshold, verifier silent.

### Phase 2b — Hot-GVK skew

Zipfian distribution: one GVK gets 80% of writes while the rest are cold. Proves cold GVKs don't starve.

**Library code exercised:** same as Phase 2 but with skewed GVK selection.

**Pass criteria:** cold-GVK p99 below configured threshold, verifier silent.

### Phase 3 — Avalanche / kill writers

Kills a fraction of writers mid-stream. New writers start and race on the same resources. The verifier confirms the event stream remains commit-ordered across the disruption.

**Library code exercised:** `writer.Write()` under crash/restart churn, `verifier.Run()` across the disruption boundary.

**Pass criteria:** verifier detects no gaps or violations.

### Phase 5 — Poll and delivery latency

Three sub-phases:

- **5A — Idle poll cost.** No writes occurring; measures the poll cycle duration of `reader.NewWatcher()` under idle conditions.
- **5B — Doorbell delivery latency.** Writes at a configured rate with LISTEN connections active; measures write-to-delivery latency through the doorbell path.
- **5C — Notify-loss drill.** Watchers created with `nil` listen connections (simulating LISTEN failure); must still deliver every event via poll-only fallback within the baseline interval.

**Library code exercised:** `reader.NewWatcher()` with and without listen connections, `writer.Write()` for stimulus, the doorbell (`pg_notify`) path end-to-end.

**Pass criteria:** doorbell delivery p99 below threshold, notify-loss drill delivers all events.

### Phase 6 — Failover drills (manual trigger)

Force an RDS failover under load. Measure RTO, verify zero event loss.

### Phase 7 — Backup/restore (manual trigger)

Restore from snapshot, restart controller pods, verify clients relist and converge to correct state.

## Data seeding

Before running phases, the harness populates the database with realistic data. The YAML spec controls:

- **GVK types** with configurable spec/status/metadata payload sizes (e.g., 8KB spec + 12KB status for HostedCluster objects)
- **Objects per GVK** — determines total row count and index depth

Large JSONB payloads matter because they exercise:

- No-op write suppression (JSONB `=` comparison on multi-KB payloads)
- Poll cycle cost (more bytes transferred per watcher poll)
- WAL volume (larger rows = more WAL per write)

## Architecture

```
┌──────────────────────────────────────┐
│          EC2 Instance (public)       │
│                                      │
│  ┌──────────────┬─────────────────┐  │
│  │  loadtest    │  CloudWatch     │  │
│  │  harness     │  Agent          │  │
│  │  /metrics ──>│  (scrapes &     │  │
│  │              │   pushes)       │  │
│  └──────┬───────┴────────┬────────┘  │
│         │                │           │
└─────────┼────────────────┼───────────┘
          │                │
          │ pgx            │ PutMetricData
          ▼                ▼
┌─────────────────┐   ┌──────────────┐
│  RDS PostgreSQL │   │  CloudWatch  │
│  16 Multi-AZ    │   │  (metrics +  │
│  (private, gp3) │   │   dashboard) │
└─────────────────┘   └──────────────┘
```

The harness is a standalone Go binary that imports the library packages directly — no RPC, no controllers, no Kubernetes API server. It connects to Postgres via `pgx`, runs the same code paths that controllers would, and exposes the same Prometheus metrics the library emits.

**Terraform** provisions a VPC, an EC2 instance (public subnet), RDS (database subnet), and a CloudWatch dashboard. The binary is cross-compiled locally and uploaded to EC2 via `scp`.

**CloudWatch Agent** runs on the EC2 instance, scraping the harness's `/metrics` endpoint and pushing `pgctl_*` + `loadtest_*` metrics to CloudWatch. RDS metrics (CPU, IOPS, connections) are already there natively. One dashboard covers everything.

**Compaction** runs in a background goroutine (every 5 minutes, 1-hour retention) to keep table size bounded during long tests.

**Checkpoints** are written periodically during long-running tests (days/weeks) with completed phase results and current phase progress. Fetch anytime with `./run.sh check`.

## YAML-driven test specs

A single YAML file controls everything: GVK payload sizes, which phases to run, target RPS, duration, pass/fail thresholds, checkpoint interval.

| Spec                        | Scenario                                                     | Duration |
| --------------------------- | ------------------------------------------------------------ | -------- |
| `specs/5k-baseline.yaml`    | 5,000-cluster tier — Phase 1+2+5                             | ~2h      |
| `specs/50k-stress.yaml`     | 50,000-cluster tier — all phases                             | ~50h     |
| `specs/ceiling-hunt.yaml`   | Max RPS discovery across worker counts and GVK sizes         | ~30min   |
| `specs/custom.yaml.example` | Fully commented template                             | varies   |

## Quick start

```bash
# Prerequisites: AWS CLI, Terraform >= 1.5, Go 1.22+, SSH key pair in AWS

# 1. Configure Terraform variables
cp loadtest/terraform/terraform.tfvars.example loadtest/terraform/terraform.tfvars
# Edit terraform.tfvars — set ec2_key_name to your AWS key pair name

# 2. Provision infrastructure and run the 5k baseline test
./run.sh specs/5k-baseline.yaml all

# 3. Check the CloudWatch dashboard (URL printed by setup)
./run.sh status

# 4. For long-running tests, check progress anytime
./run.sh check

# 5. Fetch final results
./run.sh results

# 6. Tear down when done
./run.sh teardown
```

## Commands

```bash
./run.sh [SPEC_FILE] [COMMAND]

setup       # Terraform apply + deploy harness binary to EC2
run         # Cross-compile, upload, and start the harness on EC2
check       # Fetch current checkpoint (mid-run progress)
status      # Show harness process status + CloudWatch dashboard URL
results     # Fetch final results JSON from EC2
ssh         # Open an interactive SSH session to the EC2 instance
teardown    # Destroy everything (Terraform destroy)
all         # setup + run (default)
```

## Development

Build and run locally against any Postgres:

```bash
go build -o lt ./loadtest/cmd/loadtest/
./lt --spec=loadtest/specs/5k-baseline.yaml --dsn="postgres://user:pass@localhost:5432/pgctl"
```

## Performance

### Write path

The writer uses a PL/pgSQL stored procedure (`pgctl_write`) that performs the suppression check and resource upsert (with `pg_current_xact_id()` stamp) in a single server-side call, run in autocommit mode. The debouncer handles `pg_notify` doorbells. The full write is 1 network round-trip.

COMMIT dominates write latency (~61% of total time) — this is the Multi-AZ synchronous replication wait. The WAL sync round-trip is the throughput bottleneck, not CPU.

### Throughput ceiling (sync commit)

Measured with `TestCeiling_MultiGVK`: 10 GVKs, 48 concurrent workers, 15s test duration + 2s warm-up, on db.m6g/r6g.8xlarge (32 vCPU). Both RDS (Multi-AZ sync replication) and Aurora (cross-AZ storage) tested with minimal (50B) and realistic (15-20KB) payloads.

| Engine | Payloads | Writes/s | p50   | p99   |
| ------ | -------- | -------- | ----- | ----- |
| RDS    | 50B      | 15,322   | 3.0ms | 4.3ms |
| Aurora | 50B      | 11,061   | 4.3ms | 7.4ms |
| Aurora | 15-20KB  | 3,932    | 11ms  | 26ms  |
| Aurora I/O Optimized | 15-20KB | 6,132 | 6.3ms | 29ms |
| RDS    | 15-20KB  | 1,728    | 8ms   | 1.4s  |

### Scaling characteristics

- **Payload size is the primary variable.** Realistic 15-20KB payloads (matching production GVK sizes) reduce throughput 3-4x compared to 50B payloads due to WAL volume per commit.
- **Aurora I/O Optimized is the fastest option for large payloads** (6,132 w/s) — 56% faster than standard Aurora (3,932 w/s) and nearly halves p50 latency (6.3ms vs 11ms). The `aurora-iopt1` storage type eliminates per-I/O charges and reduces WAL commit latency.
- **Aurora handles large payloads better than RDS Multi-AZ** (3,932–6,132 vs 1,728 w/s) — Aurora's distributed storage absorbs large WAL flushes more gracefully, while RDS Multi-AZ synchronous replication amplifies the per-commit cost. RDS p99 spikes to 1.4s with large payloads.
- **RDS is faster with small payloads** (15,322 vs 11,061 w/s) — Aurora's cross-AZ storage round-trip adds overhead that dominates when per-write WAL volume is minimal.

### Fleet capacity

Per [DESIGN.md §4](../DESIGN.md): 0.0374 w/s per cluster steady, 0.0748 w/s per cluster at burst (2x).

Using the realistic-payload ceiling (Aurora I/O Optimized, 6,132 w/s): **~82k clusters at burst** on a single db.r6g.8xlarge. The 5,000-cluster tier needs 374 burst w/s — over 16x headroom. The 50,000-cluster tier needs 3,740 burst w/s — 1.6x headroom.

### Remaining optimization lever: async commit

`synchronous_commit = off` would remove the WAL sync bottleneck entirely. The tradeoff is durability: a crash-and-restart-in-place can lose the WAL tail, rewinding committed state. Level-triggered controllers re-converge, but non-idempotent side effects keyed on acknowledged writes would be silently wrong. `remote_write` is a middle ground (standby durability without local fsync). See [DESIGN.md §3.7](../DESIGN.md) for the full analysis.

## Directory structure

```
loadtest/
├── README.md                   # This file
├── run.sh                      # Orchestration script (setup, run, check, teardown)
├── terraform/                  # EC2 + RDS + VPC + CloudWatch dashboard
│   ├── main.tf                 # Providers and backend
│   ├── variables.tf            # EC2 instance type, RDS class, key pair name
│   ├── vpc.tf                  # VPC with public + database subnets
│   ├── ec2.tf                  # Harness instance + security group + IAM
│   ├── rds.tf                  # PostgreSQL 16 Multi-AZ + Secrets Manager
│   ├── cloudwatch.tf           # Dashboard (RDS + pgctl metrics)
│   ├── outputs.tf              # Instance IP, SSH command, RDS endpoint
│   ├── cloudwatch-agent-config.json  # CW Agent prometheus scrape config
│   └── prometheus.yaml         # Scrape target: localhost:9090
├── specs/                      # YAML test specifications
└── cmd/loadtest/               # Go harness source
```
