---
theme: default
title: "Simplifying ROSA HyperFleet"
info: |
  Evaluation of a new architecture for ROSA HyperFleet.
transition: slide-left
mdc: true
lineNumbers: false
layout: default
---

# Simplifying ROSA HyperFleet

<div class="flex-1 flex items-center justify-center gap-4">
  <img src="./image.png" width="400" />
  <span class="text-4xl">→</span>
  <img src="./image1.png" width="300" />
</div>

<style>
.slidev-layout.default { display: flex; flex-direction: column; }
</style>

---
layout: section
---

# Part 1 - Context

---

# January: what we actually wanted

- An **eventually consistent** control plane — the Kubernetes model
  - Declare desired state, controllers reconcile toward it
  - Level-triggered
- We deliberately **did not** build on Kubernetes

<v-click>

**Why not?** The challenges of managing **etcd** at fleet scale:

- ~8 GB practical ceiling
- ~500k max objects before degraded performance
- Backup / restore / DR story tied to the cluster's own etcd (no **pitr**)

</v-click>

---

# Six months of CLM: what we learned

<div class="grid grid-cols-2 gap-8">
<div>

### The problems

- **Adapters were hard to develop and debug**
  - Adapter development became the bottleneck for our progress
  - [Example configuration](https://github.com/openshift-online/rosa-hyperfleet/blob/942b09d42b71f4cf5891317ca4b2879d8144fece/argocd/config/regional-cluster/hyperfleet-adapter1-chart/adapter-task-config.yaml)
  - Dependency on CLM team, lack of autonomy (example `maestro` to `kube-applier`)
- **Lots of moving parts** — CLM API, broker, sentinel, adapters, database
- **New custom tooling** - Everything built from the ground up, no opportunity to reuse
- **Velocity** — features took long to land (Cluster deletion ~6 months)

</div>
<div>

### The realization

What we really wanted all along was

## controller-runtime operators

The reconciler pattern, informers, work queues — the mature, well-understood Kubernetes toolchain.

</div>
</div>

---

# Rebuilding of Kubernetes semantics

<div/>

A few weeks ago, @Joel Speed pointed out that CLM was re-implementing the kube semantics — events, desired/observed state, reconciliation. Why not just use Kubernetes?

<v-click>

The only reason we hadn't just used Kubernetes was **etcd management**.

</v-click>

<v-click>

So we studied the direct route for a week: **controllers + kube-apiserver + etcd**

|  |  |
| --- | --- |
| **PITR** | Point-in-time recovery for application state living in etcd |
| **etcd scalability** | ~8 GB ceiling ⇒ sharding across multiple etcds |

</v-click>

---

# Why not a controller against Postgres?

<div/>

<v-click>

We assumed we **couldn't have kube semantics on Postgres**.

</v-click>

<v-click>

The hard part is the watch contract: a **commit-ordered event stream** per resource type. With naive sequences, a transaction can take seq *N* but commit **after** *N+1* — a watcher advances past *N* and misses it **forever**.

</v-click>

<v-click>

The known fixes are locks:

<div class="grid grid-cols-2 gap-6 mt-4">
<div class="border rounded p-4">

### Global lock

One counter for everything

❌ Very slow — every write in the system contends on one row

</div>
<div class="border rounded p-4">

### Per-GVK lock

One counter per resource type

❌ Still slow at scale — all Clusters serialize on one lock

</div>
</div>

</v-click>

<!--
This is the crux slide: the reason we never seriously considered Postgres before.
Commit ordering seems to require a serialization point, and the obvious
serialization points don't scale.
-->

---
layout: section
---

# Part 2 — Our proposal

`postgres-controller-backend`

---

# The key idea: buckets

<div/>

Split each GVK's objects into **buckets**; lock per **(GVK, bucket)** instead of per GVK.

<div class="grid grid-cols-2 gap-8">
<div>

- Each bucket contains a number of **slices**
- A slice is **all the CRs related to a single cluster**
  - Cluster, Placement, NodePool, …
- A client-side assigner maps object → bucket; parent and children co-locate

**Result:** write contention drops by the bucket count — each bucket has its own commit-ordered counter, its own ordering, its own doorbell. No global lock anywhere.

Controllers can scale horizontally and watch only their own buckets.

</div>
<div>

<img src="./buckets.png" width="400" />

</div>
</div>

---

# Performance: it's fast enough — with a big margin

<div/>

AWS RDS Multi-AZ (synchronous commit), stored-procedure write path, 64 buckets:

| Instance | vCPU | Writes/s | p50 | p99 |
| --- | --- | --- | --- | --- |
| db.m6g.large | 2 | 2,852 | 20.0 ms | 68.5 ms |
| **db.m6g.2xlarge** | **8** | **9,622** | **6.1 ms** | **13.2 ms** |
| db.m6g.8xlarge | 32 | 11,728 | 5.0 ms | 7.7 ms |

<v-click>

**We need ~200 writes/s for a 5,000-cluster fleet.**
A 2xlarge gives us **~9.6k** — roughly **50× headroom**.

Zero serialization failures, zero sequence gaps, zero invariant violations across all runs.

</v-click>

---

# Correctness: tested by forcing the failures

<div/>

Every mechanism is justified by one of **6 named invariants** (commit-ordered sequences, no regression across failover, exactly-once delivery, RV monotonicity, compaction safety, optimistic concurrency).

Every invariant has a **deterministic test that forces the bad interleaving** — 21 race tests in total:

| Theme | What they prove |
| --- | --- |
| Sequence integrity | Concurrent writes, ambiguous commits, 409 rollbacks never create gaps or duplicates |
| Watch delivery | No event swallowed by debouncing, doorbell loss, or coalescing |
| Compaction & failover | Stale watchers get `410 Gone` — never a silent skip |

Plus **Toxiproxy** fault injection (TCP RST, killed connections mid-COMMIT) — and a **continuous verifier** that runs the same invariant checks in production.

---

# Feature parity — and beyond CLM

<div/>

Replacing CLM with this approach took ~2 weeks of design and ~1 week of implementation.

- ✅ **Feature parity** with CLM plus...
- ✅ **Cluster deletion** — with Kubernetes finalizers (dying objects stay visible until cleanup completes)
- ✅ **NodePools** — independent of clusters
- ✅ **Maestro replaced with kube-applier**

---

# Migrating a controller

<div/>

The `examples/` directory has the **same controller implemented twice** — once against etcd, once against Postgres. The reconciler doesn't change; the manager wiring does:

```go
mgr, _ := pgruntime.NewManager(pgruntime.Options{
    Scheme: scheme,
    DSN:    dsn,     // that's it — connection pooling and schema migration are internal
    Logger: log,
})

(&GreetingReconciler{Client: mgr.GetClient()}).SetupWithManager(mgr)
mgr.Start(ctx)
```

- Standard `manager.Manager` comes back — reconcile loops, `Owns()` watches, optimistic concurrency all behave as they always have
- Full migration guide with a line-count breakdown and step-by-step checklist in `examples/README.md`

<!--
Live-demo candidate: show the side-by-side diff of the two example controllers.
-->

---

# Mapping CLM components

| | |
| --- | --- |
| **Database (Postgres)** | ✅ Retained |
| **Adapters** | ➡️ Become native **controller-runtime** controllers |
| **CLM API** | ❌ Removed |
| **Sentinel** | ❌ Removed |
| **Broker** | ❌ Removed |

<v-click>

## Fewer components, same guarantees.

</v-click>

<!--
One database, N controllers. Everything between them is gone.
(An API server fronting the database is planned — it's just another writer using the library.)
-->

---

# Is this generalizable?

<div/>

Any system that follows the **reconciler pattern against kube-apiserver + etcd** can use this, if:

1. **You control all the clients** — every writer uses the library with the same configuration (nothing server-side validates writers; the guarantees hold because every writer is yours)
2. **The workload is sliceable** — it partitions into buckets, so you'll see similar performance

<v-click>

<div class="mt-8"></div>

## ⚠️ Pitfall: this *feels* like kube, but it is not kube

- No admission webhooks
- No RBAC
- No owner-reference GC cascade (for now)

**Mitigation:** keep clear documentation of what **IS** supported vs what is **NOT**.

</v-click>

---
layout: center
---

# Summary

<div/>

- **The proposal:**
  - controller-runtime controllers on plain Postgres
  - buckets make the watch contract scale.
- **Why:** we want **controller-runtime** operator semantics **without operating etcd**.
- **Why removal of CLM:** Same guarantees achieved with **fewer components** and **increased developer velocity**.
- **Perfscale** 2xlarge **~9.6k writes/s** measured (**~200/s** needed).

---
layout: center
---

# Questions?

<div/>

Repo: [postgres-controller-backend](https://github.com/jmelis/postgres-controller-backend/)

[DESIGN.md](https://github.com/jmelis/postgres-controller-backend/blob/main/DESIGN.md) · [WALKTHROUGH.md](https://github.com/jmelis/postgres-controller-backend/blob/main/WALKTHROUGH.md) · [ARCHITECTURE_COMPARISON.md](https://github.com/jmelis/postgres-controller-backend/blob/main/ARCHITECTURE_COMPARISON.md) · [loadtest/README.md](https://github.com/jmelis/postgres-controller-backend/blob/main/loadtest/README.md)

---
layout: section
---

# Backup Slides

Technical deep dives

---

# Deep dive: how List/Watch works

<div/>

**Polling is the correctness mechanism; the doorbell only changes *when* a poll happens.**

<div class="grid grid-cols-2 gap-6">
<div>

### List
- One `REPEATABLE READ` transaction: read epoch + counters (build the resourceVersion), then the rows
- Snapshot and RV are the same instant — no skew window handing off into Watch

### resourceVersion
```
e7|b2:1044,b5:902,b9:4123
```
Timeline **epoch** + per-bucket high-water-mark **vector**. Failover bumps the epoch ⇒ stale watchers get `410 Gone` and relist — never a silent miss.

</div>
<div>

### Watch
- Single-goroutine poll loop per watcher: `SELECT … WHERE seq > hwm ORDER BY seq`
- **Baseline poll every 5 s** — the sole guarantee
- `LISTEN/NOTIFY` doorbell requests an early poll (100 ms debounce, leading + trailing edge)
- **Total doorbell loss costs latency, never events**
- Typical write→delivery: **under 100 ms** (p99 62 ms measured)

</div>
</div>

---

# Deep dive: how bucketing works

<div class="grid grid-cols-2 gap-6">
<div>

### Client-side partitioning
- A caller-supplied function maps (namespace, name) → bucket; the DB stores the ID and never re-shards
- Bucket topology is part of the shared writer configuration, fixed for the deployment's life

### Per-(GVK, bucket) commit-ordered counters
```sql
INSERT INTO gvk_bucket_counters …
ON CONFLICT (bucket_id, gvk)
DO UPDATE SET current_seq = current_seq + 1
```

</div>
<div>

### Why it's correct
- The counter update takes an **exclusive row lock held until COMMIT** ⇒ commit order **=** sequence order, and aborts leave no gaps
- That lock is also the throughput ceiling **per bucket** — which is exactly why many buckets scale near-linearly

### Why it's fast
- `fillfactor = 50` keeps the hottest rows HOT-updatable
- Counters created on first use — no global sequence anywhere

</div>
</div>

---

# Deep dive: horizontal scaling of controller replicas

<div/>

- The **bucket is the unit of concurrency** — so bucket count caps the maximum number of controller replicas
- Replicas shard the bucket space; each replica watches and reconciles its own buckets
- No leases, no fencing: **optimistic concurrency (`object_version` → 409)** resolves any overlap during rebalancing or crash recovery — a stale writer simply loses the CAS

<v-click>

### Sizing

| Tier | Steady RPS | Buckets needed |
| --- | --- | --- |
| 5,000 clusters | 187 | 1 |
| 50,000 clusters | 1,870 | 4–8 |

Recommended default: **16 buckets**. Expanding later = epoch-bump migration — the same mechanism as failover: all watchers get `410`, relist, carry on.

</v-click>

---

# Deep dive: the stored procedure

<div/>

One server-side call, `pgctl_write()`, does the entire write path:

```sql
BEGIN;
SELECT * FROM pgctl_write(gvk, ns, name, bucket, expected_version,
                          force_write, spec, status, metadata, deletion_ts);
-- returns (uid, object_version, seq, changed)
COMMIT;
-- doorbell AFTER commit, only if changed:
SELECT pg_notify('resource_changes_b' || bucket, '');
```

1. **No-op suppression** — content-equal writes consume no seq, emit no event (kube semantics)
2. **Counter increment** — the exclusive row lock that serializes commit order
3. **Upsert with `object_version` check** — mismatch raises ⇒ whole txn (incl. counter) rolls back cleanly

<v-click>

**Why:** 3 round-trips → 1, and `pg_notify` moved outside the txn (its queue lock serializes *all* concurrent commits). Together: **~41% more per-bucket throughput** and near-linear multi-bucket scaling.

</v-click>
