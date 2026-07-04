---
marp: true
paginate: true
theme: default
---

# Kubernetes Watch Semantics on PostgreSQL
## Replacing etcd for a 50,000-cluster fleet control plane

**Staff Engineering · Platform Storage**

<!--
SCRIPT:
Hi everyone. This talk is about a storage backend we built that lets Kubernetes
controllers run on top of plain PostgreSQL instead of etcd.

The headline is not "Postgres is faster than etcd." The headline is: the
Kubernetes watch protocol makes promises that a relational database does not
naturally keep, and if you don't understand exactly which promises those are,
you will build something that corrupts controller caches silently — no errors,
no alerts, just controllers acting on stale state forever.

I'll show you the three ways Postgres breaks those promises, the mechanisms we
used to fix each one, and — maybe most importantly — how we test race
conditions deterministically instead of hoping they show up in CI.
-->

---

# The problem

- Fleet control plane: **5,000 → 50,000 managed clusters**
- Every cluster is a set of Kubernetes resources in *our* control plane
- etcd: ~**8 GB practical ceiling**, one raft group, painful operations
- We want: RDS PostgreSQL. Managed, boring, scales, restorable.

**Everything else in this talk is the cost of that "boring."**

<!--
SCRIPT:
Context. We run a regional control plane that manages fleets of Kubernetes
clusters. Each managed cluster is represented as custom resources — think a
Cluster object, NodePools, addons — living in our control plane, reconciled by
controllers built on controller-runtime.

At 5,000 clusters, etcd is already the operational bottleneck. It has a
practical ceiling around 8 gigabytes, it's one raft group no matter how much
data you have, and day-2 operations — backup, restore, resizing — are where
incidents come from.

RDS Postgres Multi-AZ solves all of the operational problems. It's managed,
it's synchronously replicated, restore is a button. The catch is that
controllers don't talk to a database — they talk List/Watch. And List/Watch
makes very specific correctness promises that Postgres does not give you for
free. That gap is this entire talk.
-->

---

# What controllers actually need: List/Watch

1. `List` — snapshot of everything, stamped with a **resourceVersion**
2. `Watch(resourceVersion)` — *every* change after that point, **in order, exactly once**
3. Controller caches are built from this stream and **never re-verified**

> A missed event isn't an error. It's a cache that is **wrong forever**.

<!--
SCRIPT:
Quick refresher on the contract, because it's the whole game.

A controller starts by Listing: give me every object, plus a cursor called a
resourceVersion. Then it Watches from that cursor: send me every change after
this point, in order, exactly once. The controller builds an in-memory cache
from that stream, and here's the critical part — it never re-reads the
database to check the cache. The stream IS the truth.

So if your storage layer skips one event — not crashes, just quietly skips —
the controller's cache is wrong, and it stays wrong until the process
restarts, which might be weeks. It will happily "reconcile" reality against a
stale picture. These are the worst bugs in distributed systems: no error, no
log line, wrong behavior.

etcd gives you this exactly-once, ordered stream natively via raft. Our job
was to rebuild that guarantee on Postgres.
-->

---

# Why Postgres breaks it: three hazards

1. **Out-of-order commits** — sequence issued at N can *commit after* N+1
2. **Failover regression** — a promoted replica can re-issue "used" sequence numbers
3. **Split-brain writers** — two replicas both believe they own a shard

<!--
SCRIPT:
Three specific hazards. The first one is the subtle one, so let me spend a
minute on it — if you take one database subtlety home from this talk, make it
this one.

You need to order events, so you reach for a Postgres SEQUENCE. Transaction A
grabs sequence number 5. Transaction B grabs 6. But B is a small transaction
and commits first; A is slower and commits 50 milliseconds later. A watcher
polls: it sees 6, records "I've seen everything up to 6," and moves on. Then A
commits with number 5 — behind the watcher's cursor. That event is invisible.
Forever. And note: SEQUENCEs are also non-transactional, so aborted
transactions leave holes, which means you can't even tell a "gap that will
fill in" from a "gap that never will."

Hazard two: failover. A gapless counter is only trustworthy if failover never
loses an acknowledged commit. If it can, the promoted node re-issues sequence
numbers that watchers already consumed — same number, different payload —
and silently corrupts every downstream cache.

Hazard three: split-brain. Two controller replicas both think they own a
shard and both write. etcd solves this with raft consensus. We don't have
raft.
-->

---

# The cheat: we're not building etcd

This is **not** a general API server backend (that's [kine](https://github.com/k3s-io/kine)).
A fleet controller lets us assume:

| Assumption | What it buys us |
|---|---|
| Controller owns the write path | We can assign **leases** to writers |
| Resource types known at deploy time | Fixed **bucket** partitioning |
| One writer per bucket per sub-resource | Row locks instead of consensus |
| Single-primary, sync-replicated RDS | Failover never loses an acked commit |

<!--
SCRIPT:
Before the mechanisms — the honest disclaimer. We did not build a
general-purpose etcd replacement. Kine exists; if you want to run a whole
Kubernetes API server on SQL, use kine.

We exploited four constraints that a fleet controller satisfies and a general
API server doesn't.

One: writes come only from our reconcilers, not from arbitrary clients. So
writers can be told "you own this partition" via a lease.

Two: the set of resource types is closed and known at deploy time. So we can
statically partition resources into a fixed number of buckets — related
objects share a bucket, a NodePool lives in its Cluster's bucket.

Three: at most one writer per bucket per sub-resource at a time. That single
assumption is what lets us replace raft consensus with ordinary row-level
locks. This is the big trade.

Four: single-primary Postgres with a synchronous standby. Synchronous is
load-bearing: an acknowledged commit exists on two nodes, so failover cannot
rewind it. That's what makes hazard two solvable at all.

Weaken any of these and the design is invalid. That's why they're written at
the top of the README, not buried in an appendix.
-->

---

# Correctness first: named invariants

**8 invariants (I1–I8)** — the promises. Highlights:

- **I1 Gapless issuance** — per (type, bucket): seq = 1, 2, 3, … no holes
- **I2 Commit order = sequence order** — never observe B before A if seq(A) < seq(B)
- **I4 Single writer** — a stale lease holder **cannot commit**, even if it thinks it can
- **I5 Exactly-once delivery** — regardless of any push mechanism's behavior
- **I7 Compaction safety** — a skipped event is *impossible*; you get `410 Gone` instead

Every invariant → a named race → a **deterministic test**.

<!--
SCRIPT:
Methodology slide, and honestly the part of this project I'd defend hardest.

We wrote down eight invariants — the actual promises. The five that matter
most: sequence numbers are gapless per partition. Commit order equals
sequence order — that's the fix target for the out-of-order hazard. A stale
writer cannot commit, even if it believes it holds the lease — note the
wording: not "shouldn't write," CANNOT COMMIT. Exactly-once delivery no
matter what any notification mechanism does. And compaction can never
silently eat an event — the worst you can get is an explicit 410 error that
tells the client to resync.

The discipline: every mechanism in the design must justify itself by naming
which invariant it upholds. Every invariant has a named attack — a race or
failure that would violate it. And every attack has a deterministic test that
forces the interleaving. Not "run it 1000 times and hope." Forces it. I'll
show you how at the end.

If a mechanism can't name its invariant, it gets deleted. This pruned a lot
of cleverness out of earlier drafts.
-->

---

# Mechanism 1: gapless counters (I1, I2)

One counter **row** per (type, bucket) — not a SEQUENCE:

```sql
INSERT INTO gvk_bucket_counters (bucket_id, gvk, current_seq)
VALUES ($bucket, $gvk, 1)
ON CONFLICT (bucket_id, gvk)
DO UPDATE SET current_seq = gvk_bucket_counters.current_seq + 1
RETURNING current_seq;
```

- Exclusive row lock **held to COMMIT** → issuance order **= commit order**
- Abort ⇒ increment rolls back ⇒ **no holes**
- Serializes writers per bucket — that's the point. Scale = more buckets.

<!--
SCRIPT:
Mechanism one, fixing the out-of-order hazard. We don't use a SEQUENCE. We
use an ordinary table row per (resource-type, bucket) and increment it with
this upsert inside the write transaction.

Why this works is pure Postgres row-locking: the UPDATE takes an exclusive
lock on the counter row, and Postgres holds row locks until COMMIT. So if
transaction A took sequence 5, transaction B physically cannot take 6 until A
has finished committing or aborting. Issuance order and commit order are the
same order, by lock discipline. Hazard one gone — that's I2.

And because it's a transactional row, an aborted transaction rolls back its
increment. No holes — that's I1. A watcher that sees sequence 6 KNOWS 5
exists and is visible.

The obvious objection: you've serialized all writes in a bucket through one
row lock! Yes. Deliberately. That's the correctness mechanism. Throughput
comes from having many buckets — independent counters, no shared lock. The
one tuning note on the slide: fillfactor 50 on this table keeps these
super-hot updates HOT — heap-only tuples — so they don't bloat the index.

The first-write race — two transactions racing to create the counter row —
is handled by the ON CONFLICT; there's a test proving the result is exactly
{1, 2}.
-->

---

# Mechanism 2: fencing with a row lock, not raft (I4)

Writer, inside every write transaction:

```sql
SELECT 1 FROM bucket_leases
 WHERE bucket_id = $b AND domain = 'spec'
   AND holder = $me AND epoch = $my_epoch
   AND expires_at > now()
 FOR SHARE;      -- zero rows => abort
```

Coordinator, to reassign: `UPDATE bucket_leases SET holder=$new, epoch=epoch+1 …`

**`UPDATE` needs the exclusive lock → it *blocks* behind every in-flight `FOR SHARE`.**

<!--
SCRIPT:
Mechanism two: single-writer without consensus.

Each bucket has a lease row: who holds it, an epoch number that increments on
every acquisition, an expiry time. Every write transaction starts by reading
its own lease row with FOR SHARE — a shared row lock — and, crucially, holds
that lock until COMMIT. If the row doesn't match — wrong holder, old epoch,
expired — zero rows come back and the writer aborts.

Now the elegant part. The naive version of this has a hole: check the lease,
then your process pauses — GC, VM migration, whatever — for 40 seconds. The
coordinator reassigns the lease to someone else. Your COMMIT finally arrives…
and lands. You just committed under an epoch that's no longer authoritative.
Classic time-of-check to time-of-use.

The fix costs zero extra queries: reassignment is an UPDATE on that same row,
and in Postgres a row UPDATE needs the exclusive lock, which CONFLICTS with
FOR SHARE. So the coordinator's grant physically blocks until every in-flight
fenced write on that bucket finishes. Two possible orders, both safe: the old
writer commits first — fine, it was still the legitimate holder for that
write. Or the grant commits first — and the late writer's fence check now
sees the new epoch and aborts.

There is no interleaving where a stale write commits after a new epoch
exists. And notice what the clock is doing: nothing, for safety. expires_at
only bounds how long a dead holder blocks reassignment. Clock skew can delay
a handover; it cannot violate single-writer. Liveness from time, safety from
locks.
-->

---

# The full write path

```sql
BEGIN;
  -- (a) FENCE     lease row FOR SHARE, held to COMMIT        (I4)
  -- (b) SEQUENCE  counter upsert, exclusive row lock          (I1, I2)
  -- (c) UPSERT    resource row … WHERE object_version = $expected  (I8: 409)
  -- (d) DOORBELL  pg_notify('resource_changes_b<N>', '')     (latency only)
COMMIT;
```

- 409 conflict ⇒ **ROLLBACK** — never retry in-txn (counter must abort too)
- Connection died mid-COMMIT? **Read back** the row + counter, then retry — the write is idempotent to *verify*

<!--
SCRIPT:
Put together, every write is one transaction, four steps. Fence. Take the next
sequence number. Upsert the resource with an optimistic-concurrency check —
the WHERE object_version equals expected clause; zero rows updated means
someone else got there first and you surface a 409, same contract as the real
API server. Then pg_notify — a doorbell. More on that in a second.

Two client rules that look pedantic and are load-bearing.

First: on a 409 you must ROLL BACK, never retry inside the same transaction —
because the counter increment has to abort with it, or you've burned a
sequence number and created a hole. Our client library makes this
structurally impossible: the transaction helper owns BEGIN and COMMIT, so
application code can't even express the buggy retry. There's a race-catalog
entry for it anyway.

Second: ambiguous commits. The connection drops after you send COMMIT but
before the OK arrives. Did it land? You cannot just retry — you might
double-write and burn a sequence number. The protocol: read back the row and
the counter. The write is identified by object_version and seq, so it's
idempotent to VERIFY even though it isn't idempotent to blindly repeat. Check,
then retry only if it genuinely didn't land. There's a test that severs the
TCP connection at exactly that protocol moment.
-->

---

# Mechanism 3: the watch — polling is the truth

**Push (LISTEN/NOTIFY) is a latency hint. Poll is the correctness mechanism.**

- One goroutine owns all polling; each cycle = one `REPEATABLE READ` snapshot
- `WHERE seq > hwm ORDER BY seq` per leased bucket → advance high-water mark
- **5 s baseline timer** — the *only* thing correctness rests on
- Doorbell just makes the next poll happen sooner (100 ms debounce)
- Doorbell connection dies silently? **Nothing is lost.** Next baseline poll reconciles.

<!--
SCRIPT:
Mechanism three, the watch side, and the design decision I'd argue is the most
transferable to your own systems.

Postgres has LISTEN/NOTIFY — push notifications. The classic architecture is:
stream events via NOTIFY, and add a fallback poll for when it breaks. We
inverted that. Polling is the primary and ONLY correctness mechanism. NOTIFY
is purely a latency optimization.

Why? Because LISTEN/NOTIFY fails silently. The connection drops, Postgres
buffers overflow under load, failover kills every listener — and you get no
error. If your correctness depends on push, every one of those failure modes
is a hole you have to detect and patch with catch-up logic, and the catch-up
logic has its own ordering races.

Instead: a watcher polls every leased bucket — WHERE sequence greater than my
high-water mark, ORDER BY sequence — on a 5-second baseline timer,
unconditionally. Because sequences are gapless and commit-ordered, this simple
query IS exactly-once delivery. The doorbell only makes the next poll happen
sooner — 100 milliseconds instead of 5 seconds — with a debounce so a write
burst coalesces into one poll. The notify payload is literally empty; even a
corrupted doorbell can't lie to us, because it carries no information. Kill
the doorbell entirely and you lose nothing but latency. We have a test that
does exactly that — TCP-kills the LISTEN socket mid-burst via Toxiproxy —
and asserts every event still arrives.

Two implementation details that carry real weight. Single goroutine owns all
polling and the high-water-mark state — no concurrent polls, no locking, no
out-of-order dispatch; the listener just forwards into a channel. And each
poll cycle runs in one REPEATABLE READ transaction, so it sees one consistent
snapshot — a compaction running mid-poll is invisible to it. Boring Postgres
features doing correctness work.
-->

---

# Failover: timeline epochs (I3, I6)

resourceVersion is composite: `e7|b2:1044,b5:902,b9:4123`
*(timeline epoch | per-bucket high-water marks)*

- Sync standby ⇒ **no acked commit is ever lost** (RDS Multi-AZ)
- Every promotion bumps the **epoch** → old cursors get `410 Gone` → clients relist
- Backup **restore** legitimately rewinds state ⇒ runbook bumps epoch manually
- Writer tripwire: cached max seq > `current_seq` on reconnect ⇒ freeze bucket + page

<!--
SCRIPT:
Failover — hazard two. Defense in depth, three layers.

Layer one is buying the right database. RDS Multi-AZ with a SYNCHRONOUS
standby: a commit isn't acknowledged until it's on both nodes, so promotion
cannot lose an acked write, so the counter cannot rewind. That property is
purchased, not built — and it's why the assumptions slide said
single-region, single-primary.

Layer two: assume layer one fails anyway. The resourceVersion cursor isn't
just sequence numbers — it's prefixed with a timeline epoch that increments
on every promotion. Any cursor from a previous epoch gets 410 Gone, the
standard Kubernetes "your cursor is too old" error, and the client relists.
That's also the answer for backup restore — the one scenario sync
replication genuinely can't cover, because a restore legitimately rewinds
state. The runbook bumps the epoch before accepting writes, and every client
in the fleet relists instead of trusting a stale cursor. Same mechanism, one
code path, and it doubles as our resharding story: changing the bucket count
is just an epoch bump plus relist.

Layer three, pure paranoia: every writer caches the highest sequence it ever
committed; on reconnect, if the database's counter is LOWER than what it
already committed, it refuses to write and pages a human. With sync
replication that tripwire must never fire. If it fires, we don't want
software cleverness — we want the bucket frozen and a person looking at it.
-->

---

# Compaction without eating events (I7)

Deleted objects = **tombstones** (watchers must see the delete). Purge atomically:

```sql
WITH deleted AS (
  DELETE FROM kubernetes_resources
  WHERE … deletion_timestamp < now() - $retention
  RETURNING gvk_bucket_seq
)
INSERT INTO compaction_horizon … MAX(gvk_bucket_seq) …
ON CONFLICT … DO UPDATE SET compacted_seq = GREATEST(old, new);
```

Watcher below the horizon ⇒ `410 Gone` + relist. **Loud, never silent.**

<!--
SCRIPT:
One more place events can silently vanish: cleanup.

Deletes can't just remove rows — a watcher that hasn't polled yet needs to
see the deletion event, or its cache keeps a ghost object forever. So deletes
write a tombstone, and a compactor purges old tombstones later.

But now the compactor can eat events: it deletes tombstone with sequence 900,
and a slow watcher whose cursor is at 850 polls — 900 just… isn't there.
Silent gap, the exact failure class this whole design exists to prevent.

The fix is a horizon: "everything at or below sequence N may be compacted
away." A watcher whose cursor is below the horizon gets 410 Gone and relists.
Annoying, loud, correct.

The subtlety worth stealing is on the slide: the delete and the horizon
advance are ONE statement — a CTE, so one transaction, one atomic visibility
event. If they were two statements and the compactor crashed between them,
you'd have deleted rows with no horizon explaining the gap — silent loss
again. The GREATEST in the upsert makes the horizon monotonic even under
concurrent compactor runs. And remember the previous slide: poll cycles run
in REPEATABLE READ, so a compaction landing mid-poll is invisible until the
next cycle, which then sees delete-plus-horizon together or not at all.
Retention is 24 hours, with an alarm when any watcher's cursor age crosses
half of it — you find slow watchers days before they'd hit a 410.
-->

---

# Spec/status split: two leases, one stream

Kubernetes splits ownership: API server writes **spec**, controller writes **status**.

- `bucket_leases` has a `domain` column: (`bucket_id`, `'spec'`) and (`bucket_id`, `'status'`) are **independent rows** ⇒ independent fences
- Both paths share **one counter** and one `object_version` ⇒ watchers see a **single ordered stream**
- Fence locks never conflict across domains; the counter lock serializes ordering

<!--
SCRIPT:
One Kubernetes-specific wrinkle. Spec — desired state — and status — observed
state — are often written by different components. So "single writer per
bucket" is really "single writer per bucket per sub-resource."

The implementation is small and I like it for that: the lease table has a
domain column, spec or status. Two rows per bucket, fenced completely
independently with the same FOR SHARE mechanism — a status lease handover
doesn't block spec writers, because row locks are per-row. The fence-expiry
race I showed earlier is re-tested separately for the status domain.

But both write paths share the same sequence counter and the same
object_version column. So a spec writer and a status writer on the same
bucket contend only on the counter row — which is exactly the serialization
we want, because watchers see one totally-ordered gapless stream covering
both kinds of changes. Independent fencing, shared ordering. For the common
case where one controller owns both, there's an AcquireBoth that grabs both
lease rows in a single statement.
-->

---

# Testing: force the race, don't hope for it

**15-entry race catalog (R1–R15).** Every race names its invariant, interleaving, defense, and test.

- **Test hooks** pause a writer *between fence and COMMIT* → assert the lease grant blocks
- **Two DB sessions** with explicit lock ordering — deterministic interleavings
- **Toxiproxy** kills TCP at exact protocol moments (mid-COMMIT, LISTEN socket)
- All under `-race`; timing-sensitive ones repeat **100–1,000×**

<!--
SCRIPT:
The part I most want you to take back to your teams: how we test this.

The standard approach to race conditions is to run tests many times under
load and hope the bad interleaving shows up. It usually doesn't — the window
might be microseconds wide — and then it shows up in production during a GC
pause.

We maintain a catalog of fifteen named races. Each entry names the invariant
at stake, the exact interleaving that would break it, the defense, and a test
that FORCES that interleaving deterministically. Three techniques, all cheap.

One: transaction hooks in the writer. The test pauses a real writer at the
exact point between the fence check and COMMIT — the fence-expiry window —
then drives the coordinator's lease grant from another connection and asserts
it blocks until the writer finishes, in both orderings. The
microsecond-window race becomes a deterministic unit test.

Two: two raw database sessions with explicit lock ordering, for DB-level
interleavings like the counter first-write race.

Three: Toxiproxy for network faults at exact protocol moments — drop the
connection after COMMIT is sent but before the OK, to force the ambiguous
commit; RST the LISTEN socket mid-burst to prove doorbell loss loses nothing.

Everything runs under Go's race detector, and the timing-sensitive ones
repeat hundreds of times in a stress mode. The full catalog gates every load
test: races don't scale down, so there's no point measuring throughput on a
build that hasn't proven the interleavings.
-->

---

# Testing doesn't stop at GA: the verifier

A permanent low-priority consumer, in **every environment**, on the ordinary poll path:

- **Monotonic high-water marks** — any regression or duplicate pages
- **Every gap must be explained** by the compaction horizon
- **Canary writes** per bucket → write-to-delivery p99 (doorbell health)
- I3/I4 violation ⇒ **write-freeze the bucket** + page
- Same code = acceptance oracle for load tests

<!--
SCRIPT:
Last mechanism. Correctness that's only verified pre-launch decays — schema
migrations, driver upgrades, an RDS maintenance event you didn't read the
notes for.

So a verifier runs permanently in every environment, production included. It
subscribes through the ordinary poll path — no privileged access, it sees
exactly what a real controller sees — and continuously checks the invariants
that are checkable from the stream: high-water marks only move forward, no
duplicates, and every gap in sequence numbers is explained by the compaction
horizon. One honest subtlety: it can't check gaplessness per-event, because
watch coalescing — two quick writes to one object delivering only the latest
— legitimately makes delivered sequences non-contiguous. Knowing precisely
what your monitor CAN'T see matters as much as what it can. Its state is
O(buckets), so it never becomes its own capacity problem.

It also writes a synthetic canary object per bucket and measures
write-to-delivery latency. That's the doorbell health check: remember, a dead
doorbell breaks no correctness, so nothing else would ever alert on it —
you'd just silently run at 5-second latency.

Any violation pages. A single-writer or regression violation additionally
freezes writes to that bucket — those two mean something impossible
happened, and we want a human, not a retry loop.

And the same verifier binary is the acceptance oracle for every load and
failover test. The judge in CI and the judge in production are the same code.
-->

---

# Does it perform? (local podman, 8-CPU laptop)

| Setup | Throughput | p50 / p99 |
|---|---|---|
| 1 bucket, 50 workers | **1,045 writes/s** | 33 ms / 231 ms |
| 16 buckets, 48 workers | **2,548 writes/s** | 18 ms / 45 ms |

Requirement: 5k clusters ≈ **187 RPS** steady · 50k ≈ **1,870 RPS**

Zero serialization failures · zero fencing false-positives · zero invariant violations

<!--
SCRIPT:
Performance, briefly — remember the prime directive was correctness first,
performance subordinate. These are Phase-1 numbers on a laptop running
Postgres in a container, so treat them as a floor, not a benchmark.

A single bucket — one counter row, fully serialized writes — sustains about
a thousand writes per second. Sixteen buckets scale to about two and a half
thousand, with p99 latency actually improving because per-bucket contention
drops.

Now the requirements line: 5,000 clusters need under 200 writes per second
steady. Fifty thousand need under two thousand. A laptop covers the top tier.
The intentional serialization bottleneck — that per-bucket counter lock —
costs us nothing at fleet-controller scale, because reconcilers write at
human-infrastructure rates, not web-request rates.

One real constraint to note: bucket count caps controller parallelism, since
leases are per-bucket. Default is 16, and growing it later is an epoch bump
plus relist — the failover mechanism doing double duty. And the verifier ran
attached to every load test: zero violations under sustained load.
-->

---

# Takeaways

1. **A missed event is worse than an error** — design for *silent-failure-impossible*, not failure-free
2. **SEQUENCEs can't order events** — a locked counter row makes commit order = sequence order
3. **Push notifies, poll delivers** — correctness on the poll, latency on the push
4. **Fencing = `FOR SHARE` vs `UPDATE`** — consensus-free single-writer when you can assume leases
5. **Name the invariant, name the race, force the interleaving** — deterministic race tests beat stress tests
6. **Ship the verifier** — invariants checked in production, forever

**Code, invariant catalog (I1–I8), race catalog (R1–R15): see DESIGN.md**

<!--
SCRIPT:
Six things to take home.

One: in cache-building protocols like List/Watch, a missed event is worse
than an error, because nothing retries it. Design so silent loss is
impossible; loud failure — a 410 — is always acceptable.

Two: database sequences cannot order events, because issuance order isn't
commit order and aborts leave holes. A transactionally-locked counter row
gives you both properties, at the price of serializing writers — so
partition, and pay the price per partition.

Three: if you use push notifications, make them a latency hint, never a
correctness dependency. Polling against a gapless ordered log is trivially
exactly-once, and it turns "notify channel died" from an incident into a
non-event.

Four: single-writer fencing doesn't need consensus if your architecture can
assign leases — a FOR SHARE held to commit, conflicting with the grant
UPDATE, closes the stale-writer race with zero extra round trips. Safety
from locks, liveness from time.

Five: don't stress-test races and hope. Name the invariant, name the
interleaving, and build the hook that forces it deterministically.

Six: run your invariant checker in production forever. It's the only reason
to believe the guarantees still hold next year.

Everything — the design doc with the full invariant and race catalogs, the
schema, all fifteen race tests — is in the repo. Thank you. Questions?
-->
