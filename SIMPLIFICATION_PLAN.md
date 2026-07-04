# Simplification & Hardening Plan

**Status:** Proposed · **Scope:** Implements the findings of the 2026-07 review — structural simplifications, latent-bug fixes, and a controller-runtime-shaped interface — without weakening any invariant (I1–I8) or the scale targets in DESIGN.md §4.

**Prime rule — bug-first:** every bug fix in this plan starts with a regression test that is written *before* the fix and **demonstrably fails against the current code** (run it, record the failure mode). The fix lands only when that test flips to green with no other test regressing. A fix without a previously-failing test does not count as confirmed.

Decisions already made (from review discussion):

- **Split spec/status ownership stays** — some deployments have the API server writing spec and a controller writing status. Two fencing *domains* are kept; the two identical lease *tables* are merged into one.
- **Sub-200ms watch latency is required** — the LISTEN/NOTIFY doorbell stays; the watcher's internal concurrency is restructured instead.

---

## Bug Catalog (confirm each with a failing test first)

| ID | Location | Defect | Invariant at stake |
|----|----------|--------|--------------------|
| B1 | `internal/reader/watcher.go` (`debouncedPoll`) | Trailing-poll goroutine calls `w.poll` concurrently with the main loop → data race on the unprotected `hwm` map **and** concurrent use of a single `*pgx.Conn` (pgx `ErrConnBusy`); a conn-busy error on a baseline poll terminates the watcher spuriously. | I5 (availability of the delivery path) |
| B2 | `internal/reader/watcher.go:174,186` | Return value of debounced/trailing `w.poll` is discarded — a `410 Gone` (epoch mismatch or sub-horizon hwm) detected on a doorbell-triggered poll is silently dropped; the watcher keeps running as if healthy. | I6/I7 (410 must surface) |
| B3 | `internal/reader/watcher.go` (`pollBucket`) | Horizon check and row query are two statements with no shared snapshot. A compactor commit between them deletes tombstones in `(hwm, newHorizon]` → the watcher **silently skips Deleted events and never receives 410**. R7 does not catch this because it never interleaves compaction *inside* a poll cycle. | **I7 — direct violation** |
| B4 | `internal/verifier/verifier.go` (`checkEvent`) | I1 contiguity check false-positives under coalescing: two rapid updates to the same object between polls leave only the latest seq in the table; the gap is not compaction-explained → spurious I1 violation (a production page). Load tests dodge this only because every write uses a unique name. | Verifier fidelity (I5 permits coalescing) |
| B5 | `internal/verifier/verifier.go` (`seenKeys`) | Unbounded map growth in a component §6 defines as a *permanent* production consumer. Also redundant: any duplicate delivery necessarily has `seq <= hwm`, which the monotonicity check already flags. | Verifier operability |
| B6 | `internal/writer/writer.go` (create path) | Create (`ExpectedVersion == 0`) colliding with an existing row surfaces a raw Postgres duplicate-key error instead of a typed conflict. Counter rollback is correct (I1 holds); this is an error-typing defect that breaks caller retry/read-back logic. | Client protocol (§3.3 client rules) |

---

## Phase 1 — Regression tests for B1–B6 (no production code changes)

New tests live next to the existing race catalog. Each must fail against `main` before Phase 2 begins; record the observed failure (race report, wrong result, missing error) in the test's doc comment.

1. **`test/race/r13_concurrent_poll_test.go` (B1).** Force the overlap deterministically: `WatchHooks.BeforePoll` blocks the trailing-poll goroutine on a channel while the baseline ticker fires (short `BaselineInterval`, doorbell sent just after a leading poll so the trailing poll is armed). Run under `-race`. Expected current failure: race detector report on `hwm` and/or `ErrConnBusy` watcher termination.
2. **`test/race/r14_debounced_410_test.go` (B2).** Start a watcher, bump `cluster_epoch.timeline_id`, then deliver a doorbell so the *debounced* path polls (long `BaselineInterval` so the baseline timer cannot rescue the assertion). Assert `Run` terminates with `ErrGone` within the debounce window. Expected current failure: watcher keeps running; test times out waiting for termination.
3. **`test/race/r15_compaction_mid_poll_test.go` (B3).** Requires one new test seam in the watcher: a hook point between horizon check and row query (`WatchHooks` gains `AfterHorizonCheck(bucketID int)` or equivalent — in the Phase 2 rewrite this point still exists inside the per-poll transaction). Scenario: watcher hwm behind existing tombstones → pause at the seam → run `compaction.Compact` past the watcher's hwm → resume. Assert **no silent skip**: the poll must either deliver the Deleted events (snapshot predates the compaction commit) or return `ErrGone`. Expected current failure: poll returns fewer events, no error, hwm advances past the compacted range.
4. **`internal/verifier/verifier_coalescing_test.go` (B4).** Write the *same* object twice between verifier polls (pause verifier polling via its interval, or feed events directly to `checkEvent`). Assert zero violations. Expected current failure: spurious I1 violation.
5. **`internal/verifier/verifier_dup_test.go` (B5).** Feed a duplicate `(bucket, seq)` event and assert a violation is still reported *after* `seenKeys` is removed (this test defines the post-fix contract: duplicate ⇒ `seq <= hwm` ⇒ flagged as I5). Also assert, by inspection of the struct (no map field), that per-event state is O(buckets), not O(events).
6. **`internal/writer/writer_create_conflict_test.go` (B6).** Create an object, then create the same key again with `ExpectedVersion == 0`. Assert a typed `ErrAlreadyExists` (new sentinel in `internal/writer/errors.go`) and that the counter increment rolled back (next successful write gets the expected seq — I1). Expected current failure: wrapped `*pgconn.PgError` 23505, no sentinel match.

**Exit criteria:** all six tests exist, run under `make test-race` / `make test-integration`, and fail against current `main` for the documented reason.

---

## Phase 2 — Watcher rewrite: single-goroutine scheduler + snapshot polls (fixes B1, B2, B3)

Rewrite `internal/reader/watcher.go` around two structural changes; the point is that B1/B2 become *impossible by construction*, not defended by ordering discipline.

**2a. Single-goroutine scheduling.** One loop owns all polling and a single timer. The listen goroutine survives only to forward notifications into a 1-buffered channel (as today). Scheduling state (`lastPoll`, `doorbellPending`) lives as plain locals in the loop:

- Timer deadline = `lastPoll + BaselineInterval`, or `max(lastPoll + DebounceFloor, now)` when a doorbell is pending — leading edge preserved (doorbell with `now - lastPoll >= DebounceFloor` polls immediately), trailing edge preserved (doorbell during the floor schedules exactly one poll at `lastPoll + DebounceFloor`).
- Delete: `dirty atomic.Bool`, the `mu sync.Mutex`, the spawned trailing-poll goroutine, `debouncedPoll` entirely. `hwm` is only ever touched from the loop goroutine.
- Every poll error is handled uniformly: `ErrGone` and fatal errors terminate `Run` with that error (this is the B2 fix); the events channel closes.
- R2's "clear-before-snapshot" defense becomes moot — a doorbell arriving mid-poll sits in the buffered channel and schedules the next poll. Keep `TestR2_*` as delivery/latency regression tests; update their doc comments to say the interleaving is now structurally excluded.

**2b. One `REPEATABLE READ` read-only transaction per poll cycle** (the B3 fix, mirroring what `List` already does): epoch check, per-bucket horizon checks, and per-bucket row queries all read the same snapshot. Compaction committing mid-poll can no longer create a window where tombstones are gone but the horizon predates the check.

- Latency note: sub-200ms delivery is unaffected — this changes transaction framing, not scheduling.
- Bookmark support (feeds Phase 6): at the end of each poll cycle, read per-bucket `current_seq` inside the same snapshot and emit a progress/bookmark event carrying the updated composite RV. Cheap now, required later for informer RV advancement without relist.

**Exit criteria:** r13/r14/r15 green; R1–R12 and toxirace unchanged and green; `make test-race-stress` green; watcher LOC and goroutine count reduced (expect ~60–80 lines net deletion in `watcher.go`).

---

## Phase 3 — Merge the lease tables (one table, two domains)

Replace `bucket_spec_leases` + `bucket_status_leases` with:

```sql
CREATE TABLE bucket_leases (
    bucket_id  INT    NOT NULL,
    domain     TEXT   NOT NULL CHECK (domain IN ('spec', 'status')),
    holder     TEXT   NOT NULL,
    epoch      BIGINT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (bucket_id, domain)
);
```

The fence is a **row** lock, so the guarantee structure is unchanged: `FOR SHARE` on `(b, 'status')` does not conflict with a grant `UPDATE` on `(b, 'spec')` — R11 and R12 semantics are identical, just against rows instead of tables. This is greenfield (no deployments), so **edit `001_initial.sql` in place**; do not add a migration.

Code changes:

- `internal/lease/lease.go`: `Manager` gets a `domain` field instead of `tableName`; all SQL becomes static with `domain = $n` parameters. Delete `validTables` and every `fmt.Sprintf`-interpolated table name.
- `BothManager` collapses: `AcquireBoth` is one two-row upsert (`unnest(ARRAY['spec','status'])` or two `VALUES` rows) with `RETURNING domain, epoch`; `RenewBoth`/`ReleaseBoth` are single statements with `domain IN ('spec','status')` asserting `RowsAffected() == 2`. The explicit transactions and table-name loops go away (atomicity is now a property of the single statement).
- `internal/writer/writer.go`: `writeParams.fenceTable` → `domain`; the fence query becomes static SQL (`... FROM bucket_leases WHERE bucket_id=$1 AND domain=$2 AND holder=$3 AND epoch=$4 ...`).
- Update `test/race` helpers, R1/R11/R12, and DESIGN.md §3.1/§3.4 + README schema notes.

**Confirmation:** R1, R11, R12 (unmodified in *intent*, updated for schema) still force their interleavings and pass. Add one new assertion to R12: a `FOR SHARE` held on the status row does **not** block a concurrent spec-row grant (proves domain independence survived the merge).

**Exit criteria:** ~100 net lines removed from `lease.go`; one schema table fewer; no dynamic SQL identifiers anywhere; full suite green.

---

## Phase 4 — Verifier fixes (B4, B5) and cleanup

- **B4:** delete the "unexplained gap ⇒ I1 violation" alarm. Under coalescing, contiguity of *delivered* seqs is not an invariant. What remains honestly checkable from the stream: monotonic hwm (I3/I6), duplicate `seq <= hwm` (I5), and hwm-below-horizon ⇒ must have received 410 (I7). Document in the verifier's doc comment *why* stream-side gap checking is unsound (I5 explicitly permits coalescing). If gap auditing is wanted later, it must cross-check the table (gap seq superseded by a later `object_version` of some object) — out of scope here; leave a note, not code.
- **B5:** delete `seenKeys`; the duplicate check becomes `seq <= prevHWM ⇒ violation` (report as I5/I3). Verifier state becomes O(buckets).
- Replace `sortDurations` insertion sort with `sort.Slice`.
- Canary p99: cap `canaryTimes` (ring buffer or periodic truncation) — same unbounded-growth concern as seenKeys, same "runs forever" justification.

**Exit criteria:** Phase-1 tests 4 and 5 green; verifier remains the load-test oracle (`test/loadtest` unchanged and green); README §"Continuous Invariant Verifier" updated to the new check list.

---

## Phase 5 — Small simplifications (no behavior change)

- **Compactor → single CTE** in `internal/compaction/compactor.go`:
  ```sql
  WITH del AS (
      DELETE FROM kubernetes_resources
      WHERE deletion_timestamp IS NOT NULL
        AND deletion_timestamp < now() - $1::interval
      RETURNING bucket_id, gvk, gvk_bucket_seq
  )
  INSERT INTO compaction_horizon (bucket_id, gvk, compacted_seq)
  SELECT bucket_id, gvk, max(gvk_bucket_seq) FROM del GROUP BY 1, 2
  ON CONFLICT (bucket_id, gvk)
  DO UPDATE SET compacted_seq = GREATEST(compaction_horizon.compacted_seq, EXCLUDED.compacted_seq);
  ```
  Atomicity (I7: horizon never lags the delete) is now single-statement. Return the deleted count via a second CTE arm or `RETURNING` aggregation. Existing compaction tests + R7 must pass unchanged.
- **Empty doorbell payload:** nothing reads it; `pg_notify($channel, '')`. Removes the hand-built JSON (and the unescaped-GVK wart) in `writer.go`.
- **B6:** in the create path, map Postgres error 23505 on `kubernetes_resources_pkey` to a new sentinel `ErrAlreadyExists` in `internal/writer/errors.go`.

**Exit criteria:** Phase-1 test 6 green; compaction tests, R7, toxirace green; load test numbers within noise of the README baselines (the CTE and payload changes should be perf-neutral or better).

---

## Phase 6 — controller-runtime-shaped interface

The backend will be consumed by controller-runtime, which wants two seams: a **cache/watch source** (reflector-driven informers) and a **client** (reconciler reads/writes). Today's interfaces are close but speak pgx and internal types. Add an adapter package — `pkg/crbridge` (exported; `internal/` stays implementation) — and make the minimal internal changes that let it fit cleanly. Design first, then implement; this phase is deliberately last so it wraps the *simplified* internals.

**6a. Connection model: `*pgxpool.Pool` instead of `*pgx.Conn`.** Reconcilers run concurrently; `Writer` and `List` should accept a pool (or an interface satisfied by both, so race tests can keep injecting single conns for lock-ordering control). The single-writer guarantee is about *holder identity + fencing*, not connection count — concurrent writes from one holder to one bucket already serialize on the counter row lock. The watcher keeps its two dedicated conns internally (LISTEN requires a dedicated conn; the poll conn is single-goroutine after Phase 2).

**6b. Watch adapter implementing `watch.Interface`.** Wrap `Watcher.Events()`:

- `reader.Event` → `watch.Event` with `Added/Modified/Deleted` mapped 1:1, plus **`Bookmark`** events from the Phase-2b per-poll progress emission so informers advance RV without relist.
- Errors → `watch.Error` events with apimachinery statuses: `ErrGone` → `apierrors.NewResourceExpired(...)` (410) so the reflector relists — exactly the contract reflectors already implement. Fatal DB errors → `Status` with 500; the informer's backoff handles retry.
- `Stop()` → `Watcher.Stop()`.

**6c. ListerWatcher facade.** `crbridge.NewListerWatcher(pool, scheme, gvk, buckets)` returning the `cache.ListerWatcher` shape: `List` calls `reader.List`, sets the composite RV string (`resourceversion.RV.String()`) as the list's `metadata.resourceVersion`; `Watch(opts)` parses `opts.ResourceVersion` back via `resourceversion.Parse` into `WatcherConfig.StartRV`. This slots into a custom `cache.Cache` via controller-runtime's `cluster.Options.NewCache`.

**6d. Object mapping.** `model.Resource` ↔ `unstructured.Unstructured` (typed objects via scheme conversion on top):

- Per-object `metadata.resourceVersion` = `object_version` (this is what `Update` conflict detection compares); the composite RV is the *list/watch* resourceVersion. These differing representations are consistent with Kubernetes semantics (object RV and list RV are both opaque strings; they just come from different counters here). Document this loudly in `crbridge` — it is the least obvious contract in the system.
- `spec`/`status`/`metadata` JSONB columns assemble into the object; `uid`, timestamps, and `deletionTimestamp` map to `metadata`.
- **Bucket assignment** is a `crbridge` config function `func(namespace, name string) int` (e.g., FNV hash mod bucket count, or parent-cluster affinity per DESIGN §1). The storage layer keeps taking an explicit `BucketID`; policy lives in the adapter.

**6e. Client facade for reconcilers.** `crbridge.Client` implementing the subset of `client.Client` reconcilers actually use — `Get`, `List`, `Create`, `Update`, `Delete`, `Status().Update()`:

- `Create` → `Write` with `ExpectedVersion=0`; `ErrAlreadyExists` → `apierrors.NewAlreadyExists`.
- `Update` → `Write` with `ExpectedVersion` parsed from the object's RV; `ErrConflict` → `apierrors.NewConflict` (409 → controller-runtime requeue, the idiom reconcilers expect).
- `Status().Update()` → `WriteStatus` (same mapping).
- `Delete` → `Write` setting `DeletionTimestamp` (tombstone); finalizer handling stays in `metadata` JSON and is the reconciler's business, as upstream.
- Lease state (`holder`, `epoch` per bucket/domain) is injected by the lease manager that owns acquisition/renewal; `ErrFenceViolation` surfaces as a 409-class error that forces requeue — a replica that lost its lease must not mask that as success.
- `AmbiguousCommitError` + `ReadBack` stay internal: the facade runs the §3.3 read-back protocol itself so reconcilers never see ambiguity.

**Tests for Phase 6:** an envtest-style integration test — a real `SharedIndexInformer` (client-go reflector) driven by the `crbridge` ListerWatcher against the podman Postgres: relist-on-410 after epoch bump, bookmark RV advancement, create/update/conflict/delete round-trip through the facade, coalescing burst delivering final state. This doubles as an end-to-end proof that the composite-RV contract survives contact with the real reflector.

**Exit criteria:** `crbridge` package with the informer integration test green; no reconciler-facing API exposes pgx types, `model` internals, or raw RV structs.

---

## Order, verification, and acceptance

Phases land in order 1 → 2 → 3 → 4 → 5 → 6; each phase is a separate commit/PR with the full suite green (`make test`, `make test-race`, `make test-race-stress`, `make test-toxirace`). Phase 1 is the gate: **no fix merges before its regression test has been seen failing.**

Final acceptance:

1. All Phase-1 regression tests green; documented pre-fix failure mode in each test's comment.
2. R1–R12 + toxirace green, unchanged in intent (schema-touching updates only in R1/R11/R12 helpers).
3. `make test-load`: throughput/latency within noise of README baselines (per-bucket ceiling ≈1,000 w/s locally); zero verifier violations — now meaningful, since B4's false-positive source is gone.
4. Net production-code reduction on `internal/` (expect roughly 250–300 lines) with one fewer schema table and zero dynamic SQL identifiers.
5. DESIGN.md and README updated where mechanisms changed: §3.1 schema, §3.4 (single table, two domains), §3.6 (single-goroutine scheduler, snapshot polls, R2 note), §6 (verifier check list), race table (R13–R15 added).
