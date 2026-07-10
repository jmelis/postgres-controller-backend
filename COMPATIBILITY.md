# Compatibility with controller-runtime

pgruntime implements the controller-runtime `manager.Manager`, `client.Client`,
and `cache.Cache` interfaces, but it is not a complete implementation of the
Kubernetes API machinery. This document is the honest inventory: everything
that does not work, what it does today, how it could be fixed, and how hard
that fix is.

## The supported surface (the "golden path")

Controllers that stay on this path migrate cleanly:

- Typed CRD-style objects with `Spec` and `Status` struct fields
- `Get`, `List` (namespace + label selectors), `Create`, `Update`,
  `Status().Update()`, `Delete`
- Finalizers and deletion timestamps (full dying-object lifecycle)
- `Owns()` / `Watches()` with predicates; owner references are stored and
  `EnqueueRequestForOwner` works
- One reconciler replica per bucket set, coordinated by bucket assignment

Everything below is off that path.

## Difficulty legend

| Rating | Meaning |
| --- | --- |
| **Easy** | Hours. Localized change, no schema or invariant impact. |
| **Medium** | Days. New mechanism, but fits the existing design. |
| **Hard** | Weeks / architectural. Touches storage schema, invariants, or re-implements a significant apiserver subsystem. |

Where a full fix is expensive, an **interim guard** (reject loudly instead of
misbehaving silently) is listed — guards are always Easy and are the priority,
because silent divergence is worse than a missing feature.

## Tier 1 — Loud failures

These return `MethodNotSupported` or panic today. Annoying but safe: migrators
find out immediately.

| # | Problem | Today | How to fix | Difficulty |
| --- | --- | --- | --- | --- |
| 1 | `Patch()` — `client.MergeFrom`, JSON patch, `Status().Patch()`. The most common migration blocker: idiomatic in most kubebuilder controllers and `controllerutil.CreateOrPatch`. | `MethodNotSupported` | Client-side patch: read current row, apply the patch document (merge patch = JSON merge per RFC 7386; JSON patch via a standard library; strategic-merge only matters for built-in types — CRDs fall back to merge patch at the apiserver too), then `WriteObject`/`WriteStatus` with the read row's version for optimistic concurrency, retry on 409. | **Medium** |
| 2 | Server-side apply (`client.Apply`, apply configurations) | `MethodNotSupported` | Real SSA needs `managedFields` tracking and structured-merge-diff against an OpenAPI schema — a large apiserver subsystem. A degraded mode (treat apply as create-or-full-update) is Medium but silently changes co-ownership semantics; not recommended. | **Hard** |
| 3 | `DeleteAllOf` | `MethodNotSupported` | List matching objects, issue deletes through the existing write path (per-object events must still be sequenced, so a bulk SQL delete is not an option). | **Easy** |
| 4 | `GetWebhookServer()` | Panics | Admission webhooks are an apiserver concept with no equivalent here. Replace the panic with a descriptive error. If in-process admission is ever wanted, add hook points in the write path (see Tier 2 #13). | **Easy** (error) |
| 5 | Subresources other than status (`scale`, `Status().Create()`, subresource `Get`) | `MethodNotSupported` | Subresource `Get` for status is trivial (read the row). `scale` requires per-type replica-path knowledge; implement only if a migrated controller needs it. | **Easy–Medium** |

## Tier 2 — Silent wrong behavior

These succeed while doing something different from the apiserver. This is the
dangerous tier: a migrated controller appears to work.

| # | Problem | Today | How to fix | Difficulty |
| --- | --- | --- | --- | --- |
| 6 | Field selectors ignored: `IndexField` accepts and discards the indexer; `List` never reads `FieldSelector`. `client.MatchingFields{...}` returns **all** objects of the GVK. | Silently returns unfiltered results | **Guard:** error on non-nil `FieldSelector`, and on `IndexField` registration (Easy). **Full fix:** store registered `IndexerFunc`s and evaluate them client-side as a List filter — correct because List already materializes all rows. JSONB expression indexes are a later optimization, not needed for correctness. | **Easy** guard / **Medium** fix |
| 7 | `DryRun` performs the real write on Create/Update/Delete | Mutates the database | **Guard:** reject any `DryRunAll` option (Easy). **Full fix:** run `pgctl_write()` in a transaction and roll back — semantics match apiserver dry-run closely since admission doesn't exist here anyway. | **Easy** guard / **Medium** fix |
| 8 | `GenerateName` — no name generation exists; creates one object with empty name that all subsequent creates collide with | Silently stores `name = ""` | Generate the name client-side (apiserver algorithm: 5-char random alphanumeric suffix), retry on `AlreadyExists` up to a bounded attempt count. | **Easy** |
| 9 | Types without `Spec`/`Status` fields lose their payload: `ConfigMap.Data`, `Secret.Data`, RBAC types, and all of `unstructured.Unstructured` serialize as `{}` | Object stored, payload silently dropped | **Guard:** at write time, marshal the full object and error if fields outside `metadata`/`Spec`/`Status` carry data (Easy). **Full fix:** store the complete object JSON in one column (or add a `raw` column) and keep the spec/status split as generated columns or extraction at read time — touches schema, the stored procedure's no-op suppression comparisons, and the spec/status write-path split. | **Easy** guard / **Hard** fix |
| 10 | Delete preconditions ignored (`client.Preconditions{UID: ...}` — the guard against deleting a same-name recreation) | Deletes unconditionally | Compare UID/RV against the row inside the write transaction: extend `pgctl_write()` with optional expected-UID (atomic), or client-side read-then-write using the version check that already exists. | **Easy–Medium** |
| 11 | No leader election: `Elected()` closes immediately. `replicas: 2` means two active reconcilers; the DB is protected by 409s but external side effects (cloud API calls) are duplicated. | Silently multi-active | Postgres advisory locks (`pg_advisory_lock` on a key derived from the election ID) are a natural single-primary election with automatic release on connection loss; gate `Elected()` and runnable start on acquiring it. Document the intended alternative (bucket-scoped replicas) alongside. | **Medium** |
| 12 | No owner-reference garbage collection: `Owns()` watches work, but deleting a parent never cascades to children | Children leak silently | Intentional non-goal (DESIGN.md §2) — controllers own cleanup via finalizers. Options: keep documented stance (free); ship a reusable finalizer helper that deletes owned objects (Easy); or a background GC controller walking `ownerReferences` (Medium–Hard, re-introduces the graph traversal the design deliberately avoids). | **Documented** / **Easy** helper |
| 13 | No admission chain: no schema validation, no CEL rules, no defaulting (webhook or `default:` markers), no mutating/validating webhooks. Invalid objects are stored; fields expected to be defaulted arrive as zero values. | Writes succeed unvalidated and undefaulted | **Guard:** document prominently — controllers must not assume defaulting (free). **Partial fix:** accept optional per-GVK validate/default Go hooks in `Options`, run them in the client write path (Medium). **Full fix:** evaluate CRD OpenAPI structural schemas + CEL via kube-openapi/cel-go (Hard, and only worth it for multi-team deployments). | **Medium** hooks / **Hard** full |
| 14 | No periodic resync: `resyncPeriod` ignored, no `SyncPeriod`. Controllers relying on the ~10h re-queue to heal external drift never get re-poked. | Handlers never re-fired | Implement client-go semantics: per-informer timer replays the store through `OnUpdate(obj, obj)`. All state is already in `pgInformer.store`. | **Easy** |
| 15 | Event recorder is a no-op: `recorder.Event(...)` vanishes | Events dropped silently | Log-based recorder (emit through the manager's logger) is Easy and covers debugging. Storing `v1.Event` objects as resources is Medium and gives `kubectl`-like queryability if an API front-end exists. | **Easy** (logs) |
| 16 | `RemoveEventHandler` / `RemoveInformer` are no-ops | Handlers leak; informers can't be stopped | Track registrations and remove from the handler slice; wire informer stop to a per-informer context. | **Easy** |

## Tier 3 — Environmental differences

Consequences of not having an apiserver or a cluster at all. Mostly inherent;
the fix is transparency plus small ergonomic escapes.

| # | Problem | Today | How to fix | Difficulty |
| --- | --- | --- | --- | --- |
| 17 | There is no "rest of the cluster": can't watch Nodes/Pods/anything not written through this library; no Namespace objects (creating into a nonexistent namespace succeeds; namespace deletion cascades nothing) | Those GVKs simply appear empty | Inherent. For hybrid controllers, document the two-manager pattern: a pgruntime manager for owned GVKs plus a standard manager against a real cluster, sharing reconcilers. | **Documented** |
| 18 | `GetConfig()` / `GetHTTPClient()` return nil — controllers building an extra clientset from `mgr.GetConfig()` nil-panic | nil at startup | Accept an optional `rest.Config` passthrough in `Options` for hybrid deployments; otherwise return a descriptive error from a wrapper rather than nil. | **Easy** |
| 19 | Single stored version, no conversion: multi-version CRDs with conversion webhooks can't work (`GetConverterRegistry` is a stub) | Only the written version round-trips | Scheme-registered Go conversion functions could convert at read time (Medium); full webhook-equivalent conversion with storage-version migration is Hard. Reasonable stance: one storage version per deployment, documented. | **Documented** / **Medium+** |
| 20 | Everything is namespace-scoped: `buildRESTMapper` registers all kinds as namespaced; `IsObjectNamespaced` lies for cluster-scoped types | Functionally works (ns `""`), reports wrong scope | Accept scope information in `Options` (or detect via a marker interface) and register `RESTScopeRoot` accordingly. | **Easy** |
| 21 | List pagination ignored: `Limit`/`Continue` return the full result set | Full set returned | Honoring `Limit` is trivial; a correct `Continue` token needs a stable cursor (`bucket_id, gvk_bucket_seq`) and a consistent snapshot across pages — the composite RV already encodes the snapshot position. | **Easy** (Limit) / **Medium** (Continue) |
| 22 | Manager serves no `/metrics`: `pgctl_*` metrics exist but controller-runtime's registry is never exposed | No metrics endpoint | Add `MetricsBindAddress` to `Options` and serve `metrics.Registry` alongside the existing health probe server. | **Easy** |

## Recommended sequencing

1. **Fail-loud guards first** (#6, #7, #8-adjacent rejection, #9, #10) — a few
   days of work total, and they convert every silent-corruption scenario in
   Tier 2 into an immediate, debuggable error. After this, "it runs" means
   "it's on the supported surface."
2. **Easy wins with real migration value:** `GenerateName` (#8), `DeleteAllOf`
   (#3), resync (#14), log-based events (#15), metrics endpoint (#22), RESTMapper
   scope (#20), `Limit` (#21).
3. **Patch (#1) and field-selector evaluation (#6 full fix)** — these two
   unlock the majority of real-world controllers that are currently blocked.
4. **Leader election (#11)** before anyone runs multi-replica deployments.
5. **Decide and document the permanent non-goals** (SSA, admission, GC,
   conversion, rest-of-cluster) so expectations are set rather than discovered.
