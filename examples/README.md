# Migrating a controller-runtime controller to postgres-controller-backend

This directory contains two implementations of the same controller — one using
controller-runtime against etcd, and one using `crbridge` against PostgreSQL.
Both manage three CRDs that exercise the three common controller patterns:

| CR | Pattern | Description |
|---|---|---|
| `Greeting` | Own spec + status | User sets `spec.name`, controller computes `status.message` |
| `GreetingCard` | Own spec (child) | Controller creates as a child of Greeting |
| `GreetingPolicy` | Watch (external) | `spec.prefix` affects message; changes trigger re-reconciliation |

## What changes, what stays the same

The reconcile logic is identical between the two controllers. The differences
are in how you talk to the storage layer and how you wire up watches. Here is
the full list of things that change.

### 1. Types: `metav1.ObjectMeta` boilerplate → plain Go structs

**etcd** — typed Go structs with `metav1.ObjectMeta`, `DeepCopyObject()`,
scheme registration, and code generation boilerplate (~193 lines for three
types):

```go
type Greeting struct {
    metav1.TypeMeta   `json:",inline"`
    metav1.ObjectMeta `json:"metadata,omitempty"`
    Spec              GreetingSpec   `json:"spec,omitempty"`
    Status            GreetingStatus `json:"status,omitempty"`
}
// + DeepCopyObject, DeepCopyInto, SchemeBuilder.Register, ...
```

**postgres** — plain Go structs for spec and status, no metadata boilerplate,
no deepcopy, no scheme registration (~32 lines for three types):

```go
type GreetingSpec struct {
    Name string `json:"name"`
}

type GreetingStatus struct {
    Message string `json:"message,omitempty"`
    Phase   string `json:"phase,omitempty"`
    CardRef string `json:"cardRef,omitempty"`
}
```

The `crbridge.TypedObject[S, T]` generic type carries the standard metadata
(namespace, name, UID, resourceVersion) alongside your typed spec and status:

```go
type TypedObject[S any, T any] struct {
    GVK             string
    Namespace       string
    Name            string
    UID             uuid.UUID
    ResourceVersion string
    Spec            S              // your spec type
    Status          T              // your status type
    // ...
}
```

### 2. CRUD: `client.Client` → `crbridge.TypedClient[S, T]`

| Operation | controller-runtime | crbridge |
|---|---|---|
| Get | `r.Get(ctx, key, &obj)` | `client.Get(ctx, ns, name)` |
| Create | `r.Create(ctx, &obj)` | `client.Create(ctx, ns, name, spec)` |
| Update spec | `r.Update(ctx, &obj)` | `client.Update(ctx, obj)` |
| Update status | `r.Status().Update(ctx, &obj)` | `client.Status().Update(ctx, obj, status)` |
| Delete | `r.Delete(ctx, &obj)` | `client.Delete(ctx, obj)` |
| List | `r.List(ctx, &list, opts...)` | `client.List(ctx)` |

Key differences:
- `crbridge.TypedClient` is **per-GVK** (one client per kind). controller-runtime
  uses a single `client.Client` for all types.
- Create takes the typed spec directly — status defaults to the zero value.
- Status updates take the typed object + new typed status value.
- There is no `CreateOrUpdate` helper — you Get, check `ErrNotFound`, and
  branch into Create or Update yourself.
- All operations return `*TypedObject[S, T]` with typed fields — no
  `json.Unmarshal` needed in controller code.

An untyped `crbridge.Client` is also available for code that works with raw
JSON (e.g., HTTP APIs). Access it via `typedClient.Untyped()`.

### 3. Error handling: `errors.IsNotFound()` → sentinel errors

| Condition | controller-runtime | crbridge |
|---|---|---|
| Not found | `errors.IsNotFound(err)` | `err == crbridge.ErrNotFound` |
| Already exists | `errors.IsAlreadyExists(err)` | `err == crbridge.ErrAlreadyExists` |
| Conflict | `errors.IsConflict(err)` | `err == crbridge.ErrConflict` |
| Fenced | N/A | `err == crbridge.ErrFenced` |

`ErrFenced` is new — it means the lease epoch doesn't match, typically because
another replica took over. Treat it as a signal to stop processing.

### 4. Watches: `SetupWithManager` → `NewControllerFor`

**etcd** — declarative, one-liner watch setup:

```go
ctrl.NewControllerManagedBy(mgr).
    For(&Greeting{}).
    Owns(&GreetingCard{}).
    Watches(&GreetingPolicy{}, handler.EnqueueRequestsFromMapFunc(mapFn)).
    Complete(r)
```

**postgres** — same declarative pattern via `crbridge.NewControllerFor`:

```go
crbridge.NewControllerFor[GreetingSpec, GreetingStatus](mgr, gvkGreeting, reconciler).
    Watches(gvkGreetingPolicy, reconciler.policyToGreetings).
    Complete()
```

The `Manager` handles all the list-watch-relist loops, work queue, and
reconcile dispatch internally. You only write:
- A `Reconcile(ctx, *TypedObject[S, T]) (Result, error)` method
- A `MapFunc` for cross-type watches (e.g., policy change → requeue greetings)

The `MapFunc` receives an untyped `*Object` because it operates at the watch
level. In the common case (requeue by namespace) only `obj.Namespace` is needed:

```go
func (r *GreetingReconciler) policyToGreetings(ctx context.Context, obj *crbridge.Object) []crbridge.Request {
    result, _ := r.Greetings.List(ctx)
    var requests []crbridge.Request
    for _, g := range result.Objects {
        if !g.Deleted && g.Namespace == obj.Namespace {
            requests = append(requests, crbridge.Request{Namespace: g.Namespace, Name: g.Name})
        }
    }
    return requests
}
```

### 5. Startup: `ctrl.NewManager` → `crbridge.NewManager`

**etcd** — 3 lines:

```go
mgr, _ := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{Scheme: scheme})
(&GreetingReconciler{Client: mgr.GetClient()}).SetupWithManager(mgr)
mgr.Start(ctrl.SetupSignalHandler())
```

**postgres** — connect, migrate, acquire leases, then use the Manager:

```go
// 1. Connect to postgres (with retry loop)
conn, _ := pgx.Connect(ctx, dsn)

// 2. Migrate schema
schema.Migrate(ctx, conn)

// 3. Acquire leases
leaseMgr := lease.NewBothManager(leaseConn, holderID)
epochs, _ := leaseMgr.AcquireBoth(ctx, bucketID, leaseTTL)

// 4. Create typed clients
greetingClient := crbridge.NewTypedClient[GreetingSpec, GreetingStatus](
    crbridge.NewClient(connFactory, gvk, assigner, holderID, epochs.Spec),
    crbridge.NewListerWatcher(connFactory, gvk, buckets),
)

// 5. Create Manager, register controller, start
mgr := crbridge.NewManager(crbridge.ManagerConfig{...})
crbridge.NewControllerFor[GreetingSpec, GreetingStatus](mgr, gvk, reconciler).
    Watches(gvkGreetingPolicy, reconciler.policyToGreetings).
    Complete()
mgr.Start(ctx)

// 6. Start lease renewal ticker (every ~10s)
```

New concepts with no etcd equivalent:
- **Lease acquisition** — you call `AcquireBoth` on startup to get an epoch
  for write fencing. Don't release leases on shutdown (let the TTL expire).
- **Lease renewal** — a background ticker calls `RenewBoth` to keep the lease
  alive. If it lapses, writes are fenced.
- **Schema migration** — `schema.Migrate()` creates the postgres tables.
  Idempotent, safe to call on every startup.
- **Connection factory** — `crbridge.TypedClient` and `ListerWatcher` take a
  `func() (*pgx.Conn, error)` rather than a single connection, so each
  operation gets its own connection.
- **Bucket assignment** — a `func(namespace, name string) int` that maps
  objects to buckets. For a single-replica controller, return a constant.

### 6. Validation: apiserver does it → you do it

With etcd, the apiserver validates payloads against the CRD schema before
writing. With postgres, there is no apiserver — you must validate yourself.

The example uses the exact same validator the apiserver uses:

```go
// Embed CRD YAMLs, parse the OpenAPI schema, build a structural validator
validator, _ := NewValidator()

// Before every write:
if err := validator.ValidateSpec(gvk, specJSON); err != nil {
    return 422, err  // "spec.name: Invalid value: ..."
}
```

This is ~130 lines in `validator.go`. It uses `k8s.io/apiextensions-apiserver`
to parse CRD YAML into a structural schema and validate against it. The error
messages are identical to what the apiserver produces.

### 7. API surface: kubectl → HTTP (or your own)

With etcd, clients use kubectl or client-go. With postgres, there is no
apiserver serving your CRDs. You provide your own API. The example uses a
simple HTTP server (`httpapi.go`, ~240 lines) with REST-like routes:

```
POST   /namespaces/{ns}/greetings          → Create
GET    /namespaces/{ns}/greetings/{name}   → Get
GET    /namespaces/{ns}/greetings          → List
PUT    /namespaces/{ns}/greetings/{name}   → Update
```

This is optional — your controller's reconcile loop only needs
`crbridge.TypedClient` and the `Manager`. The HTTP API is for external
consumers who would otherwise use kubectl.

## Line count comparison

| | etcd-controller | postgres-controller |
|---|---|---|
| types / deepcopy / scheme | 193 | 32 (plain structs) |
| controller + reconcile | 108 | 93 |
| main / bootstrap | 32 | 168 |
| validator | 0 (apiserver does it) | 129 |
| HTTP API | 0 (apiserver does it) | 242 |
| **Total** | **333** | **664** |

The reconcile function is now roughly the same size as the etcd version. The
remaining delta is:
- Bootstrap / lease management (~136 lines)
- CRD validation (~129 lines)
- HTTP API for external access (~242 lines, optional)

Without the optional HTTP API, the delta is ~422 lines — and most of that is
validation and lease management, not controller logic.

## Migration checklist

1. **Replace type boilerplate with plain structs** — delete deepcopy methods,
   scheme registration, `metav1.ObjectMeta` embedding. Define simple Go structs
   for spec and status fields.

2. **Replace `client.Client` with `crbridge.TypedClient[S, T]`** — one typed
   client per GVK. Update all CRUD calls to the new signatures (see table
   above). Access `.Spec` and `.Status` directly on the returned
   `TypedObject` — no `json.Unmarshal` needed.

3. **Replace error checks** — `errors.IsNotFound(err)` → `err == crbridge.ErrNotFound`,
   etc. Add handling for `ErrFenced`.

4. **Replace `SetupWithManager` with `NewControllerFor`** — implement the
   `Reconciler[S, T]` interface, use `Watches()` for cross-type triggers,
   call `Complete()` to register with the Manager. The Manager handles
   list-watch-relist loops and the work queue internally.

5. **Add bootstrap code** — connect to postgres, migrate schema, acquire leases,
   create typed clients, create a `Manager`, start lease renewal ticker.

6. **Add CRD validation** — embed CRD YAMLs, build a `Validator`, call
   `ValidateSpec`/`ValidateStatus` before every write in the HTTP API layer.

7. **Add an HTTP API** (if needed) — if external consumers need to read or
   write your CRs, expose an HTTP server that wraps `crbridge.Client` (use
   `typedClient.Untyped()` to get the raw client).

8. **Update deployment manifest** — remove RBAC (ServiceAccount, ClusterRole,
   ClusterRoleBinding). Add postgres connection env vars.
