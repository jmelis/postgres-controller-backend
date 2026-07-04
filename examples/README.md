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

### 1. Types: Go structs → `json.RawMessage`

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

**postgres** — `crbridge.Object` uses `json.RawMessage` for spec, status, and
metadata. No Go types needed, no deepcopy, no scheme:

```go
type Object struct {
    GVK             string
    Namespace       string
    Name            string
    UID             uuid.UUID
    ResourceVersion string
    BucketID        int
    Spec            json.RawMessage
    Status          json.RawMessage
    Metadata        json.RawMessage
    Deleted         bool
}
```

You parse fields inline when you need them:

```go
var spec struct { Name string `json:"name"` }
json.Unmarshal(greeting.Spec, &spec)
```

### 2. CRUD: `client.Client` → `crbridge.Client`

| Operation | controller-runtime | crbridge |
|---|---|---|
| Get | `r.Get(ctx, key, &obj)` | `client.Get(ctx, ns, name)` |
| Create | `r.Create(ctx, &obj)` | `client.Create(ctx, ns, name, spec, status, metadata)` |
| Update spec | `r.Update(ctx, &obj)` | `client.Update(ctx, obj)` |
| Update status | `r.Status().Update(ctx, &obj)` | `client.Status().Update(obj, statusJSON)` |
| Delete | `r.Delete(ctx, &obj)` | `client.Delete(ctx, obj)` |
| List | `r.List(ctx, &list, opts...)` | `lw.List(ctx)` |

Key differences:
- `crbridge.Client` is **per-GVK** (one client per kind). controller-runtime
  uses a single `client.Client` for all types.
- Create takes the spec, status, and metadata as separate `json.RawMessage`
  arguments instead of a full typed object.
- Status updates take the object + new status JSON instead of mutating a struct.
- There is no `CreateOrUpdate` helper — you Get, check `ErrNotFound`, and
  branch into Create or Update yourself.

### 3. Error handling: `errors.IsNotFound()` → sentinel errors

| Condition | controller-runtime | crbridge |
|---|---|---|
| Not found | `errors.IsNotFound(err)` | `err == crbridge.ErrNotFound` |
| Already exists | `errors.IsAlreadyExists(err)` | `err == crbridge.ErrAlreadyExists` |
| Conflict | `errors.IsConflict(err)` | `err == crbridge.ErrConflict` |
| Fenced | N/A | `err == crbridge.ErrFenced` |

`ErrFenced` is new — it means the lease epoch doesn't match, typically because
another replica took over. Treat it as a signal to stop processing.

### 4. Watches: `SetupWithManager` → explicit watch loops

**etcd** — declarative, one-liner watch setup:

```go
ctrl.NewControllerManagedBy(mgr).
    For(&Greeting{}).
    Owns(&GreetingCard{}).
    Watches(&GreetingPolicy{}, handler.EnqueueRequestsFromMapFunc(mapFn)).
    Complete(r)
```

**postgres** — you write the List/Watch loop and work queue yourself:

```go
func (c *Controller) watchGreetings(ctx context.Context) {
    for ctx.Err() == nil {
        result, _ := c.greetingLW.List(ctx)
        for _, obj := range result.Objects {
            c.enqueue(obj.Namespace, obj.Name)
        }
        wi, _ := c.greetingLW.Watch(ctx, result.ResourceVersion)
        for ev := range wi.ResultChan() {
            c.enqueue(ev.Object.Namespace, ev.Object.Name)
        }
        // channel closed → relist
    }
}
```

Each GVK you want to watch gets its own `ListerWatcher` and its own goroutine.
There is no built-in `Owns()` or `EnqueueRequestsFromMapFunc()` — you implement
the mapping logic in your watch handler:

```go
// GreetingPolicy changed → requeue all Greetings in that namespace
func (c *Controller) watchPolicies(ctx context.Context) {
    // ...
    for ev := range wi.ResultChan() {
        c.requeueAllGreetings(ctx, ev.Object.Namespace)
    }
}
```

The work queue is a plain `chan string`. You push `"namespace/name"` keys to
it, and a reconcile loop drains it.

### 5. Startup: `ctrl.NewManager` → manual bootstrap

**etcd** — 3 lines:

```go
mgr, _ := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{Scheme: scheme})
(&GreetingReconciler{Client: mgr.GetClient()}).SetupWithManager(mgr)
mgr.Start(ctrl.SetupSignalHandler())
```

**postgres** — you manage the lifecycle yourself:

```go
// 1. Connect to postgres (with retry loop)
conn, _ := pgx.Connect(ctx, dsn)

// 2. Migrate schema
schema.Migrate(ctx, conn)

// 3. Acquire leases
mgr := lease.NewBothManager(leaseConn, holderID)
epochs, _ := mgr.AcquireBoth(ctx, bucketID, leaseTTL)

// 4. Create per-GVK clients and lister-watchers
client := crbridge.NewClient(connFactory, gvk, assigner, holderID, epoch)
lw := crbridge.NewListerWatcher(connFactory, gvk, bucketIDs)

// 5. Start lease renewal ticker (every ~10s)
// 6. Start watch goroutines
// 7. Start reconcile worker
```

New concepts with no etcd equivalent:
- **Lease acquisition** — you call `AcquireBoth` on startup to get an epoch
  for write fencing. Don't release leases on shutdown (let the TTL expire).
- **Lease renewal** — a background ticker calls `RenewBoth` to keep the lease
  alive. If it lapses, writes are fenced.
- **Schema migration** — `schema.Migrate()` creates the postgres tables.
  Idempotent, safe to call on every startup.
- **Connection factory** — `crbridge.Client` and `ListerWatcher` take a
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

This is optional — your controller's reconcile loop only needs `crbridge.Client`
and `ListerWatcher`. The HTTP API is for external consumers who would otherwise
use kubectl.

## Line count comparison

| | etcd-controller | postgres-controller |
|---|---|---|
| types / deepcopy / scheme | 193 | 0 (uses `json.RawMessage`) |
| controller + reconcile | 108 | 265 |
| main / bootstrap | 32 | 176 |
| validator | 0 (apiserver does it) | 129 |
| HTTP API | 0 (apiserver does it) | 242 |
| **Total** | **333** | **812** |

The reconcile function itself is roughly the same size. The additional ~480
lines in the postgres controller are:
- Bootstrap / lease management (~144 lines)
- Watch loops + work queue that controller-runtime gives you for free (~157 lines)
- CRD validation (~129 lines)
- HTTP API for external access (~242 lines, optional)

If you don't need an external HTTP API (your controller is the only consumer),
the delta drops to ~570 lines.

## Migration checklist

1. **Remove type boilerplate** — delete Go struct types, deepcopy methods,
   scheme registration. Use `json.RawMessage` and parse fields inline.

2. **Replace `client.Client` with `crbridge.Client`** — one client per GVK.
   Update all CRUD calls to the new signatures (see table above).

3. **Replace error checks** — `errors.IsNotFound(err)` → `err == crbridge.ErrNotFound`,
   etc. Add handling for `ErrFenced`.

4. **Replace `SetupWithManager` with watch loops** — write a goroutine per
   watched GVK that calls `List()` then `Watch()` in a loop, pushing keys to
   a work queue channel. Implement your own mapping logic for `Owns()`
   and cross-type triggers.

5. **Add bootstrap code** — connect to postgres, migrate schema, acquire leases,
   create clients and lister-watchers, start lease renewal ticker.

6. **Add CRD validation** — embed CRD YAMLs, build a `Validator`, call
   `ValidateSpec`/`ValidateStatus` before every write.

7. **Add an HTTP API** (if needed) — if external consumers need to read or
   write your CRs, expose an HTTP server that wraps `crbridge.Client`.

8. **Update deployment manifest** — remove RBAC (ServiceAccount, ClusterRole,
   ClusterRoleBinding). Add postgres connection env vars.
