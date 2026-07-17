# Migrating a controller-runtime controller from etcd to PostgreSQL

This directory contains two implementations of the same controller — one using
controller-runtime against etcd, and one using `pgruntime` against PostgreSQL.
Both manage three CRDs that exercise the three common controller patterns:

| CR               | Pattern           | Description                                                      |
| ---------------- | ----------------- | ---------------------------------------------------------------- |
| `Greeting`       | Own spec + status | User sets `spec.name`, controller computes `status.message`      |
| `GreetingCard`   | Own spec (child)  | Controller creates as a child of Greeting                        |
| `GreetingPolicy` | Watch (external)  | `spec.prefix` affects message; changes trigger re-reconciliation |

## What changes, what stays the same

`pgruntime.NewManager` returns a standard `manager.Manager` backed by
PostgreSQL instead of etcd. Types, `client.Client` CRUD, error handling, and
`SetupWithManager` watch setup are all unchanged — your reconciler code works
as-is. The differences are in startup bootstrapping and the absence of an
apiserver.

### 1. Startup: `ctrl.NewManager` → `pgruntime.NewManager`

**etcd**:

```go
mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
    Scheme: greeting.Scheme,
})
```

**postgres**:

```go
mgr, err := pgruntime.NewManager(pgruntime.Options{
    Scheme: greeting.Scheme,
    DSN:    dsn,
})
```

Everything after the manager creation is identical — `SetupWithManager`,
`mgr.Start`, error handling all stay the same.

New concepts with no etcd equivalent:

- **Schema migration** — handled automatically by `NewManager` on startup.
  Idempotent, safe to call on every startup.
- **PostgreSQL connection pooling** — `NewManager` creates a `pgxpool.Pool`
  internally to manage connections to PostgreSQL. Every CRUD call and every
  informer poll acquires a connection from this pool, so pool size bounds how
  many concurrent database operations can run. Configure with `MaxPoolConns`
  and `MinPoolConns` in `Options` (defaults come from pgxpool: max 4, min 0).

### 2. Validation: apiserver does it → you do it

With etcd, the apiserver validates payloads against the CRD schema before
writing. With postgres, there is no apiserver — if you expose an HTTP API for
creating resources, you must validate payloads yourself. One approach is to
embed CRD YAMLs and use `k8s.io/apiextensions-apiserver` to parse the OpenAPI
schema and validate against it.

### 3. API surface: kubectl → HTTP (or your own)

With etcd, clients use kubectl or client-go. With postgres, there is no
apiserver serving your CRDs. If external consumers need to read or write your
CRs, you provide your own API (e.g., an HTTP server with REST-like routes).

This is optional — your controller's reconcile loop only needs the `Manager`
and `client.Client`. The HTTP API is for external consumers who would
otherwise use kubectl.

## Migration checklist

1. **Replace bootstrap code** — swap `ctrl.NewManager` for
   `pgruntime.NewManager` with your DSN and scheme. Connection pooling and
   schema migration are handled automatically. Types, `client.Client`,
   error handling, and `SetupWithManager` all work unchanged.

2. **Add CRD validation** (if exposing an HTTP API) — embed CRD YAMLs and
   validate payloads before writes since there is no apiserver to do it.

3. **Add an HTTP API** (if needed) — if external consumers need to read or
   write your CRs, expose an HTTP server that wraps `mgr.GetClient()`.

4. **Update deployment manifest** — remove RBAC (ServiceAccount, ClusterRole,
   ClusterRoleBinding). Add postgres connection env vars.
