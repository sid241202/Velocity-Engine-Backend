# Backend — uid-dp-velocity-engine-control-plane-backend

Go/Gin API + WebSocket server for the Velocity Engine. See `../CLAUDE.md`
(also auto-loaded) for cross-repo rules, the session-continuity protocol,
and the autonomy/permission scope — this file only covers what's specific
to this repo.

## Stack

- Gin framework, `database/sql`.
- ClickHouse via `clickhouse-go/v2` (aggregated/live results + anomalies).
- DuckDB via `marcboeker/go-duckdb` (cgo) + `iceberg_scan()` over S3 Parquet
  for ad hoc historical replay — prefer `iceberg_scan()` over a raw
  `read_parquet()` glob (manifest-level pruning, snapshot isolation,
  delete-file handling).
- MySQL via `go-sql-driver/mysql`, **pinned to v1.9.3** — v1.10.0+ requires
  Go ≥1.24, which would silently bump `go.mod`'s `go` directive and break
  the Dockerfile's pinned `golang:1.23.3` base image. Keep `go.mod`'s `go`
  directive at `1.23`; if `go get`/`go mod tidy` ever bumps it, that's a
  regression to revert, not something to accept.

## Config

All configuration lives in `internal/config/config.go` — Kafka, ClickHouse,
Iceberg/S3, LiveStore, MySQL, RBAC cache tuning, `AuthDevMode`. Don't add
another config file — extend this one. `config.go`'s `init()` panics if
`AuthDevMode` (or a missing S3 key) would be live with `ENV=prod` — that
guard is intentional, don't relax it.

## RBAC (backend half)

- `internal/services/mysql.go` — `VerifyMySQLSchema` checks the RBAC tables
  exist via `information_schema.tables`. **Never** creates/alters/seeds
  them — the user provisions MySQL manually per environment (see
  `../CLAUDE.md` cross-repo contracts). `internal/migrations/mysql/0001_init_rbac.sql`
  is the DDL/seed reference to hand the user or run by hand with a mysql
  client; nothing in this codebase executes it automatically.
- `internal/services/rbac.go` — `GetUserPermissions` resolves + caches a
  user's roles/permissions (TTL cache with a stale-serving fail-tolerant
  window, then fail-closed). `internal/middleware/auth.go` —
  `IdentityMiddleware` (temporary `X-Debug-User-Id` pre-WSO2 shim, disabled
  outside `AuthDevMode`) and `RequirePermission(resource, action)`.
  `internal/handlers/iam.go` — `GET /me`.
- Permission keys are `resource:action` strings (e.g. `rules:publish`) —
  must match `frontend/src/permissions.js` exactly.

## Branches

- `release` — stable/demo branch.
- `rbac` — current RBAC work, cut from `release`, **not yet merged back**
  (explicitly deferred by the user).

## Local build/test — known toolchain limitation

`go build`/`go test` on this machine hits a **pre-existing, unrelated**
MinGW/libduckdb ABI mismatch at the final link step for any binary that
pulls in the DuckDB cgo dependency (`cmd/server`, and any test binary for a
package that imports `internal/services`) — the vendored `libduckdb.a` was
built against a different libstdc++ than the local MSYS2 gcc provides
(`undefined reference to std::basic_streambuf<...>::seekpos(...)`). This is
not something introduced by any change made so far — don't attempt to "fix"
it as a side effect of an unrelated task without being asked.

**Verification pattern that works around it** (used successfully throughout
this engagement):
1. `go vet ./...` and `go build` on **non-cgo-dependent** packages (e.g.
   `internal/middleware`) — these fully build, link, and run.
2. `go vet ./...` on the whole module — this succeeds even for the
   `internal/services`/`cmd/server` packages, because `vet` type-checks
   without linking. Use this to prove compile-correctness of cgo-touching
   changes.
3. `go build ./cmd/server/...` reaches the link step and fails there — that
   specific failure is expected/pre-existing; a successful compile-then-fail-
   at-link is the expected signal that the Go code itself is fine.
4. For real `go test` execution of logic inside `internal/services` (which
   can't fully build+link locally), copy the pure-Go logic under test into
   an isolated, cgo-free scratchpad Go module and `go test` it there. This
   has been done successfully for LiveStore and RBAC cache logic.
5. A system-wide Go toolchain isn't on `PATH` in this environment. A
   portable Go 1.23.3 was downloaded once and is kept at
   `E:\Projects\.claude\tools\go1.23.3` (durable — survives across
   sessions, unlike the per-session scratchpad it was originally extracted
   to). Use it like:
   ```bash
   export GOROOT="/e/Projects/.claude/tools/go1.23.3"
   export PATH="$GOROOT/bin:$PATH"
   export GOCACHE="/c/Users/siddh/AppData/Local/Temp/claude/<session-scratchpad>/gocache"   # any writable scratch dir; rebuilds automatically
   export GOPATH="/c/Users/siddh/AppData/Local/Temp/claude/<session-scratchpad>/gopath"       # modules re-download automatically if missing
   go vet ./...
   ```
   MSYS2 gcc (needed for the cgo link step, even though it currently fails
   for DuckDB-linked binaries) is a system install at
   `C:\msys64\mingw64\bin\gcc.exe` — already on `PATH` normally, no action
   needed for that part.
