# Backend — uid-dp-velocity-engine-control-plane-backend

Go/Gin API + WebSocket server for the Velocity Engine. See `../CLAUDE.md`
(also auto-loaded) for cross-repo rules, the session-continuity protocol,
and the autonomy/permission scope — this file only covers what's specific
to this repo.

## Stack

- Gin framework.
- ClickHouse via `clickhouse-go/v2` (aggregated/live results + anomalies).
- DuckDB via `marcboeker/go-duckdb` (cgo) + `iceberg_scan()` over S3 Parquet
  for ad hoc historical replay — prefer `iceberg_scan()` over a raw
  `read_parquet()` glob (manifest-level pruning, snapshot isolation,
  delete-file handling).
- **No MySQL, no `database/sql` driver, on this branch** (removed
  2026-07-20). `release` still uses MySQL; don't assume this branch matches
  it.
- **No auth/identity/RBAC layer of any kind, on this branch** (removed
  2026-07-20 — see below). Every request is treated as a single implicit
  user. `release` has real WSO2/OIDC auth + RBAC; don't assume this branch
  matches it, and don't port anything from it here without being asked.

## Config

All configuration lives in `internal/config/config.go` — Kafka, ClickHouse,
Iceberg/S3, LiveStore. Don't add another config file — extend this one. No
MySQL config keys exist here (`MySQLHost`/`Port`/`User`/`Password`/
`Database`/`MaxOpenConns`/`MaxIdleConns`), and no RBAC/auth config keys
either (`RBACPermissionCacheTTLSeconds`/`RBACPermissionMaxStaleSeconds`/
`AuthDevMode` all removed alongside the auth layer — see below).

## No auth/RBAC — demo is a fully open, single-implicit-user application

As of 2026-07-20, this branch has **no auth layer at all** — not a
lighter-weight identity, not the previous `X-Debug-User-Id` debug shim with
checks removed, but no concept of "who is asking" anywhere in the code.
Every route is reachable with no headers, no token, nothing. This is a
deliberate decision for this branch, not an oversight: `release` (a
separately-diverged branch) has real WSO2/OIDC auth + RBAC — don't assume
parity, and don't resurrect identity resolution here without being asked.

Deleted entirely (not kept as no-ops, so the code path doesn't exist to be
misread later): `internal/middleware/auth.go` + `auth_test.go`
(`IdentityMiddleware`, `RequirePermission`, `AuthMiddleware`),
`internal/services/rbac.go` + its tests (`GetUserPermissions`, the seeded
`demoUsers`/`rolePermissions` in-memory store this branch briefly had
between 2026-07-20's two changes today), `internal/handlers/iam.go` (`GET
/me`), and `internal/models/rbac.go` (`User`/`Role`/`Permission`/
`AuditLogEntry`/`MeResponse` — already-orphaned MySQL-table-mirroring
structs once `GET /me` was removed). `cmd/server/main.go` no longer
constructs an `AuthMiddleware` or `IAMHandler`, and no route anywhere calls
`IdentityMiddleware()` or `RequirePermission(...)`.

No route's business logic actually depended on "which user" beyond the
auth check itself (`internal/models/rule.go`'s `RuleRecord` has no
created-by/updated-by field) — so there was nothing to backfill with a
fixed "demo-user" constant.

`internal/handlers/rules.go` — `RulesHandler.rulesDB` (a
`map[string]*models.RuleRecord` guarded by `RulesHandler.mu
sync.RWMutex`) is unaffected by this change and remains the sole rule
store: purely in-memory, no persistence layer, no reload-on-restart.
**Demo does not survive a process restart** — an accepted, correct design
here, not a shortcut.

## Branches

- `release` — stable/demo branch, MySQL-backed RBAC + rule store, real
  WSO2/OIDC auth. Deliberately diverged from `demo` — verify `release`'s
  own `CLAUDE.md` and code directly rather than assuming parity with this
  file.
- `demo` (this branch) — in-memory rule store, no auth/RBAC/identity layer
  at all (see above). Not merged into `release`.

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
   `internal/config`, `internal/handlers`) — these fully build, link, and run.
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
   has been done successfully for LiveStore.
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
6. **Run `go vet`/`go build`/`go test` for cgo-touching packages via the
   PowerShell tool, not the Bash tool.** Confirmed 2026-07-20: invoking
   `gcc.exe` (and therefore `cgo.exe`, which shells out to it) from this
   environment's Bash tool silently fails — exit code 1/2, no stdout, no
   stderr, even for a trivial one-line `.c` file compiled directly —
   regardless of `dangerouslyDisableSandbox`. The identical command run via
   the PowerShell tool works immediately. Pure-Go packages (no cgo)
   vet/build fine from either tool; this only bites packages that actually
   invoke a C compiler (i.e. anything pulling in `internal/services`/
   `cmd/server`).
