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
  2026-07-20 — see RBAC section below). `release` still uses MySQL; don't
  assume this branch matches it.

## Config

All configuration lives in `internal/config/config.go` — Kafka, ClickHouse,
Iceberg/S3, LiveStore, RBAC cache tuning, `AuthDevMode`. Don't add another
config file — extend this one. `config.go`'s `init()` panics if
`AuthDevMode` (or a missing S3 key) would be live with `ENV=prod` — that
guard is intentional, don't relax it. No MySQL config keys exist here
anymore (`MySQLHost`/`Port`/`User`/`Password`/`Database`/`MaxOpenConns`/
`MaxIdleConns` all removed alongside the driver).

## RBAC and rule storage (backend half) — in-memory, zero MySQL

As of 2026-07-20, this branch runs with **zero MySQL tables provisioned** —
both the RBAC store and the rule store were converted from MySQL-backed to
purely in-memory. This was a storage-layer swap only: the X-Debug-User-Id
identity flow, the resource:action permission model, and rule CRUD
semantics are all unchanged from before. `release` (a separately-diverged
branch) still uses MySQL for both — don't assume parity.

- `internal/services/rbac.go` — `rolePermissions` (the four roles × ten
  permissions grant matrix) and `demoUsers` (one fixed seeded user per
  role, ids 1–4) replace the old `users`/`roles`/`permissions`/`user_roles`/
  `role_permissions` MySQL tables and `0001_init_rbac.sql`. Both are
  read-only after package init (no admin endpoints mutate them), so neither
  needs a mutex — unlike `permCache` in the same file, which is genuinely
  request-concurrent and stays mutex-guarded. `GetUserPermissions`'s public
  behavior (TTL cache, stale-serving fail-tolerant window, then fail-closed)
  is unchanged; only the underlying fetch (`fetchUserPermissionsFromMemory`,
  was `fetchUserPermissionsFromDB`) changed. Permission keys are still
  `resource:action` strings (e.g. `rules:publish`) and must still match
  `frontend/src/permissions.js` exactly.
- `internal/middleware/auth.go` — `IdentityMiddleware` (`X-Debug-User-Id`
  pre-WSO2 shim, disabled outside `AuthDevMode`) and
  `RequirePermission(resource, action)` — unchanged by the storage swap.
  `internal/handlers/iam.go` — `GET /me`.
- `internal/handlers/rules.go` — `RulesHandler.rulesDB` (a
  `map[string]*models.RuleRecord` guarded by `RulesHandler.mu
  sync.RWMutex`) is now the **sole** rule store, not just a request-serving
  cache in front of MySQL. There is no persistence layer behind it and no
  reload-on-restart — **demo does not survive a process restart**, which is
  an accepted, correct design here (not a shortcut): rulesDB starts empty
  every time the process starts. `internal/services/rule_store.go`
  (`SaveNewRuleVersion`/`UpdateRuleStatusInPlace`/`LoadActiveRules`),
  `internal/services/mysql.go`, `internal/migrations/mysql/*.sql`, and the
  now-fully-unused `internal/store` package (rule name/description
  generation, only ever called from the removed rule store) were all
  deleted outright rather than kept dead.
- Test coverage: `internal/services/rbac_memory_test.go` replaces
  `rbac_mysql_integration_test.go` (which required a real MySQL instance
  behind a `mysql_integration` build tag) — covers the same guarantees
  (seed-data shape, per-role grants, end-to-end permission resolution)
  against the in-memory store, runs unconditionally under `go test ./...`.

## Branches

- `release` — stable/demo branch, MySQL-backed RBAC + rule store, real
  WSO2/OIDC auth. Deliberately diverged from `demo` — verify `release`'s
  own `CLAUDE.md` and code directly rather than assuming parity with this
  file.
- `demo` (this branch) — in-memory RBAC + rule store (see above), still
  uses the pre-WSO2 `X-Debug-User-Id` identity shim (not WSO2 — that
  migration is explicitly out of scope for this branch, needs separate
  sign-off). Not merged into `release`.

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
