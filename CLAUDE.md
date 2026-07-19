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
Iceberg/S3, LiveStore, MySQL, RBAC cache tuning, WSO2/OIDC. Don't add
another config file — extend this one. This branch (`release`) is
**WSO2-only** — there is no `AuthDevMode`/dev-identity toggle in the code at
all (removed as part of the WSO2 merge below). `config.go`'s `init()`
panics if `WSO2_AUDIENCE` is unset, and separately if the S3 key pair would
be live with `ENV=prod` — both guards are intentional, don't relax them.
(`demo` is a different, deliberately-diverged branch — check its own
`config.go`/`CLAUDE.md` directly rather than assuming it matches this.)

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
  `IdentityMiddleware` validates the `Authorization: Bearer` token against
  WSO2's JWKS (`internal/services/jwtvalidator.go`, real RS256 signature
  verification) and resolves the token's `sub` claim to a local user via
  `internal/services/rbac.go`'s `GetUserByExternalSubject` — no dev-mode
  identity shim exists on this branch. `RequirePermission(resource, action)`
  is the reusable per-route guard. `internal/handlers/iam.go` — `GET /me`.
- Permission keys are `resource:action` strings (e.g. `rules:publish`) —
  must match `frontend/src/permissions.js` exactly. Currently wired to
  routes: `rules:create` (`POST /rules`), `rules:update`
  (`PUT /rules/:rule_id`), `rules:publish` (`POST /rules/:rule_id/prod` and
  `POST /rules/:rule_id/status` — status-change is gated on the same key as
  publish because it can also set a rule to `ACTIVE` via the identical
  `services.PublishRule` path), `rules:delete` (`DELETE /rules/:rule_id`).
  The read-only routes (`GET /rules`, `GET /rules/:rule_id`, live-results)
  remain intentionally unguarded — a separate, not-yet-made policy decision.

## Branches

- `release` — stable/demo branch. Has the full RBAC + normalized-MySQL-
  rule-store surface merged in (2026-07-16, `--no-ff` merge commit
  `c2910c2`), *and* real WSO2/OIDC + JWKS token validation merged in
  (2026-07-20, `wso2 integration in version 3.0.0`, commit `c8ea714`),
  replacing the dev-only `X-Debug-User-Id` shim entirely — this branch has
  no dev-mode auth fallback of any kind. Only `release` and `demo` exist as
  branches in this repo now; every other branch used during this engagement
  (`rbac+persistentStore`, the original `rbac`) has been merged and deleted.
- `demo` — a separate, deliberately-diverged branch, **not** merged into
  `release`. It still uses the pre-WSO2 `X-Debug-User-Id` shim for identity
  (not WSO2), and its RBAC/rule-store storage layer differs from
  `release`'s — verify demo's own `CLAUDE.md` and code directly rather than
  assuming parity with this file.

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
6. **Run `go vet`/`go build` for cgo-touching packages via the PowerShell
   tool, not the Bash tool.** Confirmed 2026-07-20: invoking `gcc.exe` (and
   therefore `cgo.exe`, which shells out to it) from this environment's Bash
   tool silently fails — exit code 1/2, no stdout, no stderr, even for a
   trivial one-line `.c` file compiled directly — regardless of
   `dangerouslyDisableSandbox`. The identical command run via the
   PowerShell tool works immediately. Pure-Go packages (no cgo) vet/build
   fine from either tool; this only bites packages that actually invoke a C
   compiler (i.e. anything pulling in `internal/services`/`cmd/server`).
