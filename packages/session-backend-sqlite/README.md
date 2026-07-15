# @earendil-works/pi-session-backend-sqlite

SQLite-only session backend for pi agent sessions.

This package contains only SQLite-specific implementation:

- `SqliteSessionRepo`
- runtime-specific execution envs under `env/`:
  - `@earendil-works/pi-session-backend-sqlite/env/node`
  - `@earendil-works/pi-session-backend-sqlite/env/bun`
  - `@earendil-works/pi-session-backend-sqlite/env/better-sqlite3`
  - `@earendil-works/pi-session-backend-sqlite/env/sqlite3`
- SQLite-specific types (`SqliteDatabase`, `SqliteEnv`, `SqliteSessionMetadata`, ...)
- SQLite storage/schema code

The package root exports the backend-agnostic repository, storage, migrations, and types only. Runtime-specific SQLite adapters are split into `env/*` subpath exports so callers do not load Node, Bun, or npm SQLite code unless they explicitly choose that runtime.

It does not contain cross-backend selection logic or JSONL fallback wiring. Backend selection should live in a higher-level package or entrypoint that explicitly depends on both `@earendil-works/pi-agent-core` and this package.

## Migrations

Schema setup uses file-based SQLite migrations under `sql/migrations/`.

- `migrations` stores one row per applied migration
- migration identity comes from the filename using ordered numeric names (for example `001_initial.sql`)
- the current schema is the full ordered set of applied migration files
- this is separate from any per-session data/version fields
