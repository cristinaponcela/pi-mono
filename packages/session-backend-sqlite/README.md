# @earendil-works/pi-session-backend-sqlite

SQLite-only session backend for pi agent sessions.

This package contains only SQLite-specific implementation:

- `SqliteSessionRepo`
- `SqliteNodeExecutionEnv`
- SQLite-specific types (`SqliteDatabase`, `SqliteEnv`, `SqliteSessionMetadata`, ...)
- SQLite storage/schema code

It does not contain cross-backend selection logic or JSONL fallback wiring. Backend selection should live in a higher-level package or entrypoint that explicitly depends on both `@earendil-works/pi-agent-core` and this package.

## Migrations

Schema setup uses file-based SQLite migrations under `sql/migrations/`.

- `migrations` stores one row per applied migration
- migration identity comes from the filename using the same version-to-version style as `absurd` (for example `0.0.0-0.80.2.sql`)
- the highest applied migration target is the current schema version
- this is separate from any per-session data/version fields
