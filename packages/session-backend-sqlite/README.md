# @earendil-works/pi-session-backend-sqlite

SQLite-only session backend for pi agent sessions.

This package contains only SQLite-specific implementation:

- `SqliteSessionRepo`
- `SqliteNodeExecutionEnv`
- SQLite-specific types (`SqliteDatabase`, `SqliteEnv`, `SqliteSessionMetadata`, ...)
- SQLite storage/schema code

It does not contain cross-backend selection logic or JSONL fallback wiring. Backend selection should live in a higher-level package or entrypoint that explicitly depends on both `@earendil-works/pi-agent-core` and this package.
