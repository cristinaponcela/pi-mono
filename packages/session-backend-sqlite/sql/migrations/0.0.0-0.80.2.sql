-- Initial SQLite session backend schema.

-- CPON becomes index of entry within session

CREATE TABLE IF NOT EXISTS sessions (
	id TEXT PRIMARY KEY,
	created_at TEXT NOT NULL,
	cwd TEXT NOT NULL,
	parent_session_id TEXT NULL
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_sessions_created_at ON sessions(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_sessions_cwd ON sessions(cwd);
CREATE INDEX IF NOT EXISTS idx_sessions_parent ON sessions(parent_session_id);

CREATE TABLE IF NOT EXISTS session_entries (
	session_id TEXT NOT NULL,
	id TEXT NOT NULL,
	entry_seq INTEGER NOT NULL,
	parent_id TEXT NULL,
	type TEXT NOT NULL,
	timestamp TEXT NOT NULL,
	payload TEXT NOT NULL,
	target_id TEXT NULL,
	message_role TEXT NULL,
	custom_type TEXT NULL,
	PRIMARY KEY (session_id, id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_session_entries_session_seq ON session_entries(session_id, entry_seq);
CREATE INDEX IF NOT EXISTS idx_session_entries_session_parent ON session_entries(session_id, parent_id);
CREATE INDEX IF NOT EXISTS idx_session_entries_session_type ON session_entries(session_id, type);
CREATE INDEX IF NOT EXISTS idx_session_entries_session_target ON session_entries(session_id, target_id);
CREATE INDEX IF NOT EXISTS idx_session_entries_session_message_role ON session_entries(session_id, message_role);

CREATE TABLE IF NOT EXISTS session_sequences (
	session_id TEXT PRIMARY KEY,
	next_seq INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS branch_entries (
	session_id TEXT NOT NULL,
	branch_id TEXT NOT NULL,
	entry_id TEXT NOT NULL,
	PRIMARY KEY (session_id, branch_id, entry_id)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_branch_entries_session_branch ON branch_entries(session_id, branch_id);
CREATE INDEX IF NOT EXISTS idx_branch_entries_session_entry ON branch_entries(session_id, entry_id);

CREATE TABLE IF NOT EXISTS session_materialized (
	session_id TEXT PRIMARY KEY,
	name TEXT NULL,
	message_count INTEGER NOT NULL,
	cached_tokens INTEGER NOT NULL,
	uncached_tokens INTEGER NOT NULL,
	total_tokens INTEGER NOT NULL,
	cost_total REAL NOT NULL,
	labels_json TEXT NOT NULL,
	model_thinking_configs_json TEXT NOT NULL
) WITHOUT ROWID;
