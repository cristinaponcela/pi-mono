CREATE TABLE IF NOT EXISTS sessions (
	id TEXT PRIMARY KEY,
	created_at TEXT NOT NULL,
	cwd TEXT NOT NULL,
	parent_session_id TEXT NULL,
	metadata TEXT NULL
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_sessions_created_at ON sessions(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_sessions_cwd ON sessions(cwd);
CREATE INDEX IF NOT EXISTS idx_sessions_parent ON sessions(parent_session_id);

CREATE TABLE IF NOT EXISTS entries (
	session_id TEXT NOT NULL,
	seq INTEGER NOT NULL,
	id TEXT NOT NULL,
	parent_id TEXT NULL,
	type TEXT NOT NULL,
	timestamp TEXT NOT NULL,
	payload TEXT NOT NULL,
	PRIMARY KEY (session_id, id),
	UNIQUE (session_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_entries_session_seq ON entries(session_id, seq);
CREATE INDEX IF NOT EXISTS idx_entries_session_parent ON entries(session_id, parent_id);
CREATE INDEX IF NOT EXISTS idx_entries_session_type_seq ON entries(session_id, type, seq);

CREATE TABLE IF NOT EXISTS session_sequences (
	session_id TEXT PRIMARY KEY,
	next_seq INTEGER NOT NULL
) WITHOUT ROWID;

-- Derived branch cache. Parent links in entries remain canonical; this cache
-- exists only to make branch scans cheap.
CREATE TABLE IF NOT EXISTS branch_entries (
	session_id TEXT NOT NULL,
	branch_id TEXT NOT NULL,
	entry_id TEXT NOT NULL,
	entry_seq INTEGER NOT NULL,
	entry_type TEXT NULL,
	custom_type TEXT NULL,
	PRIMARY KEY (session_id, branch_id, entry_id)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_branch_entries_session_branch_seq ON branch_entries(session_id, branch_id, entry_seq);
CREATE INDEX IF NOT EXISTS idx_branch_entries_session_entry ON branch_entries(session_id, entry_id);
CREATE INDEX IF NOT EXISTS idx_branch_entries_session_branch_type_seq ON branch_entries(session_id, branch_id, entry_type, entry_seq);
CREATE INDEX IF NOT EXISTS idx_branch_entries_session_branch_custom_seq ON branch_entries(session_id, branch_id, custom_type, entry_seq);
