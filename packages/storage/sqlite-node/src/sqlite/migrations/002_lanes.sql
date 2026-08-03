-- V2 harness: session/lane orchestration tables. 
-- This migration adds the new durable surfaces introduced by lanes and records.

CREATE TABLE IF NOT EXISTS lanes (
	session_id TEXT NOT NULL,
	lane TEXT NOT NULL,
	leaf_id TEXT NULL,
	PRIMARY KEY (session_id, lane)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_lanes_session_leaf ON lanes(session_id, leaf_id);

CREATE TABLE IF NOT EXISTS records (
	session_id TEXT NOT NULL,
	seq INTEGER NOT NULL,
	id TEXT NOT NULL,
	lane TEXT NOT NULL,
	run_id TEXT NULL,
	type TEXT NOT NULL,
	op_kind TEXT NULL,
	timestamp TEXT NOT NULL,
	payload TEXT NOT NULL,
	PRIMARY KEY (session_id, id),
	UNIQUE (session_id, seq)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_records_session_seq ON records(session_id, seq);
CREATE INDEX IF NOT EXISTS idx_records_session_lane_type_seq ON records(session_id, lane, type, seq);
CREATE INDEX IF NOT EXISTS idx_records_session_lane_type_op_kind_seq ON records(session_id, lane, type, op_kind, seq);
CREATE INDEX IF NOT EXISTS idx_records_session_run_id_seq ON records(session_id, run_id, seq);

CREATE TABLE IF NOT EXISTS lane_moves (
	session_id TEXT NOT NULL,
	seq INTEGER NOT NULL,
	lane TEXT NOT NULL,
	leaf_id TEXT NULL,
	PRIMARY KEY (session_id, seq)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_lane_moves_session_lane_seq ON lane_moves(session_id, lane, seq);

CREATE TABLE IF NOT EXISTS facts (
	session_id TEXT NOT NULL,
	seq INTEGER NOT NULL,
	kind TEXT NOT NULL,
	key TEXT NULL,
	value TEXT NULL,
	PRIMARY KEY (session_id, seq)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_facts_session_kind_key_seq ON facts(session_id, kind, key, seq);

CREATE TABLE IF NOT EXISTS branch_tips (
	session_id TEXT NOT NULL,
	tip_id TEXT NOT NULL,
	branch_id TEXT NOT NULL,
	PRIMARY KEY (session_id, tip_id),
	UNIQUE (session_id, branch_id)
) WITHOUT ROWID;

-- Writer claim. The serving layer owns policy; SQLite can still persist the
-- current owner/heartbeat for a session.
CREATE TABLE IF NOT EXISTS leases (
	session_id TEXT PRIMARY KEY,
	owner TEXT NOT NULL,
	heartbeat INTEGER NOT NULL
) WITHOUT ROWID;
