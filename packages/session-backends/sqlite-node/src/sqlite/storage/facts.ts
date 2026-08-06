import type { SqliteDatabase } from "../types.ts";

export interface FactRow {
	session_id: string;
	seq: number;
	kind: string;
	key: string | null;
	value: string | null;
}

export function appendFact(
	db: SqliteDatabase,
	sessionId: string,
	seq: number,
	kind: string,
	key: string | null,
	value: string | null,
) {
	db.prepare("INSERT INTO facts (session_id, seq, kind, key, value) VALUES (?, ?, ?, ?, ?)").run(
		sessionId,
		seq,
		kind,
		key,
		value,
	);
}

export function readLatestFact(db: SqliteDatabase, sessionId: string, kind: string, key: string | null) {
	return db
		.prepare(
			`SELECT session_id, seq, kind, key, value
			FROM facts INDEXED BY idx_facts_session_kind_key_seq
			WHERE session_id = ? AND kind = ? AND key IS ?
			ORDER BY seq DESC
			LIMIT 1`,
		)
		.get<FactRow>(sessionId, kind, key);
}

export function readLatestLabelFacts(db: SqliteDatabase, sessionId: string) {
	return db
		.prepare(
			`WITH latest AS (
				SELECT key, MAX(seq) AS seq
				FROM facts INDEXED BY idx_facts_session_kind_key_seq
				WHERE session_id = ? AND kind = 'label'
				GROUP BY key
			)
			SELECT latest.key, f.value
			FROM latest
			JOIN facts AS f ON f.session_id = ? AND f.seq = latest.seq
			WHERE f.value IS NOT NULL
			ORDER BY latest.key`,
		)
		.all<{ key: string; value: string }>(sessionId, sessionId);
}

export function readFactRows(
	db: SqliteDatabase,
	sessionId: string,
	options: { afterSeq?: number; limit?: number } = {},
) {
	const predicates = ["session_id = ?"];
	const params: unknown[] = [sessionId];
	if (options.afterSeq !== undefined) {
		predicates.push("seq > ?");
		params.push(options.afterSeq);
	}
	const limit = options.limit === undefined ? "" : " LIMIT ?";
	if (options.limit !== undefined) params.push(options.limit);
	return db
		.prepare(
			`SELECT session_id, seq, kind, key, value
			FROM facts
			WHERE ${predicates.join(" AND ")}
			ORDER BY seq${limit}`,
		)
		.all<FactRow>(...params);
}

export function deleteFactRows(db: SqliteDatabase, sessionId: string) {
	db.prepare("DELETE FROM facts WHERE session_id = ?").run(sessionId);
}
