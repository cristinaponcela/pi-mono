import type { SqliteDatabase } from "../types.ts";

export interface FactRow {
	session_id: string;
	seq: number;
	kind: string;
	key: string | null;
	value: string | null;
}

export async function appendFact(
	db: SqliteDatabase,
	sessionId: string,
	seq: number,
	kind: string,
	key: string | null,
	value: string | null,
): Promise<void> {
	await db
		.prepare("INSERT INTO facts (session_id, seq, kind, key, value) VALUES (?, ?, ?, ?, ?)")
		.run(sessionId, seq, kind, key, value);
}

export async function readLatestFact(
	db: SqliteDatabase,
	sessionId: string,
	kind: string,
	key: string | null,
): Promise<FactRow | undefined> {
	return db
		.prepare(
			`SELECT session_id, seq, kind, key, value
			FROM facts
			WHERE session_id = ? AND kind = ? AND key IS ?
			ORDER BY seq DESC
			LIMIT 1`,
		)
		.get<FactRow>(sessionId, kind, key);
}

export async function readLatestLabelFacts(
	db: SqliteDatabase,
	sessionId: string,
): Promise<Array<{ key: string; value: string }>> {
	return db
		.prepare(
			`SELECT key, value FROM (
				SELECT key, value, ROW_NUMBER() OVER (PARTITION BY key ORDER BY seq DESC) AS rank
				FROM facts
				WHERE session_id = ? AND kind = 'label'
			)
			WHERE rank = 1 AND value IS NOT NULL
			ORDER BY key`,
		)
		.all<{ key: string; value: string }>(sessionId);
}

export async function readFactRows(
	db: SqliteDatabase,
	sessionId: string,
	options: { afterSeq?: number } = {},
): Promise<FactRow[]> {
	const predicates = ["session_id = ?"];
	const params: unknown[] = [sessionId];
	if (options.afterSeq !== undefined) {
		predicates.push("seq > ?");
		params.push(options.afterSeq);
	}
	return db
		.prepare(
			`SELECT session_id, seq, kind, key, value
			FROM facts
			WHERE ${predicates.join(" AND ")}
			ORDER BY seq`,
		)
		.all<FactRow>(...params);
}

export async function deleteFactRows(db: SqliteDatabase, sessionId: string): Promise<void> {
	await db.prepare("DELETE FROM facts WHERE session_id = ?").run(sessionId);
}
