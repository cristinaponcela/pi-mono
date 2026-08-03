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

export async function deleteFactsForSession(db: SqliteDatabase, sessionId: string): Promise<void> {
	await db.prepare("DELETE FROM facts WHERE session_id = ?").run(sessionId);
}
