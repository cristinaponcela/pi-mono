import type { SqliteDatabase } from "../types.ts";

export interface LeaseRow {
	session_id: string;
	owner: string;
	heartbeat: number;
}

export async function readLease(db: SqliteDatabase, sessionId: string): Promise<LeaseRow | undefined> {
	return db.prepare("SELECT session_id, owner, heartbeat FROM leases WHERE session_id = ?").get<LeaseRow>(sessionId);
}

export async function upsertLease(
	db: SqliteDatabase,
	sessionId: string,
	owner: string,
	heartbeat: number,
): Promise<void> {
	await db
		.prepare(
			`INSERT INTO leases (session_id, owner, heartbeat) VALUES (?, ?, ?)
			ON CONFLICT(session_id) DO UPDATE SET owner = excluded.owner, heartbeat = excluded.heartbeat`,
		)
		.run(sessionId, owner, heartbeat);
}

export async function deleteLease(db: SqliteDatabase, sessionId: string): Promise<void> {
	await db.prepare("DELETE FROM leases WHERE session_id = ?").run(sessionId);
}

export async function deleteLeasesForSession(db: SqliteDatabase, sessionId: string): Promise<void> {
	await deleteLease(db, sessionId);
}
