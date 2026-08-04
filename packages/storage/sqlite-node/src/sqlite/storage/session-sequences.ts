import { SessionError } from "@earendil-works/pi-agent-core/experimental";
import type { SqliteDatabase } from "../types.ts";

export async function createSequence(db: SqliteDatabase, sessionId: string, nextSeq = 1): Promise<void> {
	await db.prepare("INSERT INTO session_sequences (session_id, next_seq) VALUES (?, ?)").run(sessionId, nextSeq);
}

export async function getNextSequence(db: SqliteDatabase, sessionId: string): Promise<number> {
	const sequenceRow = await db
		.prepare("SELECT next_seq FROM session_sequences WHERE session_id = ?")
		.get<{ next_seq: number }>(sessionId);
	if (!sequenceRow) {
		throw new SessionError("storage", `Missing sequence row for session ${sessionId}`);
	}
	return sequenceRow.next_seq;
}

export async function setNextSequence(db: SqliteDatabase, sessionId: string, nextSeq: number): Promise<void> {
	await db.prepare("UPDATE session_sequences SET next_seq = ? WHERE session_id = ?").run(nextSeq, sessionId);
}

export async function advanceSequence(db: SqliteDatabase, sessionId: string, seq: number): Promise<void> {
	await setNextSequence(db, sessionId, seq + 1);
}

export async function deleteSequence(db: SqliteDatabase, sessionId: string): Promise<void> {
	await db.prepare("DELETE FROM session_sequences WHERE session_id = ?").run(sessionId);
}
