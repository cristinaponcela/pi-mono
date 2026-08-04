import { SessionError } from "@earendil-works/pi-agent-core/experimental";
import type { SqliteDatabase } from "../types.ts";

export async function getNextSequence(db: SqliteDatabase, sessionId: string): Promise<number> {
	const sequenceRow = await db
		.prepare("SELECT next_seq FROM session_sequences WHERE session_id = ?")
		.get<{ next_seq: number }>(sessionId);
	if (!sequenceRow) {
		throw new SessionError("storage", `Missing sequence row for session ${sessionId}`);
	}
	return sequenceRow.next_seq;
}

export async function advanceSequence(db: SqliteDatabase, sessionId: string, nextSeq: number): Promise<void> {
	await db.prepare("UPDATE session_sequences SET next_seq = ? WHERE session_id = ?").run(nextSeq + 1, sessionId);
}
