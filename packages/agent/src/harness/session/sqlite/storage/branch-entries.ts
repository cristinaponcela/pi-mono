import type { SessionTreeEntry } from "../../../types.ts";
import type { SqliteDatabase } from "../types.ts";

export interface BranchEntryRow {
	entry_id: string;
	entry_seq: number;
}

export async function getMaterializedBranchPathOrCompaction(
	db: SqliteDatabase,
	sessionId: string,
	branchId: string,
	byId: Map<string, SessionTreeEntry>,
): Promise<SessionTreeEntry[]> {
	const rows = await db
		.prepare(
			"SELECT entry_id, entry_seq FROM branch_entries WHERE session_id = ? AND branch_id = ? ORDER BY entry_seq",
		)
		.all<BranchEntryRow>([sessionId, branchId]);
	const entries: SessionTreeEntry[] = [];
	for (const row of rows) {
		const entry = byId.get(row.entry_id);
		if (entry) entries.push(entry);
	}
	return entries;
}
