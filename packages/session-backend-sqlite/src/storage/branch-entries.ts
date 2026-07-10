import type { SessionTreeEntry } from "@earendil-works/pi-agent-core";
import { SessionError } from "@earendil-works/pi-agent-core";
import type { SqliteDatabase } from "../types.ts";

export interface BranchEntryIdRow {
	entry_id: string;
}

export function buildPathToRoot(byId: Map<string, SessionTreeEntry>, leafId: string | null): SessionTreeEntry[] {
	if (leafId === null) return [];
	const path: SessionTreeEntry[] = [];
	let stopAtEntryId: string | null = null;
	let current = byId.get(leafId);
	if (!current) throw new SessionError("not_found", `Entry ${leafId} not found`);
	while (current) {
		path.unshift(current);
		if (stopAtEntryId !== null && current.id === stopAtEntryId) break;
		if (current.type === "compaction") {
			if (current.retainedTail) break;
			stopAtEntryId = current.firstKeptEntryId;
		}
		if (!current.parentId) break;
		const parent = byId.get(current.parentId);
		if (!parent) throw new SessionError("invalid_session", `Entry ${current.parentId} not found`);
		current = parent;
	}
	return path;
}

export async function getMaterializedBranchPath(
	db: SqliteDatabase,
	sessionId: string,
	branchId: string,
	byId: Map<string, SessionTreeEntry>,
): Promise<SessionTreeEntry[]> {
	const rows = await db
		.prepare(
			"SELECT e.id AS entry_id FROM session_entries e WHERE e.session_id = ? AND EXISTS (SELECT 1 FROM branch_entries b WHERE b.session_id = e.session_id AND b.branch_id = ? AND b.entry_id = e.id) ORDER BY e.entry_seq",
		)
		.all<BranchEntryIdRow>([sessionId, branchId]);
	const entries: SessionTreeEntry[] = [];
	for (const row of rows) {
		const entry = byId.get(row.entry_id);
		if (entry) entries.push(entry);
	}
	return entries;
}
