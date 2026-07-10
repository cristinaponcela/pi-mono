import type { SqliteSessionMetadata } from "../types.ts";

export interface SessionRow {
	id: string;
	created_at: string;
	cwd: string;
	parent_session_id: string | null;
}

export function rowToMetadata(row: SessionRow, path: string): SqliteSessionMetadata {
	return {
		id: row.id,
		createdAt: row.created_at,
		cwd: row.cwd,
		path,
		parentSessionId: row.parent_session_id ?? undefined,
	};
}
