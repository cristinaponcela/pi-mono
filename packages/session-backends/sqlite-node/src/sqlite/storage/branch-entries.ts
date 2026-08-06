import type { Entry } from "@earendil-works/pi-agent-core";
import type { SqliteDatabase } from "../types.ts";

/** Derived root-to-tip branch cache membership. Canonical parent links remain in entries. */
export interface CachedBranch {
	branchId: string;
	leafSeq: number;
}

export interface CachedBranchEntryRow {
	session_id: string;
	id: string;
	entry_seq: number;
	parent_id: string | null;
	type: Entry["type"];
	timestamp: string;
	payload: string;
}

export interface CachedBranchQuery {
	type?: Entry["type"];
	customType?: string;
	stopAtType?: Entry["type"];
	stopAtId?: string;
	cursor?: { afterSeq: number };
	order?: "newestFirst" | "oldestFirst";
	limit?: number;
}

export function readCachedBranch(db: SqliteDatabase, sessionId: string, leafId: string) {
	const membership = db
		.prepare(
			"SELECT branch_id, entry_seq FROM branch_entries WHERE session_id = ? AND entry_id = ? ORDER BY branch_id LIMIT 1",
		)
		.get<{ branch_id: string; entry_seq: number }>(sessionId, leafId);
	if (!membership) return undefined;
	return { branchId: membership.branch_id, leafSeq: membership.entry_seq };
}

export function queryCachedBranchRows(
	db: SqliteDatabase,
	sessionId: string,
	branch: CachedBranch,
	query: CachedBranchQuery,
) {
	const oldestFirst = query.order === "oldestFirst";
	const boundaryParams: unknown[] = [sessionId, branch.branchId, branch.leafSeq];
	const stopPredicates: string[] = [];
	if (query.stopAtType !== undefined) {
		stopPredicates.push("stop.entry_type = ?");
		boundaryParams.push(query.stopAtType);
	}
	if (query.stopAtId !== undefined) {
		stopPredicates.push("stop.entry_id = ?");
		boundaryParams.push(query.stopAtId);
	}

	const boundary = stopPredicates.length
		? `WITH boundary AS (
			SELECT ${oldestFirst ? "MIN" : "MAX"}(stop.entry_seq) AS entry_seq
			FROM branch_entries AS stop
			WHERE stop.session_id = ? AND stop.branch_id = ? AND stop.entry_seq <= ?
				AND (${stopPredicates.join(" OR ")})
		)`
		: "";

	const predicates = ["b.session_id = ?", "b.branch_id = ?", "b.entry_seq <= ?"];
	const params = [...(stopPredicates.length === 0 ? [] : boundaryParams), sessionId, branch.branchId, branch.leafSeq];
	if (stopPredicates.length > 0) {
		predicates.push(
			`b.entry_seq ${oldestFirst ? "<=" : ">="} COALESCE((SELECT entry_seq FROM boundary), ${
				oldestFirst ? branch.leafSeq : 0
			})`,
		);
	}
	if (query.cursor !== undefined) {
		predicates.push(`b.entry_seq ${oldestFirst ? ">" : "<"} ?`);
		params.push(query.cursor.afterSeq);
	}
	if (query.type !== undefined) {
		predicates.push("b.entry_type = ?");
		params.push(query.type);
	}
	if (query.customType !== undefined) {
		predicates.push("b.custom_type = ?");
		params.push(query.customType);
	}
	const limit = query.limit === undefined ? "" : " LIMIT ?";
	if (query.limit !== undefined) params.push(query.limit);

	const sql = `${boundary}
		SELECT e.session_id, e.id, e.seq AS entry_seq, e.parent_id, e.type, e.timestamp, e.payload
		FROM branch_entries AS b
		JOIN entries AS e ON e.session_id = b.session_id AND e.id = b.entry_id
		WHERE ${predicates.join(" AND ")}
		ORDER BY b.entry_seq ${oldestFirst ? "ASC" : "DESC"}${limit}`;

	return db.prepare(sql).all<CachedBranchEntryRow>(...params);
}

export function deleteBranchEntries(db: SqliteDatabase, sessionId: string) {
	db.prepare("DELETE FROM branch_entries WHERE session_id = ?").run(sessionId);
}

export function insertBranchEntry(
	db: SqliteDatabase,
	sessionId: string,
	branchId: string,
	entryId: string,
	entrySeq: number,
	entryType: string,
	customType: string | null,
) {
	db.prepare(
		`INSERT INTO branch_entries
			(session_id, branch_id, entry_id, entry_seq, entry_type, custom_type)
			VALUES (?, ?, ?, ?, ?, ?)`,
	).run(sessionId, branchId, entryId, entrySeq, entryType, customType);
}

export function insertBranchEntriesForPath(db: SqliteDatabase, sessionId: string, branchId: string, leafId: string) {
	db.prepare(
		`WITH RECURSIVE path(id, entry_seq, parent_id, type, custom_type) AS (
				SELECT id, seq, parent_id, type,
					CASE WHEN type = 'custom' THEN json_extract(payload, '$.customType') ELSE NULL END
				FROM entries
				WHERE session_id = ? AND id = ?
				UNION ALL
				SELECT parent.id, parent.seq, parent.parent_id, parent.type,
					CASE WHEN parent.type = 'custom' THEN json_extract(parent.payload, '$.customType') ELSE NULL END
				FROM entries AS parent
				JOIN path AS child ON child.parent_id = parent.id
				WHERE parent.session_id = ?
			)
			INSERT INTO branch_entries (session_id, branch_id, entry_id, entry_seq, entry_type, custom_type)
			SELECT ?, ?, id, entry_seq, type, custom_type FROM path`,
	).run(sessionId, leafId, sessionId, sessionId, branchId);
}

export function readBranchContainingEntry(db: SqliteDatabase, sessionId: string, entryId: string) {
	const row = db
		.prepare(
			`SELECT b.branch_id, b.entry_seq
			FROM branch_entries AS b
			WHERE b.session_id = ? AND b.entry_id = ?
			ORDER BY b.branch_id
			LIMIT 1`,
		)
		.get<{ branch_id: string; entry_seq: number }>(sessionId, entryId);
	return row === undefined ? undefined : { branchId: row.branch_id, entrySeq: row.entry_seq };
}

export function copyBranchEntriesThroughSeq(
	db: SqliteDatabase,
	sessionId: string,
	targetBranchId: string,
	sourceBranchId: string,
	throughSeq: number,
) {
	db.prepare(
		`INSERT INTO branch_entries (session_id, branch_id, entry_id, entry_seq, entry_type, custom_type)
			SELECT session_id, ?, entry_id, entry_seq, entry_type, custom_type
			FROM branch_entries
			WHERE session_id = ? AND branch_id = ? AND entry_seq <= ?`,
	).run(targetBranchId, sessionId, sourceBranchId, throughSeq);
}
