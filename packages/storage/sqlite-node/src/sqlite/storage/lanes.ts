import { SessionError } from "@earendil-works/pi-agent-core/experimental";
import type { SqliteDatabase } from "../types.ts";

export interface LaneRow {
	session_id: string;
	lane: string;
	leaf_id: string | null;
}

export interface LaneMoveRow {
	session_id: string;
	seq: number;
	lane: string;
	leaf_id: string | null;
}

export async function createInitialLane(
	db: SqliteDatabase,
	sessionId: string,
	lane = "main",
	leafId: string | null = null,
): Promise<void> {
	await db.prepare("INSERT INTO lanes (session_id, lane, leaf_id) VALUES (?, ?, ?)").run(sessionId, lane, leafId);
}

export async function readLanes(db: SqliteDatabase, sessionId: string): Promise<LaneRow[]> {
	return db
		.prepare("SELECT session_id, lane, leaf_id FROM lanes WHERE session_id = ? ORDER BY lane")
		.all<LaneRow>(sessionId);
}

export async function readLane(db: SqliteDatabase, sessionId: string, lane: string): Promise<LaneRow | undefined> {
	return db
		.prepare("SELECT session_id, lane, leaf_id FROM lanes WHERE session_id = ? AND lane = ?")
		.get<LaneRow>(sessionId, lane);
}

export async function readLaneHead(
	db: SqliteDatabase,
	sessionId: string,
	lane: string,
): Promise<{ leafId: string | null }> {
	const row = await db
		.prepare(
			`SELECT
				l.leaf_id,
				(l.leaf_id IS NULL OR EXISTS (
					SELECT 1 FROM entries AS e WHERE e.session_id = l.session_id AND e.id = l.leaf_id
				)) AS leaf_exists
			FROM lanes AS l
			WHERE l.session_id = ? AND l.lane = ?`,
		)
		.get<{ leaf_id: string | null; leaf_exists: number }>(sessionId, lane);
	if (!row) throw new SessionError("invalid_lane", `Lane not found: ${lane}`);
	if (row.leaf_exists === 0) throw new SessionError("storage", `Entry ${row.leaf_id} not found`);
	return { leafId: row.leaf_id };
}

export async function createLane(
	db: SqliteDatabase,
	sessionId: string,
	seq: number,
	lane: string,
	leafId: string | null,
): Promise<void> {
	await db.prepare("INSERT INTO lanes (session_id, lane, leaf_id) VALUES (?, ?, ?)").run(sessionId, lane, leafId);
	await appendLaneMove(db, sessionId, seq, lane, leafId);
}

export async function moveLane(
	db: SqliteDatabase,
	sessionId: string,
	seq: number,
	lane: string,
	leafId: string | null,
): Promise<void> {
	const result = await db
		.prepare("UPDATE lanes SET leaf_id = ? WHERE session_id = ? AND lane = ?")
		.run(leafId, sessionId, lane);
	if (result.changes !== 1) throw new SessionError("invalid_lane", `Lane not found: ${lane}`);
	await appendLaneMove(db, sessionId, seq, lane, leafId);
}

export async function setLaneLeaf(
	db: SqliteDatabase,
	sessionId: string,
	lane: string,
	leafId: string | null,
): Promise<void> {
	const result = await db
		.prepare("UPDATE lanes SET leaf_id = ? WHERE session_id = ? AND lane = ?")
		.run(leafId, sessionId, lane);
	if (result.changes !== 1) throw new SessionError("invalid_lane", `Lane not found: ${lane}`);
}

async function appendLaneMove(
	db: SqliteDatabase,
	sessionId: string,
	seq: number,
	lane: string,
	leafId: string | null,
): Promise<void> {
	await db
		.prepare("INSERT INTO lane_moves (session_id, seq, lane, leaf_id) VALUES (?, ?, ?, ?)")
		.run(sessionId, seq, lane, leafId);
}
