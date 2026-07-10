import type { LeafEntry, SessionStorage, SessionTreeEntry } from "@earendil-works/pi-agent-core";
import { SessionError, uuidv7 } from "@earendil-works/pi-agent-core";
import type { SqliteDatabase, SqliteSessionMetadata } from "../types.ts";
import { buildPathToRoot, getMaterializedBranchPath } from "./branch-entries.ts";
import { decodeEntry, encodeEntry, type SessionEntryRow } from "./session-entries.ts";
import {
	applyEntryToMaterializedState,
	createEmptyMaterializedState,
	materializedStateFromRow,
	materializedStateValues,
	type SessionMaterializedRow,
	type SessionMaterializedState,
	serializeLabels,
	serializeModelThinkingConfigs,
} from "./session-materialized.ts";
import { advanceSequence, getNextSequence } from "./session-sequences.ts";
import { rowToMetadata, type SessionRow } from "./sessions.ts";
import { generateEntryId, invalidSession, leafIdAfterEntry } from "./shared.ts";

async function loadSqliteStorage(
	db: SqliteDatabase,
	sessionId: string,
): Promise<{
	row: SessionRow;
	entries: SessionTreeEntry[];
	leafId: string | null;
	materializedState: SessionMaterializedState;
}> {
	const row = await db
		.prepare("SELECT id, created_at, cwd, parent_session_id FROM sessions WHERE id = ?")
		.get<SessionRow>([sessionId]);
	if (!row) throw new SessionError("not_found", `Session not found: ${sessionId}`);

	const entryRows = await db
		.prepare(
			"SELECT session_id, id, entry_seq, parent_id, type, timestamp, payload, target_id, message_role, custom_type FROM session_entries WHERE session_id = ? ORDER BY entry_seq",
		)
		.all<SessionEntryRow>([sessionId]);
	const entries: SessionTreeEntry[] = [];
	let leafId: string | null = null;
	for (const entryRow of entryRows) {
		try {
			const entry = decodeEntry(entryRow);
			entries.push(entry);
			leafId = leafIdAfterEntry(entry);
		} catch {
			// Keep JSONL-like permissive resume behavior: skip malformed entries.
		}
	}
	const materializedRow = await db
		.prepare(
			"SELECT session_id, name, message_count, cached_tokens, uncached_tokens, total_tokens, cost_total, labels_json, model_thinking_configs_json FROM session_materialized WHERE session_id = ?",
		)
		.get<SessionMaterializedRow>([sessionId]);
	if (!materializedRow) throw invalidSession(`missing materialized row for session ${sessionId}`);
	return { row, entries, leafId, materializedState: materializedStateFromRow(materializedRow, entries) };
}

export class SqliteSessionStorage implements SessionStorage<SqliteSessionMetadata> {
	private readonly db: SqliteDatabase;
	private readonly metadata: SqliteSessionMetadata;
	private entries: SessionTreeEntry[];
	private byId: Map<string, SessionTreeEntry>;
	private labelsById: Map<string, string>;
	private currentLeafId: string | null;
	private activeBranchId: string | null;
	private materializedState: SessionMaterializedState;

	private constructor(
		db: SqliteDatabase,
		metadata: SqliteSessionMetadata,
		entries: SessionTreeEntry[],
		leafId: string | null,
		activeBranchId: string | null,
		materializedState: SessionMaterializedState,
	) {
		this.db = db;
		this.metadata = metadata;
		this.entries = entries;
		this.byId = new Map(entries.map((entry) => [entry.id, entry]));
		this.materializedState = materializedState;
		this.labelsById = materializedState.labelsById;
		this.currentLeafId = leafId;
		this.activeBranchId = activeBranchId;
	}

	static async open(db: SqliteDatabase, metadata: SqliteSessionMetadata): Promise<SqliteSessionStorage> {
		const loaded = await loadSqliteStorage(db, metadata.id);
		return new SqliteSessionStorage(
			db,
			rowToMetadata(loaded.row, metadata.path),
			loaded.entries,
			loaded.leafId,
			null,
			loaded.materializedState,
		);
	}

	static async create(
		db: SqliteDatabase,
		path: string,
		options: { cwd: string; sessionId: string; parentSessionId?: string },
	): Promise<SqliteSessionStorage> {
		const createdAt = new Date().toISOString();
		await db
			.prepare("INSERT INTO sessions (id, created_at, cwd, parent_session_id) VALUES (?, ?, ?, ?)")
			.run([options.sessionId, createdAt, options.cwd, options.parentSessionId ?? null]);
		await db
			.prepare("INSERT INTO session_sequences (session_id, next_seq) VALUES (?, ?)")
			.run([options.sessionId, 1]);
		await db
			.prepare(
				"INSERT INTO session_materialized (session_id, name, message_count, cached_tokens, uncached_tokens, total_tokens, cost_total, labels_json, model_thinking_configs_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
			)
			.run(materializedStateValues(options.sessionId, createEmptyMaterializedState()));
		return new SqliteSessionStorage(
			db,
			{
				id: options.sessionId,
				createdAt,
				cwd: options.cwd,
				path,
				parentSessionId: options.parentSessionId,
			},
			[],
			null,
			null,
			createEmptyMaterializedState(),
		);
	}

	async getMetadata(): Promise<SqliteSessionMetadata> {
		return this.metadata;
	}

	async getLeafId(): Promise<string | null> {
		if (this.currentLeafId !== null && !this.byId.has(this.currentLeafId)) {
			throw new SessionError("invalid_session", `Entry ${this.currentLeafId} not found`);
		}
		return this.currentLeafId;
	}

	private async materializeBranch(leafId: string | null): Promise<void> {
		const branchId = uuidv7();
		const path = buildPathToRoot(this.byId, leafId);
		for (const entry of path) {
			await this.db
				.prepare("INSERT INTO branch_entries (session_id, branch_id, entry_id) VALUES (?, ?, ?)")
				.run([this.metadata.id, branchId, entry.id]);
		}
		this.activeBranchId = branchId;
	}

	private async appendToActiveBranch(entryId: string, parentId: string | null): Promise<void> {
		if (!this.activeBranchId) {
			await this.materializeBranch(parentId);
		}
		if (!this.activeBranchId) {
			throw invalidSession(`active branch missing for session ${this.metadata.id}`);
		}
		await this.db
			.prepare("INSERT INTO branch_entries (session_id, branch_id, entry_id) VALUES (?, ?, ?)")
			.run([this.metadata.id, this.activeBranchId, entryId]);
	}

	async setLeafId(leafId: string | null): Promise<void> {
		if (leafId !== null && !this.byId.has(leafId)) {
			throw new SessionError("not_found", `Entry ${leafId} not found`);
		}
		const entry: LeafEntry = {
			type: "leaf",
			id: generateEntryId(this.byId),
			parentId: this.currentLeafId,
			timestamp: new Date().toISOString(),
			targetId: leafId,
		};
		await this.appendEntry(entry);
	}

	async createEntryId(): Promise<string> {
		return generateEntryId(this.byId);
	}

	async appendEntry(entry: SessionTreeEntry): Promise<void> {
		const encoded = encodeEntry(entry);
		const previousMaterializedState: SessionMaterializedState = {
			...this.materializedState,
			labelsById: new Map(this.materializedState.labelsById),
			modelThinkingConfigs: [...this.materializedState.modelThinkingConfigs],
			currentModel: this.materializedState.currentModel ? { ...this.materializedState.currentModel } : null,
		};
		try {
			applyEntryToMaterializedState(this.materializedState, entry);
			await this.db.transaction(async () => {
				const nextSeq = await getNextSequence(this.db, this.metadata.id);
				await this.db
					.prepare(
						"INSERT INTO session_entries (session_id, id, entry_seq, parent_id, type, timestamp, payload, target_id, message_role, custom_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
					)
					.run([
						this.metadata.id,
						entry.id,
						nextSeq,
						entry.parentId,
						entry.type,
						entry.timestamp,
						encoded.payload,
						encoded.targetId ?? null,
						encoded.messageRole ?? null,
						encoded.customType ?? null,
					]);
				await advanceSequence(this.db, this.metadata.id, nextSeq);
				await this.db
					.prepare(
						"UPDATE session_materialized SET name = ?, message_count = ?, cached_tokens = ?, uncached_tokens = ?, total_tokens = ?, cost_total = ?, labels_json = ?, model_thinking_configs_json = ? WHERE session_id = ?",
					)
					.run([
						this.materializedState.name ?? null,
						this.materializedState.messageCount,
						this.materializedState.cachedTokens,
						this.materializedState.uncachedTokens,
						this.materializedState.totalTokens,
						this.materializedState.costTotal,
						serializeLabels(this.materializedState.labelsById),
						serializeModelThinkingConfigs(this.materializedState.modelThinkingConfigs),
						this.metadata.id,
					]);
				this.entries.push(entry);
				this.byId.set(entry.id, entry);
				this.currentLeafId = leafIdAfterEntry(entry);
				if (entry.type === "leaf") {
					await this.materializeBranch(entry.targetId);
				} else {
					await this.appendToActiveBranch(entry.id, entry.parentId);
				}
			});
		} catch (error) {
			this.materializedState = previousMaterializedState;
			this.labelsById = previousMaterializedState.labelsById;
			if (error instanceof SessionError) throw error;
			throw new SessionError("storage", `Failed to append SQLite session entry ${entry.id}`);
		}
	}

	async getEntry(id: string): Promise<SessionTreeEntry | undefined> {
		return this.byId.get(id);
	}

	async findEntries<TType extends SessionTreeEntry["type"]>(
		type: TType,
	): Promise<Array<Extract<SessionTreeEntry, { type: TType }>>> {
		return this.entries.filter((entry): entry is Extract<SessionTreeEntry, { type: TType }> => entry.type === type);
	}

	async getLabel(id: string): Promise<string | undefined> {
		return this.labelsById.get(id);
	}

	async getPathToRoot(leafId: string | null): Promise<SessionTreeEntry[]> {
		if (leafId === null) return [];
		if (leafId === this.currentLeafId && this.activeBranchId) {
			return getMaterializedBranchPath(this.db, this.metadata.id, this.activeBranchId, this.byId);
		}
		return buildPathToRoot(this.byId, leafId);
	}

	async getEntries(): Promise<SessionTreeEntry[]> {
		return [...this.entries];
	}

	async cleanup(): Promise<void> {
		await this.db.close();
	}
}
