import type {
	ClosableSessionStorage,
	LeafEntry,
	SessionStorage,
	SessionTreeEntry,
	SessionTreeEntryBase,
	SqliteDatabase,
	SqliteSessionMetadata,
} from "../../types.ts";
import { SessionError } from "../../types.ts";
import { uuidv7 } from "../uuid.ts";

interface SessionRow {
	id: string;
	created_at: string;
	cwd: string;
	parent_session_id: string | null;
}

interface SessionEntryRow {
	seq: number;
	session_id: string;
	id: string;
	parent_id: string | null;
	type: SessionTreeEntry["type"];
	timestamp: string;
	payload: string;
	target_id: string | null;
	message_role: string | null;
	custom_type: string | null;
	entry_counter: number | null;
}

interface BranchEntryIdRow {
	entry_id: string;
}

type EncodedEntry = {
	targetId?: string | null;
	messageRole?: string;
	customType?: string;
	payload: string;
};

type EntryPayload<TEntry extends SessionTreeEntry> = Omit<TEntry, keyof SessionTreeEntryBase | "type">;

type MessagePayload = EntryPayload<Extract<SessionTreeEntry, { type: "message" }>>;
type ThinkingLevelChangePayload = EntryPayload<Extract<SessionTreeEntry, { type: "thinking_level_change" }>>;
type ModelChangePayload = EntryPayload<Extract<SessionTreeEntry, { type: "model_change" }>>;
type ActiveToolsChangePayload = EntryPayload<Extract<SessionTreeEntry, { type: "active_tools_change" }>>;
type CompactionPayload = EntryPayload<Extract<SessionTreeEntry, { type: "compaction" }>>;
type BranchSummaryPayload = EntryPayload<Extract<SessionTreeEntry, { type: "branch_summary" }>>;
type CustomPayload = EntryPayload<Extract<SessionTreeEntry, { type: "custom" }>>;
type CustomMessagePayload = EntryPayload<Extract<SessionTreeEntry, { type: "custom_message" }>>;
type LabelPayload = EntryPayload<Extract<SessionTreeEntry, { type: "label" }>>;
type SessionInfoPayload = EntryPayload<Extract<SessionTreeEntry, { type: "session_info" }>>;
type LeafPayload = EntryPayload<Extract<SessionTreeEntry, { type: "leaf" }>>;

function updateLabelCache(labelsById: Map<string, string>, entry: SessionTreeEntry): void {
	if (entry.type !== "label") return;
	const label = entry.label?.trim();
	if (label) {
		labelsById.set(entry.targetId, label);
	} else {
		labelsById.delete(entry.targetId);
	}
}

function buildLabelsById(entries: SessionTreeEntry[]): Map<string, string> {
	const labelsById = new Map<string, string>();
	for (const entry of entries) {
		updateLabelCache(labelsById, entry);
	}
	return labelsById;
}

function generateEntryId(byId: { has(id: string): boolean }): string {
	for (let i = 0; i < 100; i++) {
		const id = uuidv7().slice(0, 8);
		if (!byId.has(id)) return id;
	}
	return uuidv7();
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null;
}

function invalidSession(message: string, cause?: Error): SessionError {
	return new SessionError("invalid_session", `Invalid SQLite session: ${message}`, cause);
}

function invalidEntry(message: string, cause?: Error): SessionError {
	return new SessionError("invalid_entry", `Invalid SQLite session entry: ${message}`, cause);
}

function parsePayload(row: SessionEntryRow): unknown {
	try {
		return JSON.parse(row.payload);
	} catch (error) {
		throw invalidEntry(`entry ${row.id} payload is not valid JSON`, error instanceof Error ? error : undefined);
	}
}

function entryToPayload<TEntry extends SessionTreeEntry>(entry: TEntry): EntryPayload<TEntry> {
	const { type: _type, id: _id, parentId: _parentId, timestamp: _timestamp, ...payload } = entry;
	return payload as EntryPayload<TEntry>;
}

function encodeEntry(entry: SessionTreeEntry): EncodedEntry {
	switch (entry.type) {
		case "message":
			return { payload: JSON.stringify(entryToPayload(entry)), messageRole: entry.message.role };
		case "custom":
			return { payload: JSON.stringify(entryToPayload(entry)), customType: entry.customType };
		case "custom_message":
			return { payload: JSON.stringify(entryToPayload(entry)), customType: entry.customType };
		case "label":
			return { payload: JSON.stringify(entryToPayload(entry)), targetId: entry.targetId };
		case "leaf":
			return { payload: JSON.stringify(entryToPayload(entry)), targetId: entry.targetId };
		case "thinking_level_change":
		case "model_change":
		case "active_tools_change":
		case "compaction":
		case "branch_summary":
		case "session_info":
			return { payload: JSON.stringify(entryToPayload(entry)) };
		default: {
			const exhaustive: never = entry;
			return exhaustive;
		}
	}
}

function decodeEntry(row: SessionEntryRow): SessionTreeEntry {
	const payload = parsePayload(row);
	if (!isRecord(payload)) throw invalidEntry(`entry ${row.id} payload is not an object`);
	const base = {
		id: row.id,
		parentId: row.parent_id,
		timestamp: row.timestamp,
	};

	switch (row.type) {
		case "message": {
			if (!("message" in payload)) throw invalidEntry(`entry ${row.id} is missing message payload`);
			const messagePayload = payload as MessagePayload;
			return { ...base, type: "message", ...messagePayload };
		}
		case "thinking_level_change":
			if (typeof payload.thinkingLevel !== "string") throw invalidEntry(`entry ${row.id} is missing thinkingLevel`);
			return { ...base, type: "thinking_level_change", ...(payload as ThinkingLevelChangePayload) };
		case "model_change":
			if (typeof payload.provider !== "string" || typeof payload.modelId !== "string") {
				throw invalidEntry(`entry ${row.id} has invalid model_change payload`);
			}
			return { ...base, type: "model_change", ...(payload as ModelChangePayload) };
		case "active_tools_change":
			if (
				!Array.isArray(payload.activeToolNames) ||
				payload.activeToolNames.some((value) => typeof value !== "string")
			) {
				throw invalidEntry(`entry ${row.id} has invalid active_tools_change payload`);
			}
			return { ...base, type: "active_tools_change", ...(payload as ActiveToolsChangePayload) };
		case "compaction":
			if (
				typeof payload.summary !== "string" ||
				typeof payload.firstKeptEntryId !== "string" ||
				typeof payload.tokensBefore !== "number" ||
				(payload.retainedTail !== undefined && !Array.isArray(payload.retainedTail))
			) {
				throw invalidEntry(`entry ${row.id} has invalid compaction payload`);
			}
			return { ...base, type: "compaction", ...(payload as CompactionPayload) };
		case "branch_summary":
			if (typeof payload.fromId !== "string" || typeof payload.summary !== "string") {
				throw invalidEntry(`entry ${row.id} has invalid branch_summary payload`);
			}
			return { ...base, type: "branch_summary", ...(payload as BranchSummaryPayload) };
		case "custom":
			if (typeof payload.customType !== "string") throw invalidEntry(`entry ${row.id} has invalid custom payload`);
			return { ...base, type: "custom", ...(payload as CustomPayload) };
		case "custom_message":
			if (
				typeof payload.customType !== "string" ||
				typeof payload.display !== "boolean" ||
				!("content" in payload)
			) {
				throw invalidEntry(`entry ${row.id} has invalid custom_message payload`);
			}
			return { ...base, type: "custom_message", ...(payload as CustomMessagePayload) };
		case "label":
			if (typeof payload.targetId !== "string") throw invalidEntry(`entry ${row.id} has invalid label payload`);
			if (payload.label !== undefined && typeof payload.label !== "string") {
				throw invalidEntry(`entry ${row.id} has invalid label payload`);
			}
			return { ...base, type: "label", ...(payload as LabelPayload) };
		case "session_info":
			if (payload.name !== undefined && typeof payload.name !== "string") {
				throw invalidEntry(`entry ${row.id} has invalid session_info payload`);
			}
			return { ...base, type: "session_info", ...(payload as SessionInfoPayload) };
		case "leaf":
			if (payload.targetId !== null && typeof payload.targetId !== "string") {
				throw invalidEntry(`entry ${row.id} has invalid leaf payload`);
			}
			return { ...base, type: "leaf", ...(payload as LeafPayload) };
		default:
			throw invalidEntry(`unknown entry type ${row.type}`);
	}
}

function leafIdAfterEntry(entry: SessionTreeEntry): string | null {
	return entry.type === "leaf" ? entry.targetId : entry.id;
}

function rowToMetadata(row: SessionRow, path: string): SqliteSessionMetadata {
	return {
		id: row.id,
		createdAt: row.created_at,
		cwd: row.cwd,
		path,
		parentSessionId: row.parent_session_id ?? undefined,
	};
}

function buildPathToRootOrCompaction(byId: Map<string, SessionTreeEntry>, leafId: string | null): SessionTreeEntry[] {
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

async function loadSqliteStorage(
	db: SqliteDatabase,
	sessionId: string,
): Promise<{
	row: SessionRow;
	entries: SessionTreeEntry[];
	leafId: string | null;
}> {
	const row = await db
		.prepare("SELECT id, created_at, cwd, parent_session_id FROM sessions WHERE id = ?")
		.get<SessionRow>([sessionId]);
	if (!row) throw new SessionError("not_found", `Session not found: ${sessionId}`);

	const entryRows = await db
		.prepare(
			"SELECT seq, session_id, id, parent_id, type, timestamp, payload, target_id, message_role, custom_type, entry_counter FROM session_entries WHERE session_id = ? ORDER BY entry_counter, seq",
		)
		.all<SessionEntryRow>([sessionId]);
	const entries = entryRows.map((entryRow) => decodeEntry(entryRow));
	let leafId: string | null = null;
	for (const entry of entries) {
		leafId = leafIdAfterEntry(entry);
	}
	return { row, entries, leafId };
}

export class SqliteSessionStorage implements SessionStorage<SqliteSessionMetadata>, ClosableSessionStorage {
	private readonly db: SqliteDatabase;
	private readonly metadata: SqliteSessionMetadata;
	private entries: SessionTreeEntry[];
	private byId: Map<string, SessionTreeEntry>;
	private labelsById: Map<string, string>;
	private currentLeafId: string | null;
	private activeBranchId: string | null;

	private constructor(
		db: SqliteDatabase,
		metadata: SqliteSessionMetadata,
		entries: SessionTreeEntry[],
		leafId: string | null,
		activeBranchId: string | null,
	) {
		this.db = db;
		this.metadata = metadata;
		this.entries = entries;
		this.byId = new Map(entries.map((entry) => [entry.id, entry]));
		this.labelsById = buildLabelsById(entries);
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
		);
	}

	static async create(
		db: SqliteDatabase,
		path: string,
		options: { cwd: string; sessionId: string; parentSessionId?: string },
	): Promise<SqliteSessionStorage> {
		const createdAt = new Date().toISOString();
		await db
			.prepare(
				"INSERT INTO sessions (id, created_at, cwd, parent_session_id, storage_version) VALUES (?, ?, ?, ?, 1)",
			)
			.run([options.sessionId, createdAt, options.cwd, options.parentSessionId ?? null]);
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
		const path = buildPathToRootOrCompaction(this.byId, leafId);
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
		try {
			await this.db.transaction(async () => {
				const entryCounter = this.entries.length + 1;
				await this.db
					.prepare(
						"INSERT INTO session_entries (session_id, id, parent_id, type, timestamp, payload, target_id, message_role, custom_type, entry_counter) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
					)
					.run([
						this.metadata.id,
						entry.id,
						entry.parentId,
						entry.type,
						entry.timestamp,
						encoded.payload,
						encoded.targetId ?? null,
						encoded.messageRole ?? null,
						encoded.customType ?? null,
						entryCounter,
					]);
				this.entries.push(entry);
				this.byId.set(entry.id, entry);
				updateLabelCache(this.labelsById, entry);
				this.currentLeafId = leafIdAfterEntry(entry);
				if (entry.type === "leaf") {
					await this.materializeBranch(entry.targetId);
				} else {
					await this.appendToActiveBranch(entry.id, entry.parentId);
				}
			});
		} catch (error) {
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

	private async getMaterializedBranchPath(branchId: string): Promise<SessionTreeEntry[]> {
		const rows = await this.db
			.prepare(
				"SELECT e.id AS entry_id FROM session_entries e WHERE e.session_id = ? AND EXISTS (SELECT 1 FROM branch_entries b WHERE b.session_id = e.session_id AND b.branch_id = ? AND b.entry_id = e.id) ORDER BY e.entry_counter, e.seq",
			)
			.all<BranchEntryIdRow>([this.metadata.id, branchId]);
		return rows.map((row) => {
			const entry = this.byId.get(row.entry_id);
			if (!entry) throw invalidSession(`Entry ${row.entry_id} not found for branch ${branchId}`);
			return entry;
		});
	}

	async getPathToRootOrCompaction(leafId: string | null): Promise<SessionTreeEntry[]> {
		if (leafId === null) return [];
		if (leafId === this.currentLeafId && this.activeBranchId) {
			return this.getMaterializedBranchPath(this.activeBranchId);
		}
		return buildPathToRootOrCompaction(this.byId, leafId);
	}

	async getEntries(): Promise<SessionTreeEntry[]> {
		return [...this.entries];
	}

	async cleanup(): Promise<void> {
		await this.db.close();
	}
}
