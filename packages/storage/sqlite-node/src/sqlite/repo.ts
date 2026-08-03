import type { FileError, FileSystem, Result } from "@earendil-works/pi-agent-core";
import {
	type BranchBounds,
	type Entry,
	type EntryOrder,
	type EntryQuery,
	type ForkOptions,
	type LaneRecord,
	type LogItem,
	type LogOptions,
	type NewRecord,
	type ProvisionedEntry,
	type RecordQuery,
	Session,
	type SessionCreateOptions,
	SessionError,
	type SessionMetadata,
	type SessionRepo as SessionRepository,
	type SessionStats,
	type SessionStorage,
} from "@earendil-works/pi-agent-core/experimental";
import { uuidv7 } from "@earendil-works/pi-ai";
import { applyMigrations } from "./migrations.ts";
import { appendEntryToBranchCache, queryCachedBranchRows, readCachedBranch } from "./storage/branch-cache.ts";
import { appendFact, readLatestFact } from "./storage/facts.ts";
import {
	createInitialLane,
	createLane as insertLane,
	readLaneHead,
	readLanes,
	setLaneLeaf,
	moveLane as updateLane,
} from "./storage/lanes.ts";
import { appendRecordRow, readRecordRows } from "./storage/records.ts";
import { advanceSequence, getNextSequence } from "./storage/session-sequences.ts";
import { rowToMetadata, type SessionRow } from "./storage/sessions.ts";
import type { SqliteDatabase, SqliteDatabaseFactory } from "./types.ts";

export interface SqliteSessionMetadata extends SessionMetadata {
	cwd: string;
	path: string;
	metadata?: Record<string, unknown>;
}

export interface SqliteSessionCreateOptions extends SessionCreateOptions {
	cwd: string;
	metadata?: Record<string, unknown>;
}

export interface SqliteSessionListOptions {
	cwd?: string;
}

export type SqliteSessionRepositoryEnv = Pick<FileSystem, "absolutePath" | "createDir" | "exists">;

export interface SqliteSessionRepositoryOptions {
	env: SqliteSessionRepositoryEnv;
	sqlite: SqliteDatabaseFactory;
	databasePath: string;
}

interface EntryRow {
	session_id: string;
	seq: number;
	id: string;
	parent_id: string | null;
	type: Entry["type"];
	timestamp: string;
	payload: string;
}

interface FactRow {
	seq: number;
	kind: string;
	key: string | null;
	value: string | null;
}

interface LaneMoveRow {
	seq: number;
	lane: string;
	leaf_id: string | null;
}

class SerialOperationQueue {
	private tail: Promise<void> = Promise.resolve();

	enqueue<T>(operation: () => Promise<T> | T): Promise<T> {
		const result = this.tail.then(operation);
		this.tail = result.then(
			() => undefined,
			() => undefined,
		);
		return result;
	}

	async drain(): Promise<void> {
		await this.tail;
	}
}

function resultOrThrow<T>(result: Result<T, FileError>, message: string): T {
	if (!result.ok) {
		const code = result.error.code === "not_found" ? "not_found" : "storage";
		throw new SessionError(code, `${message}: ${result.error.message}`, result.error);
	}
	return result.value;
}

function getParentPath(path: string): string {
	const normalized = path.replace(/[\\/]+$/, "");
	const lastSlash = Math.max(normalized.lastIndexOf("/"), normalized.lastIndexOf("\\"));
	if (lastSlash < 0) return ".";
	if (lastSlash === 0) return normalized.slice(0, 1);
	return normalized.slice(0, lastSlash);
}

async function configureSqliteDatabase(db: SqliteDatabase): Promise<void> {
	await db.exec("PRAGMA journal_mode=WAL");
	await db.exec("PRAGMA synchronous=FULL");
	await db.exec("PRAGMA busy_timeout=5000");
}

function timestampToText(timestamp: number): string {
	return new Date(timestamp).toISOString();
}

function timestampFromText(timestamp: string): number {
	return Date.parse(timestamp);
}

function entryPayload(entry: Entry): Record<string, unknown> {
	const { type: _type, id: _id, seq: _seq, parentId: _parentId, timestamp: _timestamp, ...payload } = entry;
	return payload;
}

function decodeEntry(row: EntryRow): Entry {
	const payload = JSON.parse(row.payload) as Record<string, unknown>;
	return {
		...payload,
		type: row.type,
		id: row.id,
		seq: row.seq,
		parentId: row.parent_id,
		timestamp: timestampFromText(row.timestamp),
	} as Entry;
}

function recordRunId(record: NewRecord): string | undefined {
	return record.type === "operation_started" ? record.id : "runId" in record ? record.runId : undefined;
}

function recordOpKind(record: NewRecord): string | undefined {
	return record.type === "operation_started" ? record.intent.kind : undefined;
}

function decodeRecord(row: { seq: number; timestamp: string; payload: string }): LaneRecord {
	return {
		...(JSON.parse(row.payload) as object),
		seq: row.seq,
		timestamp: timestampFromText(row.timestamp),
	} as LaneRecord;
}

function matchesEntryQuery(entry: Entry, query: EntryQuery): boolean {
	return (
		(query.type === undefined || entry.type === query.type) &&
		(query.customType === undefined || (entry.type === "custom" && entry.customType === query.customType)) &&
		(query.cursor === undefined ||
			(query.order === "oldestFirst" ? entry.seq > query.cursor.afterSeq : entry.seq < query.cursor.afterSeq))
	);
}

function orderedSql(order: EntryOrder | undefined): string {
	return order === "oldestFirst" ? "ASC" : "DESC";
}

function usageFrom(
	value: unknown,
): { input: number; output: number; cacheRead: number; cacheWrite: number; costTotal: number } | undefined {
	if (typeof value !== "object" || value === null || !("cost" in value)) return undefined;
	const usage = value as {
		input?: unknown;
		output?: unknown;
		cacheRead?: unknown;
		cacheWrite?: unknown;
		cost?: { total?: unknown };
	};
	if (
		typeof usage.input !== "number" ||
		typeof usage.output !== "number" ||
		typeof usage.cacheRead !== "number" ||
		typeof usage.cacheWrite !== "number" ||
		typeof usage.cost?.total !== "number"
	) {
		return undefined;
	}
	return {
		input: usage.input,
		output: usage.output,
		cacheRead: usage.cacheRead,
		cacheWrite: usage.cacheWrite,
		costTotal: usage.cost.total,
	};
}

function addUsage(stats: SessionStats, usage: NonNullable<ReturnType<typeof usageFrom>>): void {
	stats.cachedTokens += usage.cacheRead;
	stats.uncachedTokens += usage.input + usage.cacheWrite;
	stats.totalTokens += usage.input + usage.output + usage.cacheRead + usage.cacheWrite;
	stats.costTotal += usage.costTotal;
}

function applyEntryStats(stats: SessionStats, entry: Entry): void {
	if (entry.type === "message") {
		stats.messageCount += 1;
		if (entry.message.role === "assistant") {
			const usage = usageFrom(entry.message.usage);
			if (usage) addUsage(stats, usage);
		}
		return;
	}
	if (entry.type === "compaction" || entry.type === "branch_summary") {
		const usage = usageFrom(entry.usage);
		if (usage) addUsage(stats, usage);
	}
}

function emptyStats(): SessionStats {
	return { messageCount: 0, cachedTokens: 0, uncachedTokens: 0, totalTokens: 0, costTotal: 0 };
}

function parseStats(payload: string): SessionStats {
	return JSON.parse(payload) as SessionStats;
}

class SqliteSessionStorage implements SessionStorage<SqliteSessionMetadata> {
	private readonly db: SqliteDatabase;
	private readonly metadata: SqliteSessionMetadata;
	private stats: SessionStats;

	constructor(db: SqliteDatabase, metadata: SqliteSessionMetadata, stats: SessionStats) {
		this.db = db;
		this.metadata = metadata;
		this.stats = stats;
	}

	async getMetadata(): Promise<SqliteSessionMetadata> {
		return structuredClone(this.metadata);
	}

	async getLanes(): Promise<{ lane: string; leafId: string | null }[]> {
		return (await readLanes(this.db, this.metadata.id)).map((row) => ({ lane: row.lane, leafId: row.leaf_id }));
	}

	async createLane(lane: string, at: string | null): Promise<void> {
		if (at !== null && !(await this.getEntry(at))) throw new SessionError("not_found", `Entry not found: ${at}`);
		await this.db.transaction(async () => {
			const seq = await getNextSequence(this.db, this.metadata.id);
			await insertLane(this.db, this.metadata.id, seq, lane, at);
			await advanceSequence(this.db, this.metadata.id, seq);
		});
	}

	async moveLane(lane: string, to: string | null): Promise<void> {
		if (to !== null && !(await this.getEntry(to))) throw new SessionError("not_found", `Entry not found: ${to}`);
		await this.db.transaction(async () => {
			const seq = await getNextSequence(this.db, this.metadata.id);
			await updateLane(this.db, this.metadata.id, seq, lane, to);
			await advanceSequence(this.db, this.metadata.id, seq);
		});
	}

	async appendEntry<TEntry extends Entry>(entry: ProvisionedEntry<TEntry>, lane: string): Promise<TEntry> {
		let committed: Entry | undefined;
		await this.db.transaction(async () => {
			const parentId = (await readLaneHead(this.db, this.metadata.id, lane)).leafId;
			const seq = await getNextSequence(this.db, this.metadata.id);
			committed = { ...entry, parentId, seq, timestamp: Date.now() } as Entry;
			await this.db
				.prepare(
					"INSERT INTO entries (session_id, id, seq, parent_id, type, timestamp, payload) VALUES (?, ?, ?, ?, ?, ?, ?)",
				)
				.run(
					this.metadata.id,
					committed.id,
					seq,
					committed.parentId,
					committed.type,
					timestampToText(committed.timestamp),
					JSON.stringify(entryPayload(committed)),
				);
			await setLaneLeaf(this.db, this.metadata.id, lane, committed.id);
			await appendEntryToBranchCache(
				this.db,
				this.metadata.id,
				committed.id,
				seq,
				committed.type,
				committed.type === "custom" ? committed.customType : null,
				committed.parentId,
			);
			const nextStats = structuredClone(this.stats);
			applyEntryStats(nextStats, committed);
			await this.db
				.prepare("UPDATE session_materialized SET payload = ? WHERE session_id = ?")
				.run(JSON.stringify(nextStats), this.metadata.id);
			await advanceSequence(this.db, this.metadata.id, seq);
			this.stats = nextStats;
		});
		return structuredClone(committed as TEntry);
	}

	async appendRecord<TRecord extends LaneRecord>(record: NewRecord<TRecord>): Promise<TRecord> {
		let committed: TRecord | undefined;
		await this.db.transaction(async () => {
			const seq = await getNextSequence(this.db, this.metadata.id);
			committed = { ...record, seq, timestamp: Date.now() } as unknown as TRecord;
			await appendRecordRow(this.db, this.metadata.id, {
				seq,
				id: record.id,
				lane: record.lane,
				runId: recordRunId(record),
				type: record.type,
				opKind: recordOpKind(record),
				timestamp: timestampToText(committed.timestamp),
				payload: JSON.stringify(record),
			});
			await advanceSequence(this.db, this.metadata.id, seq);
		});
		return structuredClone(committed as TRecord);
	}

	async getEntry(id: string): Promise<Entry | undefined> {
		const row = await this.db
			.prepare(
				"SELECT session_id, seq, id, parent_id, type, timestamp, payload FROM entries WHERE session_id = ? AND id = ?",
			)
			.get<EntryRow>(this.metadata.id, id);
		return row ? decodeEntry(row) : undefined;
	}

	async findEntries(query: EntryQuery = {}): Promise<Entry[]> {
		const rows = await this.db
			.prepare(
				`SELECT session_id, seq, id, parent_id, type, timestamp, payload
				FROM entries
				WHERE session_id = ?
				ORDER BY seq ${orderedSql(query.order)}`,
			)
			.all<EntryRow>(this.metadata.id);
		const entries = rows.map(decodeEntry).filter((entry) => matchesEntryQuery(entry, query));
		return structuredClone(query.limit === undefined ? entries : entries.slice(0, query.limit));
	}

	async findEntriesOnBranch(query: EntryQuery & BranchBounds & { start: string }): Promise<Entry[]> {
		const cached = await readCachedBranch(this.db, this.metadata.id, query.start);
		if (!cached) throw new SessionError("invalid_entry", `Branch cache missing entry ${query.start}`);
		const rows = await queryCachedBranchRows(this.db, this.metadata.id, cached, query);
		const entries = (rows as unknown as EntryRow[])
			.map(decodeEntry)
			.filter((entry) => matchesEntryQuery(entry, query));
		return structuredClone(query.limit === undefined ? entries : entries.slice(0, query.limit));
	}

	async findRecords(query: RecordQuery = {}): Promise<LaneRecord[]> {
		const rows = await readRecordRows(this.db, this.metadata.id, query);
		return structuredClone(rows.map(decodeRecord));
	}

	async getLog(options: LogOptions = {}): Promise<LogItem[]> {
		const afterSeq = options.afterSeq ?? 0;
		const entryRows = await this.db
			.prepare(
				`SELECT session_id, seq, id, parent_id, type, timestamp, payload
				FROM entries
				WHERE session_id = ? AND seq > ?`,
			)
			.all<EntryRow>(this.metadata.id, afterSeq);
		const recordRows = await readRecordRows(this.db, this.metadata.id, { afterSeq });
		const laneRows = await this.db
			.prepare("SELECT seq, lane, leaf_id FROM lane_moves WHERE session_id = ? AND seq > ? ORDER BY seq")
			.all<LaneMoveRow>(this.metadata.id, afterSeq);
		const factRows = await this.db
			.prepare("SELECT seq, kind, key, value FROM facts WHERE session_id = ? AND seq > ? ORDER BY seq")
			.all<FactRow>(this.metadata.id, afterSeq);

		const log: LogItem[] = [
			...entryRows.map((row) => ({ kind: "entry" as const, seq: row.seq, entry: decodeEntry(row) })),
			...recordRows.map((row) => ({ kind: "record" as const, seq: row.seq, record: decodeRecord(row) })),
			...laneRows.map((row) => ({ kind: "lane" as const, seq: row.seq, lane: row.lane, leafId: row.leaf_id })),
			...factRows.map((row) => {
				if (row.kind === "name")
					return {
						kind: "fact" as const,
						seq: row.seq,
						fact: "name" as const,
						name: JSON.parse(row.value ?? "null") as string,
					};
				return {
					kind: "fact" as const,
					seq: row.seq,
					fact: "label" as const,
					targetId: row.key ?? "",
					label: row.value === null ? undefined : (JSON.parse(row.value) as string),
				};
			}),
		].sort((left, right) => left.seq - right.seq);
		return structuredClone(options.limit === undefined ? log : log.slice(0, options.limit));
	}

	async getName(): Promise<string | undefined> {
		const row = await readLatestFact(this.db, this.metadata.id, "name", null);
		return row?.value === undefined || row.value === null ? undefined : (JSON.parse(row.value) as string);
	}

	async setName(name: string): Promise<void> {
		await this.db.transaction(async () => {
			const seq = await getNextSequence(this.db, this.metadata.id);
			await appendFact(this.db, this.metadata.id, seq, "name", null, JSON.stringify(name));
			await advanceSequence(this.db, this.metadata.id, seq);
		});
	}

	async getLabel(id: string): Promise<string | undefined> {
		const row = await readLatestFact(this.db, this.metadata.id, "label", id);
		return row?.value === undefined || row.value === null ? undefined : (JSON.parse(row.value) as string);
	}

	async setLabel(id: string, label: string | undefined): Promise<void> {
		if (!(await this.getEntry(id))) throw new SessionError("not_found", `Entry not found: ${id}`);
		await this.db.transaction(async () => {
			const seq = await getNextSequence(this.db, this.metadata.id);
			await appendFact(
				this.db,
				this.metadata.id,
				seq,
				"label",
				id,
				label === undefined ? null : JSON.stringify(label),
			);
			await advanceSequence(this.db, this.metadata.id, seq);
		});
	}

	async getStats(): Promise<SessionStats> {
		return structuredClone(this.stats);
	}
}

async function loadStorage(db: SqliteDatabase, metadata: SqliteSessionMetadata): Promise<SqliteSessionStorage> {
	const row = await db
		.prepare("SELECT id, created_at, metadata, cwd, parent_session_id FROM sessions WHERE id = ?")
		.get<SessionRow>(metadata.id);
	if (!row) throw new SessionError("not_found", `Session not found: ${metadata.id}`);
	const materialized = await db
		.prepare("SELECT payload FROM session_materialized WHERE session_id = ?")
		.get<{ payload: string }>(metadata.id);
	if (!materialized) throw new SessionError("storage", `Missing materialized stats for session ${metadata.id}`);
	return new SqliteSessionStorage(db, metadataFromRow(row, metadata.path), parseStats(materialized.payload));
}

function metadataFromRow(row: SessionRow, path: string): SqliteSessionMetadata {
	const base = rowToMetadata(row, path);
	return { ...base, createdAt: Date.parse(base.createdAt) };
}

export class SqliteSessionRepository
	implements SessionRepository<SqliteSessionMetadata, SqliteSessionCreateOptions, SqliteSessionListOptions>
{
	private databasePath: string | undefined;
	private database: SqliteDatabase | undefined;
	private databasePromise: Promise<SqliteDatabase> | undefined;
	private readonly operations = new SerialOperationQueue();

	private readonly options: SqliteSessionRepositoryOptions;

	constructor(options: SqliteSessionRepositoryOptions) {
		this.options = options;
	}

	async create(options: SqliteSessionCreateOptions): Promise<Session<SqliteSessionMetadata>> {
		return this.operations.enqueue(async () => {
			const db = await this.getDatabase();
			const path = await this.getDatabasePath();
			const id = options.id ?? uuidv7();
			const createdAt = Date.now();
			await db.transaction(async () => {
				await db
					.prepare(
						"INSERT INTO sessions (id, created_at, metadata, cwd, parent_session_id) VALUES (?, ?, ?, ?, ?)",
					)
					.run(
						id,
						timestampToText(createdAt),
						options.metadata === undefined ? null : JSON.stringify(options.metadata),
						options.cwd,
						options.parentSessionId ?? null,
					);
				await db.prepare("INSERT INTO session_sequences (session_id, next_seq) VALUES (?, ?)").run(id, 1);
				await db
					.prepare("INSERT INTO session_materialized (session_id, payload) VALUES (?, ?)")
					.run(id, JSON.stringify(emptyStats()));
				await createInitialLane(db, id);
			});
			return new Session(
				await loadStorage(db, {
					id,
					createdAt,
					cwd: options.cwd,
					path,
					parentSessionId: options.parentSessionId,
					metadata: options.metadata,
				}),
			);
		});
	}

	async open(metadata: SqliteSessionMetadata): Promise<Session<SqliteSessionMetadata>> {
		return new Session(await this.operations.enqueue(async () => loadStorage(await this.getDatabase(), metadata)));
	}

	async list(options: SqliteSessionListOptions = {}): Promise<SqliteSessionMetadata[]> {
		return this.operations.enqueue(async () => {
			const path = await this.getDatabasePath();
			if (!resultOrThrow(await this.options.env.exists(path), `Failed to check database ${path}`)) return [];
			const db = await this.getDatabase();
			const rows = options.cwd
				? await db
						.prepare(
							"SELECT id, created_at, metadata, cwd, parent_session_id FROM sessions WHERE cwd = ? ORDER BY created_at DESC",
						)
						.all<SessionRow>(options.cwd)
				: await db
						.prepare(
							"SELECT id, created_at, metadata, cwd, parent_session_id FROM sessions ORDER BY created_at DESC",
						)
						.all<SessionRow>();
			return rows.map((row) => metadataFromRow(row, path));
		});
	}

	async delete(metadata: SqliteSessionMetadata): Promise<void> {
		return this.operations.enqueue(async () => {
			const db = await this.getDatabase();
			await db.transaction(async () => {
				for (const table of [
					"branch_tips",
					"branch_entries",
					"facts",
					"lane_moves",
					"records",
					"entries",
					"lanes",
					"leases",
					"session_materialized",
					"session_sequences",
				]) {
					await db.prepare(`DELETE FROM ${table} WHERE session_id = ?`).run(metadata.id);
				}
				await db.prepare("DELETE FROM sessions WHERE id = ?").run(metadata.id);
			});
		});
	}

	async fork(
		source: SqliteSessionMetadata,
		options: ForkOptions & SqliteSessionCreateOptions,
	): Promise<Session<SqliteSessionMetadata>> {
		const sourceSession = await this.open(source);
		const fork = await this.create({ ...options, parentSessionId: options.parentSessionId ?? source.id });
		const targetId =
			options.scope === "tree" || options.entryId === undefined
				? await sourceSession.getLeafId()
				: options.position === "at"
					? options.entryId
					: (await sourceSession.getEntry(options.entryId))?.parentId;
		const entries =
			targetId === null || targetId === undefined
				? []
				: await sourceSession.findEntriesOnBranch({ start: targetId, order: "oldestFirst" });
		for (const entry of entries) {
			const { parentId: _parentId, seq: _seq, timestamp: _timestamp, ...provisioned } = entry;
			await fork.appendEntry(provisioned as ProvisionedEntry, "main");
		}
		return fork;
	}

	async close(): Promise<void> {
		await this.operations.drain();
		if (this.database) await this.database.close();
		this.database = undefined;
		this.databasePromise = undefined;
	}

	async [Symbol.asyncDispose](): Promise<void> {
		await this.close();
	}

	private async getDatabasePath(): Promise<string> {
		this.databasePath ??= resultOrThrow(
			await this.options.env.absolutePath(this.options.databasePath),
			`Failed to resolve SQLite sessions database ${this.options.databasePath}`,
		);
		return this.databasePath;
	}

	private async getDatabase(): Promise<SqliteDatabase> {
		if (!this.databasePromise) this.databasePromise = this.openDatabase();
		this.database = await this.databasePromise;
		return this.database;
	}

	private async openDatabase(): Promise<SqliteDatabase> {
		const path = await this.getDatabasePath();
		resultOrThrow(
			await this.options.env.createDir(getParentPath(path), { recursive: true }),
			`Failed to create SQLite sessions directory ${path}`,
		);
		const db = await this.options.sqlite.open(path);
		try {
			await configureSqliteDatabase(db);
			await applyMigrations(db);
			return db;
		} catch (error) {
			await db.close();
			throw error;
		}
	}
}
