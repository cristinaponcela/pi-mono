import type {
	LeafEntry,
	SessionEntryCursorOptions,
	SessionSnapshot,
	SessionStorage,
	SessionTreeEntry,
} from "@earendil-works/pi-agent-core";
import {
	createSessionId,
	createSessionRepository,
	getEntriesToFork,
	getFileSystemResultOrThrow,
	SessionError,
	type SessionRepository,
	type SessionStore,
} from "@earendil-works/pi-agent-core";
import { applyMigrations } from "./migrations.ts";
import { createSqliteSessionSearch } from "./search-backend.ts";
import { SqliteSessionStorage } from "./storage/index.ts";
import { rowToMetadata, type SessionRow } from "./storage/sessions.ts";
import type {
	SqliteDatabase,
	SqliteDatabaseFactory,
	SqliteSessionCreateOptions,
	SqliteSessionListOptions,
	SqliteSessionMetadata,
	SqliteSessionStoreEnv,
} from "./types.ts";

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

async function cleanupSessionStorage(storage: SessionStorage): Promise<void> {
	const maybeClosable = storage as SessionStorage & { cleanup?: () => Promise<void> };
	if (typeof maybeClosable.cleanup === "function") await maybeClosable.cleanup();
}

export type SqliteSessionStoreOptions = {
	env: SqliteSessionStoreEnv;
	sqlite: SqliteDatabaseFactory;
	databasePath: string;
};

class SqliteSessionStore
	implements SessionStore<SqliteSessionMetadata, SqliteSessionCreateOptions, SqliteSessionListOptions>
{
	private readonly env: SqliteSessionStoreEnv;
	private readonly sqlite: SqliteDatabaseFactory;
	private readonly databasePathInput: string;
	private databasePath: string | undefined;

	constructor(options: SqliteSessionStoreOptions) {
		this.env = options.env;
		this.sqlite = options.sqlite;
		this.databasePathInput = options.databasePath;
	}

	private async getDatabasePath(): Promise<string> {
		if (!this.databasePath) {
			this.databasePath = getFileSystemResultOrThrow(
				await this.env.absolutePath(this.databasePathInput),
				`Failed to resolve SQLite sessions database ${this.databasePathInput}`,
			);
		}
		return this.databasePath;
	}

	private async ensureDatabaseDir(): Promise<void> {
		const path = await this.getDatabasePath();
		const directory = getParentPath(path);
		getFileSystemResultOrThrow(
			await this.env.createDir(directory, { recursive: true }),
			`Failed to create SQLite sessions directory ${directory}`,
		);
	}

	private async openDatabase(): Promise<SqliteDatabase> {
		await this.ensureDatabaseDir();
		const db = await this.sqlite.open(await this.getDatabasePath());
		try {
			await configureSqliteDatabase(db);
			await applyMigrations(db);
			return db;
		} catch (error) {
			await db.close();
			throw error;
		}
	}

	async create(options: SqliteSessionCreateOptions): Promise<SqliteSessionMetadata> {
		const db = await this.openDatabase();
		try {
			const id = options.id ?? createSessionId();
			const storage = await SqliteSessionStorage.create(db, await this.getDatabasePath(), {
				cwd: options.cwd,
				sessionId: id,
				parentSessionId: options.parentSessionId,
				metadata: options.metadata,
			});
			return await storage.getMetadata();
		} finally {
			await db.close();
		}
	}

	async open(metadata: SqliteSessionMetadata): Promise<SessionStorage<SqliteSessionMetadata>> {
		if (
			!getFileSystemResultOrThrow(await this.env.exists(metadata.path), `Failed to check database ${metadata.path}`)
		) {
			throw new SessionError("not_found", `Session not found: ${metadata.id}`);
		}
		const db = await this.openDatabase();
		try {
			return await SqliteSessionStorage.open(db, metadata);
		} catch (error) {
			await db.close();
			throw error;
		}
	}

	async load(metadata: SqliteSessionMetadata): Promise<SessionSnapshot<SqliteSessionMetadata>> {
		const storage = await this.open(metadata);
		try {
			return {
				metadata: await storage.getMetadata(),
				leafId: await storage.getLeafId(),
				entries: await storage.getEntries(),
			};
		} finally {
			await cleanupSessionStorage(storage);
		}
	}

	async list(options: SqliteSessionListOptions = {}): Promise<SqliteSessionMetadata[]> {
		const path = await this.getDatabasePath();
		if (!getFileSystemResultOrThrow(await this.env.exists(path), `Failed to check database ${path}`)) return [];
		const db = await this.openDatabase();
		try {
			const rows = options.cwd
				? await db
						.prepare(
							"SELECT id, created_at, metadata, cwd, parent_session_id, active_leaf_id FROM sessions WHERE cwd = ? ORDER BY created_at DESC",
						)
						.all<SessionRow>(options.cwd)
				: await db
						.prepare(
							"SELECT id, created_at, metadata, cwd, parent_session_id, active_leaf_id FROM sessions ORDER BY created_at DESC",
						)
						.all<SessionRow>();
			return rows.map((row) => rowToMetadata(row, path));
		} finally {
			await db.close();
		}
	}

	async getEntries(metadata: SqliteSessionMetadata, options?: SessionEntryCursorOptions): Promise<SessionTreeEntry[]> {
		const storage = await this.open(metadata);
		try {
			return await storage.getEntries(options);
		} finally {
			await cleanupSessionStorage(storage);
		}
	}

	async createEntryId(metadata: SqliteSessionMetadata): Promise<string> {
		const storage = await this.open(metadata);
		try {
			return await storage.createEntryId();
		} finally {
			await cleanupSessionStorage(storage);
		}
	}

	async appendEntry(metadata: SqliteSessionMetadata, entry: SessionTreeEntry): Promise<void> {
		const storage = await this.open(metadata);
		try {
			await storage.appendEntry(entry);
		} finally {
			await cleanupSessionStorage(storage);
		}
	}

	async setLeafId(metadata: SqliteSessionMetadata, leafId: string | null): Promise<LeafEntry> {
		const storage = await this.open(metadata);
		try {
			return await storage.setLeafId(leafId);
		} finally {
			await cleanupSessionStorage(storage);
		}
	}

	async delete(metadata: SqliteSessionMetadata): Promise<void> {
		const db = await this.openDatabase();
		try {
			await db.transaction(async () => {
				await db.prepare("DELETE FROM branch_entries WHERE session_id = ?").run(metadata.id);
				await db.prepare("DELETE FROM session_entries WHERE session_id = ?").run(metadata.id);
				await db.prepare("DELETE FROM entry_materialized WHERE session_id = ?").run(metadata.id);
				await db.prepare("DELETE FROM session_materialized WHERE session_id = ?").run(metadata.id);
				await db.prepare("DELETE FROM session_sequences WHERE session_id = ?").run(metadata.id);
				const result = await db.prepare("DELETE FROM sessions WHERE id = ?").run(metadata.id);
				if (result.changes === 0) throw new SessionError("not_found", `Session not found: ${metadata.id}`);
			});
		} finally {
			await db.close();
		}
	}

	async fork(
		sourceMetadata: SqliteSessionMetadata,
		options: SqliteSessionCreateOptions & { entryId?: string; position?: "before" | "at"; id?: string },
	): Promise<SqliteSessionMetadata> {
		const source = await this.open(sourceMetadata);
		let forkedEntries: SessionTreeEntry[];
		try {
			forkedEntries = await getEntriesToFork(source, options);
		} finally {
			await cleanupSessionStorage(source);
		}
		const db = await this.openDatabase();
		try {
			const id = options.id ?? createSessionId();
			const storage = await SqliteSessionStorage.create(db, await this.getDatabasePath(), {
				cwd: options.cwd,
				sessionId: id,
				parentSessionId: options.parentSessionId ?? sourceMetadata.id,
				metadata: options.metadata ?? sourceMetadata.metadata,
			});
			for (const entry of forkedEntries) await storage.appendEntry(entry);
			return await storage.getMetadata();
		} finally {
			await db.close();
		}
	}
}

export function createSqliteSessionStore(
	options: SqliteSessionStoreOptions,
): SessionStore<SqliteSessionMetadata, SqliteSessionCreateOptions, SqliteSessionListOptions> {
	return new SqliteSessionStore(options);
}

export function createSqliteSessionRepository(
	options: SqliteSessionStoreOptions,
): SessionRepository<SqliteSessionMetadata, SqliteSessionCreateOptions, SqliteSessionListOptions> {
	const store = createSqliteSessionStore(options);
	return createSessionRepository({
		store,
		search: createSqliteSessionSearch<SqliteSessionMetadata>({ ...options, mode: "canonical" }),
	});
}
