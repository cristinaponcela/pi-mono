import { mkdir } from "node:fs/promises";
import { createRequire } from "node:module";
import { resolve } from "node:path";
import { NodeExecutionEnv } from "../../../env/nodejs.ts";
import type { SqliteDatabase, SqliteRunResult, SqliteStatement } from "../types.ts";

const require = createRequire(import.meta.url);

type BetterSqlite3Module = {
	new (path: string): BetterSqlite3DatabaseHandle;
};

type BetterSqlite3StatementHandle = {
	run(...params: unknown[]): { changes: number; lastInsertRowid: number | bigint };
	get(...params: unknown[]): unknown;
	all(...params: unknown[]): unknown[];
};

type BetterSqlite3DatabaseHandle = {
	exec(sql: string): void;
	prepare(sql: string): BetterSqlite3StatementHandle;
	close(): void;
};

function loadBetterSqlite3(): BetterSqlite3Module {
	return require("better-sqlite3") as BetterSqlite3Module;
}

class BetterSqlite3Statement implements SqliteStatement {
	private readonly statement: BetterSqlite3StatementHandle;

	constructor(statement: BetterSqlite3StatementHandle) {
		this.statement = statement;
	}

	async run(params?: unknown[]): Promise<SqliteRunResult> {
		const result = params ? this.statement.run(...params) : this.statement.run();
		return {
			changes: result.changes,
			lastInsertRowid:
				typeof result.lastInsertRowid === "bigint" ? Number(result.lastInsertRowid) : result.lastInsertRowid,
		};
	}

	async get<TRow extends object>(params?: unknown[]): Promise<TRow | undefined> {
		return (params ? this.statement.get(...params) : this.statement.get()) as TRow | undefined;
	}

	async all<TRow extends object>(params?: unknown[]): Promise<TRow[]> {
		return (params ? this.statement.all(...params) : this.statement.all()) as TRow[];
	}
}

class BetterSqlite3Database implements SqliteDatabase {
	private readonly db: BetterSqlite3DatabaseHandle;

	constructor(db: BetterSqlite3DatabaseHandle) {
		this.db = db;
	}

	async exec(sql: string): Promise<void> {
		this.db.exec(sql);
	}

	prepare(sql: string): SqliteStatement {
		return new BetterSqlite3Statement(this.db.prepare(sql));
	}

	async transaction<T>(fn: () => Promise<T>): Promise<T> {
		this.db.exec("BEGIN");
		try {
			const result = await fn();
			this.db.exec("COMMIT");
			return result;
		} catch (error) {
			try {
				this.db.exec("ROLLBACK");
			} catch {
				// Ignore rollback errors to rethrow original error.
			}
			throw error;
		}
	}

	async close(): Promise<void> {
		this.db.close();
	}
}

function resolvePath(cwd: string, path: string): string {
	return resolve(cwd, path);
}

export class SqliteBetterSqlite3ExecutionEnv extends NodeExecutionEnv {
	async openSqlite(path: string): Promise<SqliteDatabase> {
		const resolved = resolvePath(this.cwd, path);
		await mkdir(resolve(resolved, ".."), { recursive: true });
		const BetterSqlite3 = loadBetterSqlite3();
		return new BetterSqlite3Database(new BetterSqlite3(resolved));
	}
}
