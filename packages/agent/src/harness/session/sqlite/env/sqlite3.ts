import { mkdir } from "node:fs/promises";
import { createRequire } from "node:module";
import { resolve } from "node:path";
import { NodeExecutionEnv } from "../../../env/nodejs.ts";
import type { SqliteDatabase, SqliteRunResult, SqliteStatement } from "../types.ts";

const require = createRequire(import.meta.url);

type Sqlite3RunResultHandle = {
	lastID: number;
	changes: number;
};

type Sqlite3StatementHandle = {
	run(callback: (this: Sqlite3RunResultHandle, error: Error | null) => void): void;
	run(params: unknown[], callback: (this: Sqlite3RunResultHandle, error: Error | null) => void): void;
	get<TRow extends object>(callback: (error: Error | null, row?: TRow) => void): void;
	get<TRow extends object>(params: unknown[], callback: (error: Error | null, row?: TRow) => void): void;
	all<TRow extends object>(callback: (error: Error | null, rows: TRow[]) => void): void;
	all<TRow extends object>(params: unknown[], callback: (error: Error | null, rows: TRow[]) => void): void;
};

type Sqlite3DatabaseHandle = {
	close(callback: (error: Error | null) => void): void;
	exec(sql: string, callback: (error: Error | null) => void): void;
	prepare(sql: string): Sqlite3StatementHandle;
};

type Sqlite3Module = {
	Database: new (path: string, callback: (error: Error | null) => void) => Sqlite3DatabaseHandle;
};

function loadSqlite3(): Sqlite3Module {
	return require("sqlite3") as Sqlite3Module;
}

function resolvePath(cwd: string, path: string): string {
	return resolve(cwd, path);
}

function openDatabase(path: string): Promise<Sqlite3DatabaseHandle> {
	return new Promise((resolveDatabase, reject) => {
		const { Database } = loadSqlite3();
		const db = new Database(path, (error) => {
			if (error) {
				reject(error);
				return;
			}
			resolveDatabase(db);
		});
	});
}

function closeDatabase(db: Sqlite3DatabaseHandle): Promise<void> {
	return new Promise((resolveClose, reject) => {
		db.close((error) => {
			if (error) {
				reject(error);
				return;
			}
			resolveClose();
		});
	});
}

function execDatabase(db: Sqlite3DatabaseHandle, sql: string): Promise<void> {
	return new Promise((resolveExec, reject) => {
		db.exec(sql, (error) => {
			if (error) {
				reject(error);
				return;
			}
			resolveExec();
		});
	});
}

class Sqlite3Statement implements SqliteStatement {
	private readonly statement: Sqlite3StatementHandle;

	constructor(statement: Sqlite3StatementHandle) {
		this.statement = statement;
	}

	async run(params?: unknown[]): Promise<SqliteRunResult> {
		return new Promise((resolveRun, reject) => {
			const callback = function (this: Sqlite3RunResultHandle, error: Error | null): void {
				if (error) {
					reject(error);
					return;
				}
				resolveRun({
					changes: this.changes,
					lastInsertRowid: this.lastID,
				});
			};
			if (params) {
				this.statement.run(params, callback);
				return;
			}
			this.statement.run(callback);
		});
	}

	async get<TRow extends object>(params?: unknown[]): Promise<TRow | undefined> {
		return new Promise((resolveGet, reject) => {
			const callback = (error: Error | null, row?: TRow): void => {
				if (error) {
					reject(error);
					return;
				}
				resolveGet(row);
			};
			if (params) {
				this.statement.get(params, callback);
				return;
			}
			this.statement.get(callback);
		});
	}

	async all<TRow extends object>(params?: unknown[]): Promise<TRow[]> {
		return new Promise((resolveAll, reject) => {
			const callback = (error: Error | null, rows: TRow[]): void => {
				if (error) {
					reject(error);
					return;
				}
				resolveAll(rows);
			};
			if (params) {
				this.statement.all(params, callback);
				return;
			}
			this.statement.all(callback);
		});
	}
}

class Sqlite3Database implements SqliteDatabase {
	private readonly db: Sqlite3DatabaseHandle;

	constructor(db: Sqlite3DatabaseHandle) {
		this.db = db;
	}

	async exec(sql: string): Promise<void> {
		await execDatabase(this.db, sql);
	}

	prepare(sql: string): SqliteStatement {
		return new Sqlite3Statement(this.db.prepare(sql));
	}

	async transaction<T>(fn: () => Promise<T>): Promise<T> {
		await this.exec("BEGIN");
		try {
			const result = await fn();
			await this.exec("COMMIT");
			return result;
		} catch (error) {
			try {
				await this.exec("ROLLBACK");
			} catch {
				// Ignore rollback errors to rethrow original error.
			}
			throw error;
		}
	}

	async close(): Promise<void> {
		await closeDatabase(this.db);
	}
}

export class SqliteSqlite3ExecutionEnv extends NodeExecutionEnv {
	async openSqlite(path: string): Promise<SqliteDatabase> {
		const resolved = resolvePath(this.cwd, path);
		await mkdir(resolve(resolved, ".."), { recursive: true });
		return new Sqlite3Database(await openDatabase(resolved));
	}
}
