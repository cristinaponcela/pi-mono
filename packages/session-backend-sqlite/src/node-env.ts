import { mkdir } from "node:fs/promises";
import { resolve } from "node:path";
import type { SQLInputValue } from "node:sqlite";
// CPON use npm sqlite and no built in :(((
// bun or other specific, pick and load. and cry.
// static and shared libraries in linux?
// test to load sqlite to bun executable as shared library. problems?
// -> Armin will deal with this :)
import { DatabaseSync } from "node:sqlite";
import { NodeExecutionEnv } from "@earendil-works/pi-agent-core/node";
import type { SqliteDatabase, SqliteRunResult, SqliteStatement } from "./types.ts";

class NodeSqliteStatement implements SqliteStatement {
	private readonly statement: ReturnType<DatabaseSync["prepare"]>;

	constructor(statement: ReturnType<DatabaseSync["prepare"]>) {
		this.statement = statement;
	}

	async run(params?: unknown[]): Promise<SqliteRunResult> {
		const sqliteParams = params as SQLInputValue[] | undefined;
		const result = sqliteParams ? this.statement.run(...sqliteParams) : this.statement.run();
		return {
			changes: Number(result.changes),
			lastInsertRowid: result.lastInsertRowid === undefined ? undefined : Number(result.lastInsertRowid),
		};
	}

	async get<TRow extends object>(params?: unknown[]): Promise<TRow | undefined> {
		const sqliteParams = params as SQLInputValue[] | undefined;
		return (sqliteParams ? this.statement.get(...sqliteParams) : this.statement.get()) as TRow | undefined;
	}

	async all<TRow extends object>(params?: unknown[]): Promise<TRow[]> {
		const sqliteParams = params as SQLInputValue[] | undefined;
		return (sqliteParams ? this.statement.all(...sqliteParams) : this.statement.all()) as TRow[];
	}
}

class NodeSqliteDatabase implements SqliteDatabase {
	private readonly db: DatabaseSync;

	constructor(db: DatabaseSync) {
		this.db = db;
	}

	async exec(sql: string): Promise<void> {
		this.db.exec(sql);
	}

	prepare(sql: string): SqliteStatement {
		return new NodeSqliteStatement(this.db.prepare(sql));
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

export class SqliteNodeExecutionEnv extends NodeExecutionEnv {
	async openSqlite(path: string): Promise<SqliteDatabase> {
		const resolved = resolvePath(this.cwd, path);
		await mkdir(resolve(resolved, ".."), { recursive: true });
		return new NodeSqliteDatabase(new DatabaseSync(resolved));
	}
}
