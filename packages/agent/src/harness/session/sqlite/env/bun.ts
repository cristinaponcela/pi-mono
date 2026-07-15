// @ts-expect-error Bun runtime builtin module is only available when running on Bun.
import { Database } from "bun:sqlite";
import { mkdir } from "node:fs/promises";
import { resolve } from "node:path";
import { NodeExecutionEnv } from "../../../env/nodejs.ts";
import type { SqliteDatabase, SqliteRunResult, SqliteStatement } from "../types.ts";

function getNumberField(record: Record<string, unknown>, key: string): number | undefined {
	const value = record[key];
	return typeof value === "number" ? value : undefined;
}

function toRunResult(result: unknown): SqliteRunResult {
	if (typeof result !== "object" || result === null) {
		return { changes: 0 };
	}
	const record = result as Record<string, unknown>;
	return {
		changes: getNumberField(record, "changes") ?? 0,
		lastInsertRowid: getNumberField(record, "lastInsertRowid"),
	};
}

class BunSqliteStatement implements SqliteStatement {
	private readonly statement: ReturnType<Database["prototype"]["query"]>;

	constructor(statement: ReturnType<Database["prototype"]["query"]>) {
		this.statement = statement;
	}

	async run(params?: unknown[]): Promise<SqliteRunResult> {
		return toRunResult(params ? this.statement.run(...params) : this.statement.run());
	}

	async get<TRow extends object>(params?: unknown[]): Promise<TRow | undefined> {
		return (params ? this.statement.get(...params) : this.statement.get()) as TRow | undefined;
	}

	async all<TRow extends object>(params?: unknown[]): Promise<TRow[]> {
		return (params ? this.statement.all(...params) : this.statement.all()) as TRow[];
	}
}

class BunSqliteDatabase implements SqliteDatabase {
	private readonly db: Database;

	constructor(db: Database) {
		this.db = db;
	}

	async exec(sql: string): Promise<void> {
		this.db.run(sql);
	}

	prepare(sql: string): SqliteStatement {
		return new BunSqliteStatement(this.db.query(sql));
	}

	async transaction<T>(fn: () => Promise<T>): Promise<T> {
		this.db.run("BEGIN");
		try {
			const result = await fn();
			this.db.run("COMMIT");
			return result;
		} catch (error) {
			try {
				this.db.run("ROLLBACK");
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

export class SqliteBunExecutionEnv extends NodeExecutionEnv {
	async openSqlite(path: string): Promise<SqliteDatabase> {
		const resolved = resolvePath(this.cwd, path);
		await mkdir(resolve(resolved, ".."), { recursive: true });
		return new BunSqliteDatabase(new Database(resolved));
	}
}
