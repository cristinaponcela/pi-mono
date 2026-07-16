import type { SQLInputValue } from "node:sqlite";
import { DatabaseSync } from "node:sqlite";
import type {
	SqliteDatabase,
	SqliteDatabaseFactory,
	SqliteRunResult,
	SqliteStatement,
} from "@earendil-works/pi-agent-core/sqlite";

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

export function wrapNodeSqliteDatabase(db: DatabaseSync): SqliteDatabase {
	return new NodeSqliteDatabase(db);
}

export function createNodeSqliteFactory(): SqliteDatabaseFactory {
	return {
		async open(path: string): Promise<SqliteDatabase> {
			return new NodeSqliteDatabase(new DatabaseSync(path));
		},
	};
}
