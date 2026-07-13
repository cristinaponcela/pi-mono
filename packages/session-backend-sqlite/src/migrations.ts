import { readdir, readFile } from "node:fs/promises";
import { basename } from "node:path";
import { fileURLToPath } from "node:url";
import { SessionError } from "@earendil-works/pi-agent-core";
import type { SqliteDatabase } from "./types.ts";

const MIGRATIONS_DIR_URL = new URL("../sql/migrations/", import.meta.url);
const MIGRATION_FILE_PATTERN = /^(\d+)_([a-z0-9_]+)\.sql$/;

export interface SqliteMigration {
	id: string;
	order: number;
	sql: string;
}

function parseMigrationFilename(filename: string): { order: number } | undefined {
	const match = MIGRATION_FILE_PATTERN.exec(filename);
	if (!match) return undefined;
	return { order: Number(match[1]) };
}

async function ensureMigrationsTable(db: SqliteDatabase): Promise<void> {
	await db.exec(`
CREATE TABLE IF NOT EXISTS migrations (
	id TEXT PRIMARY KEY,
	applied_at TEXT NOT NULL
);
`);
}

export async function loadMigrations(): Promise<SqliteMigration[]> {
	let entries: string[];
	try {
		entries = await readdir(fileURLToPath(MIGRATIONS_DIR_URL));
	} catch (error) {
		throw new SessionError(
			"storage",
			`Failed to read SQLite migrations from ${fileURLToPath(MIGRATIONS_DIR_URL)}`,
			error instanceof Error ? error : undefined,
		);
	}

	const migrations: SqliteMigration[] = [];
	for (const filename of entries) {
		const parsed = parseMigrationFilename(filename);
		if (!parsed) continue;
		const url = new URL(filename, MIGRATIONS_DIR_URL);
		let sql: string;
		try {
			sql = await readFile(url, "utf8");
		} catch (error) {
			throw new SessionError(
				"storage",
				`Failed to read SQLite migration ${basename(filename)}`,
				error instanceof Error ? error : undefined,
			);
		}
		migrations.push({ id: filename, order: parsed.order, sql });
	}

	migrations.sort((left, right) => left.order - right.order || left.id.localeCompare(right.id));
	return migrations;
}

export async function applyMigrations(db: SqliteDatabase): Promise<void> {
	await ensureMigrationsTable(db);
	const migrations = await loadMigrations();
	const appliedRows = await db.prepare("SELECT id FROM migrations ORDER BY applied_at, id").all<{ id: string }>();
	const applied = new Set(appliedRows.map((row) => row.id));

	for (const migration of migrations) {
		if (applied.has(migration.id)) continue;
		await db.transaction(async () => {
			await db.exec(migration.sql);
			await db
				.prepare("INSERT INTO migrations (id, applied_at) VALUES (?, ?)")
				.run([migration.id, new Date().toISOString()]);
		});
		applied.add(migration.id);
	}
}

export async function getCurrentMigrationId(db: SqliteDatabase): Promise<string | undefined> {
	await ensureMigrationsTable(db);
	const row = await db
		.prepare("SELECT id FROM migrations ORDER BY applied_at DESC, id DESC LIMIT 1")
		.get<{ id: string }>();
	return row?.id;
}
