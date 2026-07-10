import { readdir, readFile } from "node:fs/promises";
import { basename } from "node:path";
import { fileURLToPath } from "node:url";
import { SessionError } from "@earendil-works/pi-agent-core";
import type { SqliteDatabase } from "./types.ts";

const MIGRATIONS_DIR_URL = new URL("../sql/migrations/", import.meta.url);
const INITIAL_SCHEMA_VERSION = "0.0.0";
const MIGRATION_FILE_PATTERN = /^(\d+\.\d+\.\d+)-(\d+\.\d+\.\d+|main)\.sql$/;

export interface SqliteMigration {
	id: string;
	source: string;
	target: string;
	sql: string;
}

function parseVersion(version: string): [number, number, number] {
	const match = /^(\d+)\.(\d+)\.(\d+)$/.exec(version);
	if (!match) {
		throw new SessionError("storage", `Invalid SQLite migration version ${version}`);
	}
	return [Number(match[1]), Number(match[2]), Number(match[3])];
}

function compareVersions(left: string, right: string): number {
	if (left === right) return 0;
	if (left === "main") return 1;
	if (right === "main") return -1;
	const leftParts = parseVersion(left);
	const rightParts = parseVersion(right);
	for (let i = 0; i < leftParts.length; i++) {
		const diff = leftParts[i]! - rightParts[i]!;
		if (diff !== 0) return diff;
	}
	return 0;
}

function parseMigrationFilename(filename: string): { source: string; target: string } | undefined {
	const match = MIGRATION_FILE_PATTERN.exec(filename);
	if (!match) return undefined;
	return { source: match[1], target: match[2] };
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
		migrations.push({ id: filename, source: parsed.source, target: parsed.target, sql });
	}

	migrations.sort((left, right) => {
		const sourceDiff = compareVersions(left.source, right.source);
		if (sourceDiff !== 0) return sourceDiff;
		return compareVersions(left.target, right.target);
	});
	return migrations;
}

function getAppliedMigrationTargets(appliedIds: string[]): string[] {
	const targets: string[] = [];
	for (const id of appliedIds) {
		const parsed = parseMigrationFilename(id);
		if (!parsed) {
			throw new SessionError("storage", `Applied SQLite migration ${id} has an invalid filename`);
		}
		targets.push(parsed.target);
	}
	return targets;
}

function getCurrentSchemaVersion(appliedIds: string[]): string {
	const targets = getAppliedMigrationTargets(appliedIds);
	if (targets.length === 0) return INITIAL_SCHEMA_VERSION;
	return targets.sort(compareVersions).at(-1) ?? INITIAL_SCHEMA_VERSION;
}

export async function applyMigrations(db: SqliteDatabase): Promise<void> {
	await ensureMigrationsTable(db);
	const migrations = await loadMigrations();
	const appliedRows = await db.prepare("SELECT id FROM migrations ORDER BY applied_at, id").all<{ id: string }>();
	const appliedIds = appliedRows.map((row) => row.id);
	const applied = new Set(appliedIds);
	let currentVersion = getCurrentSchemaVersion(appliedIds);

	while (true) {
		const nextMigration = migrations.find(
			(migration) => migration.source === currentVersion && !applied.has(migration.id),
		);
		if (!nextMigration) break;
		await db.transaction(async () => {
			await db.exec(nextMigration.sql);
			await db
				.prepare("INSERT INTO migrations (id, applied_at) VALUES (?, ?)")
				.run([nextMigration.id, new Date().toISOString()]);
		});
		applied.add(nextMigration.id);
		currentVersion = nextMigration.target;
	}
}

export async function getCurrentMigrationId(db: SqliteDatabase): Promise<string | undefined> {
	await ensureMigrationsTable(db);
	const row = await db
		.prepare("SELECT id FROM migrations ORDER BY applied_at DESC, id DESC LIMIT 1")
		.get<{ id: string }>();
	return row?.id;
}
