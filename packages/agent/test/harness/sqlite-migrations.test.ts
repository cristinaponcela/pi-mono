import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { SqliteNodeExecutionEnv } from "../../src/harness/session/sqlite/env/node.ts";
import { SqliteSessionRepo } from "../../src/harness/session/sqlite/index.ts";
import { createAssistantMessage, createUserMessage } from "./session-test-utils.ts";

function createTempDir(): string {
	return mkdtempSync(join(tmpdir(), "pi-agent-sqlite-"));
}

describe("SQLite migrations", () => {
	it("applies file-based migrations and records them", async () => {
		const root = createTempDir();
		const databasePath = join(root, "sessions.sqlite");
		const env = new SqliteNodeExecutionEnv({ cwd: root });
		const repo = new SqliteSessionRepo({ env, databasePath });
		await repo.create({ cwd: root, id: "session-1" });

		const db = await env.openSqlite(databasePath);
		try {
			const rows = await db.prepare("SELECT id FROM migrations ORDER BY id").all<{ id: string }>();
			expect(rows.map((row) => row.id)).toEqual(["001_initial.sql"]);
			const tables = await db
				.prepare("SELECT name, sql FROM sqlite_master WHERE type = 'table' ORDER BY name")
				.all<{ name: string; sql: string | null }>();
			expect(tables.map((row) => row.name)).toEqual(
				expect.arrayContaining([
					"migrations",
					"sessions",
					"session_entries",
					"session_sequences",
					"branch_entries",
					"session_materialized",
					"entry_materialized",
				]),
			);
			const sessionColumns = await db.prepare("PRAGMA table_info(sessions)").all<{ name: string }>();
			expect(sessionColumns.map((column) => column.name)).toContain("active_leaf_id");
			for (const tableName of [
				"sessions",
				"session_sequences",
				"branch_entries",
				"session_materialized",
				"entry_materialized",
			]) {
				const table = tables.find((row) => row.name === tableName);
				expect(table?.sql).toContain("WITHOUT ROWID");
			}
		} finally {
			await db.close();
		}
	});

	it("persists session metadata through create, list, open, and fork", async () => {
		const root = createTempDir();
		const databasePath = join(root, "sessions.sqlite");
		const env = new SqliteNodeExecutionEnv({ cwd: root });
		const repo = new SqliteSessionRepo({ env, databasePath });
		const source = await repo.create({
			cwd: root,
			id: "session-1",
			metadata: { profile: "reviewer" },
		});
		const sourceMetadata = await source.getMetadata();
		expect(sourceMetadata.metadata).toEqual({ profile: "reviewer" });
		expect((await repo.list({ cwd: root })).map((listed) => listed.metadata)).toEqual([{ profile: "reviewer" }]);
		expect((await (await repo.open(sourceMetadata)).getMetadata()).metadata).toEqual({ profile: "reviewer" });
		const fork = await repo.fork(sourceMetadata, { cwd: root, id: "session-2" });
		expect((await fork.getMetadata()).metadata).toEqual({ profile: "reviewer" });
		const overridden = await repo.fork(sourceMetadata, {
			cwd: root,
			id: "session-3",
			metadata: { profile: "writer" },
		});
		expect((await overridden.getMetadata()).metadata).toEqual({ profile: "writer" });
	});

	it("materializes active leaf id in sessions transactionally", async () => {
		const root = createTempDir();
		const databasePath = join(root, "sessions.sqlite");
		const env = new SqliteNodeExecutionEnv({ cwd: root });
		const repo = new SqliteSessionRepo({ env, databasePath });
		const session = await repo.create({ cwd: root, id: "session-1" });
		const rootId = await session.appendMessage(createUserMessage("root"));
		const childId = await session.appendMessage(createAssistantMessage("child"));
		await session.getStorage().setLeafId(rootId);

		const db = await env.openSqlite(databasePath);
		try {
			const row = await db
				.prepare("SELECT active_leaf_id FROM sessions WHERE id = ?")
				.get<{ active_leaf_id: string | null }>(["session-1"]);
			expect(row?.active_leaf_id).toBe(rootId);
		} finally {
			await db.close();
		}

		const reopened = await repo.open(await session.getMetadata());
		expect(await reopened.getLeafId()).toBe(rootId);
		expect(childId).not.toBe(rootId);
	});

	it("materializes a new branch when appending from a parent with an existing child", async () => {
		const root = createTempDir();
		const databasePath = join(root, "sessions.sqlite");
		const env = new SqliteNodeExecutionEnv({ cwd: root });
		const repo = new SqliteSessionRepo({ env, databasePath });
		const session = await repo.create({ cwd: root, id: "session-1" });
		const rootId = await session.appendMessage(createUserMessage("root"));
		const firstChildId = await session.appendMessage(createAssistantMessage("first child"));
		await session.getStorage().setLeafId(rootId);
		const secondChildId = await session.appendMessage(createAssistantMessage("second child"));

		const db = await env.openSqlite(databasePath);
		try {
			const branchRows = await db
				.prepare(
					"SELECT branch_id, entry_id, entry_seq FROM branch_entries WHERE session_id = ? ORDER BY branch_id, entry_seq",
				)
				.all<{ branch_id: string; entry_id: string; entry_seq: number }>(["session-1"]);
			const branchIds = [...new Set(branchRows.map((row) => row.branch_id))];
			expect(branchIds).toHaveLength(3);
			expect(branchRows.filter((row) => row.entry_id === rootId)).toHaveLength(3);
			expect(branchRows.filter((row) => row.entry_id === firstChildId)).toHaveLength(1);
			expect(branchRows.filter((row) => row.entry_id === secondChildId)).toHaveLength(1);
		} finally {
			await db.close();
		}
	});

	it("materializes session summary fields transactionally", async () => {
		const root = createTempDir();
		const databasePath = join(root, "sessions.sqlite");
		const env = new SqliteNodeExecutionEnv({ cwd: root });
		const repo = new SqliteSessionRepo({ env, databasePath });
		const session = await repo.create({ cwd: root, id: "session-1" });
		const userId = await session.appendMessage(createUserMessage("one"));
		await session.appendThinkingLevelChange("high");
		await session.appendModelChange("anthropic", "claude-sonnet-4-5");
		const assistant = {
			...createAssistantMessage("two"),
			provider: "anthropic",
			model: "claude-sonnet-4-5",
			usage: {
				input: 100,
				output: 25,
				cacheRead: 40,
				cacheWrite: 10,
				totalTokens: 175,
				cost: { input: 0.1, output: 0.2, cacheRead: 0.03, cacheWrite: 0.04, total: 0.37 },
			},
		};
		await session.appendMessage(assistant);
		await session.appendSessionName("  My Session  ");
		await session.appendLabel(userId, "checkpoint");

		const db = await env.openSqlite(databasePath);
		try {
			const row = await db.prepare("SELECT session_id, payload FROM session_materialized WHERE session_id = ?").get<{
				session_id: string;
				payload: string;
			}>(["session-1"]);
			expect(row).toBeDefined();
			expect(row?.session_id).toBe("session-1");
			expect(JSON.parse(row?.payload ?? "null")).toMatchObject({
				name: "My Session",
				messageCount: 2,
				cachedTokens: 40,
				uncachedTokens: 110,
				totalTokens: 175,
				costTotal: 0.37,
			});
			const entryRows = await db
				.prepare(
					"SELECT session_id, entry_seq, type, payload FROM entry_materialized WHERE session_id = ? ORDER BY entry_seq, type",
				)
				.all<{
					session_id: string;
					entry_seq: number;
					type: string;
					payload: string;
				}>(["session-1"]);
			expect(
				entryRows.some((entryRow) => entryRow.type === "label" && JSON.parse(entryRow.payload).targetId === userId),
			).toBe(true);
			expect(
				entryRows.some(
					(entryRow) => entryRow.type === "thinking" && JSON.parse(entryRow.payload).thinkingLevel === "high",
				),
			).toBe(true);
			expect(
				entryRows.some(
					(entryRow) => entryRow.type === "model" && JSON.parse(entryRow.payload).modelId === "claude-sonnet-4-5",
				),
			).toBe(true);
		} finally {
			await db.close();
		}
	});
});
