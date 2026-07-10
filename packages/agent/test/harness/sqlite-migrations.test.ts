import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { SqliteNodeExecutionEnv, SqliteSessionRepo } from "../../../session-backend-sqlite/src/index.ts";
import { createAssistantMessage, createUserMessage } from "./session-test-utils.ts";

function createTempDir(): string {
	return mkdtempSync(join(tmpdir(), "pi-session-backend-sqlite-"));
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
			expect(rows.map((row) => row.id)).toEqual(["0.0.0-0.80.2.sql"]);
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
				]),
			);
			for (const tableName of ["sessions", "session_sequences", "branch_entries", "session_materialized"]) {
				const table = tables.find((row) => row.name === tableName);
				expect(table?.sql).toContain("WITHOUT ROWID");
			}
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
			const row = await db
				.prepare(
					"SELECT session_id, name, message_count, cached_tokens, uncached_tokens, total_tokens, cost_total, labels_json, model_thinking_configs_json FROM session_materialized WHERE session_id = ?",
				)
				.get<{
					session_id: string;
					name: string | null;
					message_count: number;
					cached_tokens: number;
					uncached_tokens: number;
					total_tokens: number;
					cost_total: number;
					labels_json: string;
					model_thinking_configs_json: string;
				}>(["session-1"]);
			expect(row).toBeDefined();
			expect(row).toMatchObject({
				session_id: "session-1",
				name: "My Session",
				message_count: 2,
				cached_tokens: 40,
				uncached_tokens: 110,
				total_tokens: 175,
				cost_total: 0.37,
			});
			expect(JSON.parse(row?.labels_json ?? "null")).toEqual({ [userId]: "checkpoint" });
			expect(JSON.parse(row?.model_thinking_configs_json ?? "null")).toEqual([
				{ provider: "anthropic", modelId: "claude-sonnet-4-5", thinkingLevel: "high" },
			]);
		} finally {
			await db.close();
		}
	});
});
