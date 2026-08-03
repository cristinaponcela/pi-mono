import type { SessionStats, SessionTreeEntry } from "@earendil-works/pi-agent-core";
import type { SqliteDatabase } from "../types.ts";
import { invalidSession, isRecord } from "./shared.ts";

export interface SessionMaterializedRow {
	session_id: string;
	payload: string;
}

function getUsage(value: unknown):
	| {
			input: number;
			output: number;
			cacheRead: number;
			cacheWrite: number;
			costTotal: number;
	  }
	| undefined {
	if (!isRecord(value) || !isRecord(value.cost)) return undefined;
	const { input, output, cacheRead, cacheWrite } = value;
	const costTotal = value.cost.total;
	if (
		typeof input !== "number" ||
		typeof output !== "number" ||
		typeof cacheRead !== "number" ||
		typeof cacheWrite !== "number" ||
		typeof costTotal !== "number"
	) {
		return undefined;
	}
	return { input, output, cacheRead, cacheWrite, costTotal };
}

function getAssistantUsage(message: unknown): ReturnType<typeof getUsage> {
	if (!isRecord(message) || message.role !== "assistant") return undefined;
	return getUsage(message.usage);
}

function addUsage(state: SessionStats, usage: NonNullable<ReturnType<typeof getUsage>>): void {
	state.cachedTokens += usage.cacheRead;
	state.uncachedTokens += usage.input + usage.cacheWrite;
	state.totalTokens += usage.input + usage.output + usage.cacheRead + usage.cacheWrite;
	state.costTotal += usage.costTotal;
}

export function createEmptyMaterializedState(): SessionStats {
	return {
		messageCount: 0,
		cachedTokens: 0,
		uncachedTokens: 0,
		totalTokens: 0,
		costTotal: 0,
	};
}

export function applyEntryToMaterializedState(state: SessionStats, entry: SessionTreeEntry): void {
	switch (entry.type) {
		case "message": {
			state.messageCount += 1;
			const usage = getAssistantUsage(entry.message);
			if (usage) addUsage(state, usage);
			break;
		}
		case "compaction":
		case "branch_summary": {
			const usage = getUsage(entry.usage);
			if (usage) addUsage(state, usage);
			break;
		}
		case "active_tools_change":
		case "custom":
		case "custom_message":
		case "label":
		case "leaf":
		case "model_change":
		case "session_info":
		case "thinking_level_change":
			break;
		default: {
			const exhaustive: never = entry;
			void exhaustive;
			break;
		}
	}
}

export function serializeSummary(state: SessionStats): string {
	const summary: SessionStats = {
		messageCount: state.messageCount,
		cachedTokens: state.cachedTokens,
		uncachedTokens: state.uncachedTokens,
		totalTokens: state.totalTokens,
		costTotal: state.costTotal,
	};
	return JSON.stringify(summary);
}

function parseSummary(json: string): SessionStats {
	let parsed: unknown;
	try {
		parsed = JSON.parse(json);
	} catch (error) {
		throw invalidSession(
			`materialized session summary is not valid JSON`,
			error instanceof Error ? error : undefined,
		);
	}
	if (!isRecord(parsed) || Array.isArray(parsed)) {
		throw invalidSession("materialized session summary is not an object");
	}
	if (
		typeof parsed.messageCount !== "number" ||
		typeof parsed.cachedTokens !== "number" ||
		typeof parsed.uncachedTokens !== "number" ||
		typeof parsed.totalTokens !== "number" ||
		typeof parsed.costTotal !== "number"
	) {
		throw invalidSession("materialized session summary has invalid fields");
	}
	return {
		messageCount: parsed.messageCount,
		cachedTokens: parsed.cachedTokens,
		uncachedTokens: parsed.uncachedTokens,
		totalTokens: parsed.totalTokens,
		costTotal: parsed.costTotal,
	};
}

export function materializedStateFromRow(row: SessionMaterializedRow): SessionStats {
	return parseSummary(row.payload);
}

export function materializedStateValues(sessionId: string, state: SessionStats): [sessionId: string, payload: string] {
	return [sessionId, serializeSummary(state)];
}

export async function deleteMaterializedSession(db: SqliteDatabase, sessionId: string): Promise<void> {
	await db.prepare("DELETE FROM session_materialized WHERE session_id = ?").run(sessionId);
}
