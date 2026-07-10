import type { SessionTreeEntry, ThinkingLevel } from "@earendil-works/pi-agent-core";
import { invalidSession, isRecord } from "./shared.ts";

export interface SessionMaterializedRow {
	session_id: string;
	name: string | null;
	message_count: number;
	cached_tokens: number;
	uncached_tokens: number;
	total_tokens: number;
	cost_total: number;
	labels_json: string;
	model_thinking_configs_json: string;
}

export interface ModelThinkingConfig {
	provider: string;
	modelId: string;
	thinkingLevel: ThinkingLevel;
}

export interface SessionMaterializedState {
	name: string | undefined;
	messageCount: number;
	cachedTokens: number;
	uncachedTokens: number;
	totalTokens: number;
	costTotal: number;
	labelsById: Map<string, string>;
	modelThinkingConfigs: ModelThinkingConfig[];
	currentModel: { provider: string; modelId: string } | null;
	currentThinkingLevel: ThinkingLevel | null;
}

export function updateLabelCache(labelsById: Map<string, string>, entry: SessionTreeEntry): void {
	if (entry.type !== "label") return;
	const label = entry.label?.trim();
	if (label) {
		labelsById.set(entry.targetId, label);
	} else {
		labelsById.delete(entry.targetId);
	}
}

export function createEmptyMaterializedState(): SessionMaterializedState {
	return {
		name: undefined,
		messageCount: 0,
		cachedTokens: 0,
		uncachedTokens: 0,
		totalTokens: 0,
		costTotal: 0,
		labelsById: new Map<string, string>(),
		modelThinkingConfigs: [],
		currentModel: null,
		currentThinkingLevel: null,
	};
}

export function serializeLabels(labelsById: ReadonlyMap<string, string>): string {
	const entries = [...labelsById.entries()].sort(([left], [right]) => left.localeCompare(right));
	return JSON.stringify(Object.fromEntries(entries));
}

function parseLabelsJson(json: string): Map<string, string> {
	let parsed: unknown;
	try {
		parsed = JSON.parse(json);
	} catch (error) {
		throw invalidSession(`materialized labels_json is not valid JSON`, error instanceof Error ? error : undefined);
	}
	if (!isRecord(parsed) || Array.isArray(parsed)) {
		throw invalidSession("materialized labels_json is not an object");
	}
	const labelsById = new Map<string, string>();
	for (const [key, value] of Object.entries(parsed)) {
		if (typeof value !== "string") throw invalidSession("materialized labels_json has a non-string label");
		if (value.trim()) labelsById.set(key, value);
	}
	return labelsById;
}

function compareModelThinkingConfig(left: ModelThinkingConfig, right: ModelThinkingConfig): number {
	return (
		left.provider.localeCompare(right.provider) ||
		left.modelId.localeCompare(right.modelId) ||
		left.thinkingLevel.localeCompare(right.thinkingLevel)
	);
}

function normalizeModelThinkingConfigs(configs: readonly ModelThinkingConfig[]): ModelThinkingConfig[] {
	const unique = new Map<string, ModelThinkingConfig>();
	for (const config of configs) {
		unique.set(`${config.provider}\u0000${config.modelId}\u0000${config.thinkingLevel}`, config);
	}
	return [...unique.values()].sort(compareModelThinkingConfig);
}

export function serializeModelThinkingConfigs(configs: readonly ModelThinkingConfig[]): string {
	return JSON.stringify(normalizeModelThinkingConfigs(configs));
}

export function isThinkingLevel(value: unknown): value is ThinkingLevel {
	return (
		value === "off" ||
		value === "minimal" ||
		value === "low" ||
		value === "medium" ||
		value === "high" ||
		value === "xhigh"
	);
}

function parseModelThinkingConfigsJson(json: string): ModelThinkingConfig[] {
	let parsed: unknown;
	try {
		parsed = JSON.parse(json);
	} catch (error) {
		throw invalidSession(
			`materialized model_thinking_configs_json is not valid JSON`,
			error instanceof Error ? error : undefined,
		);
	}
	if (!Array.isArray(parsed)) {
		throw invalidSession("materialized model_thinking_configs_json is not an array");
	}
	const configs: ModelThinkingConfig[] = [];
	for (const item of parsed) {
		if (!isRecord(item) || typeof item.provider !== "string" || typeof item.modelId !== "string") {
			throw invalidSession("materialized model_thinking_configs_json has an invalid item");
		}
		if (!isThinkingLevel(item.thinkingLevel)) {
			throw invalidSession("materialized model_thinking_configs_json has an invalid thinking level");
		}
		configs.push({ provider: item.provider, modelId: item.modelId, thinkingLevel: item.thinkingLevel });
	}
	return normalizeModelThinkingConfigs(configs);
}

function addModelThinkingConfig(
	state: SessionMaterializedState,
	provider: string,
	modelId: string,
	thinkingLevel: ThinkingLevel,
): void {
	state.modelThinkingConfigs = normalizeModelThinkingConfigs([
		...state.modelThinkingConfigs,
		{ provider, modelId, thinkingLevel },
	]);
}

function getAssistantUsage(message: unknown):
	| {
			provider: string;
			modelId: string;
			input: number;
			output: number;
			cacheRead: number;
			cacheWrite: number;
			costTotal: number;
	  }
	| undefined {
	if (!isRecord(message) || message.role !== "assistant") return undefined;
	if (typeof message.provider !== "string" || typeof message.model !== "string") return undefined;
	if (!isRecord(message.usage) || !isRecord(message.usage.cost)) return undefined;
	const { input, output, cacheRead, cacheWrite } = message.usage;
	const costTotal = message.usage.cost.total;
	if (
		typeof input !== "number" ||
		typeof output !== "number" ||
		typeof cacheRead !== "number" ||
		typeof cacheWrite !== "number" ||
		typeof costTotal !== "number"
	) {
		return undefined;
	}
	return {
		provider: message.provider,
		modelId: message.model,
		input,
		output,
		cacheRead,
		cacheWrite,
		costTotal,
	};
}

export function applyEntryToMaterializedState(state: SessionMaterializedState, entry: SessionTreeEntry): void {
	switch (entry.type) {
		case "session_info":
			state.name = entry.name?.trim() || undefined;
			break;
		case "label":
			updateLabelCache(state.labelsById, entry);
			break;
		case "model_change":
			state.currentModel = { provider: entry.provider, modelId: entry.modelId };
			if (state.currentThinkingLevel) {
				addModelThinkingConfig(state, entry.provider, entry.modelId, state.currentThinkingLevel);
			}
			break;
		case "thinking_level_change":
			if (!isThinkingLevel(entry.thinkingLevel)) break;
			state.currentThinkingLevel = entry.thinkingLevel;
			if (state.currentModel) {
				addModelThinkingConfig(state, state.currentModel.provider, state.currentModel.modelId, entry.thinkingLevel);
			}
			break;
		case "message": {
			state.messageCount += 1;
			const usage = getAssistantUsage(entry.message);
			if (!usage) break;
			state.cachedTokens += usage.cacheRead;
			state.uncachedTokens += usage.input + usage.cacheWrite;
			state.totalTokens += usage.input + usage.output + usage.cacheRead + usage.cacheWrite;
			state.costTotal += usage.costTotal;
			state.currentModel = { provider: usage.provider, modelId: usage.modelId };
			if (state.currentThinkingLevel) {
				addModelThinkingConfig(state, usage.provider, usage.modelId, state.currentThinkingLevel);
			}
			break;
		}
		case "active_tools_change":
		case "compaction":
		case "branch_summary":
		case "custom":
		case "custom_message":
		case "leaf":
			break;
		default: {
			const exhaustive: never = entry;
			void exhaustive;
			break;
		}
	}
}

export function materializedStateFromRow(
	row: SessionMaterializedRow,
	entries: SessionTreeEntry[],
): SessionMaterializedState {
	const state: SessionMaterializedState = {
		name: row.name ?? undefined,
		messageCount: row.message_count,
		cachedTokens: row.cached_tokens,
		uncachedTokens: row.uncached_tokens,
		totalTokens: row.total_tokens,
		costTotal: row.cost_total,
		labelsById: parseLabelsJson(row.labels_json),
		modelThinkingConfigs: parseModelThinkingConfigsJson(row.model_thinking_configs_json),
		currentModel: null,
		currentThinkingLevel: null,
	};
	for (const entry of entries) {
		if (entry.type === "model_change") {
			state.currentModel = { provider: entry.provider, modelId: entry.modelId };
		} else if (entry.type === "thinking_level_change") {
			if (isThinkingLevel(entry.thinkingLevel)) state.currentThinkingLevel = entry.thinkingLevel;
		} else if (entry.type === "message") {
			const usage = getAssistantUsage(entry.message);
			if (usage) state.currentModel = { provider: usage.provider, modelId: usage.modelId };
		}
	}
	return state;
}

export function materializedStateValues(sessionId: string, state: SessionMaterializedState): unknown[] {
	return [
		sessionId,
		state.name ?? null,
		state.messageCount,
		state.cachedTokens,
		state.uncachedTokens,
		state.totalTokens,
		state.costTotal,
		serializeLabels(state.labelsById),
		serializeModelThinkingConfigs(state.modelThinkingConfigs),
	];
}
