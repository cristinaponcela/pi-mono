#!/usr/bin/env node

import { rm } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const packageDir = resolve(scriptDir, "..");
const distDir = resolve(packageDir, "dist");

async function clean() {
	await rm(distDir, { recursive: true, force: true });
}

const command = process.argv[2];

if (command === "clean") {
	await clean();
	process.exit(0);
}

console.error("Usage: node scripts/prepare-dist.mjs <clean>");
process.exit(1);
