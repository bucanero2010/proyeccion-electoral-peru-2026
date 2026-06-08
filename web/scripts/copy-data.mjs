#!/usr/bin/env node
// Copy ../data/summary_2v.json into public/data/ so Next.js can statically serve it.
// Runs as predev/prebuild so the dashboard always reads the latest projection.

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(__dirname, "..", "..");
const src = path.join(repoRoot, "data", "summary_2v.json");
const destDir = path.resolve(__dirname, "..", "public", "data");
const dest = path.join(destDir, "summary_2v.json");

fs.mkdirSync(destDir, { recursive: true });

if (!fs.existsSync(src)) {
  console.warn(`[copy-data] ${src} not found. Writing placeholder.`);
  const placeholder = {
    timestamp: new Date().toISOString(),
    pct_actas: 0,
    fp: { votos_actuales: 0, votos_proyectados: 0, pct_actual: 0, pct_proyectado: 0, win_probability: 0 },
    jp: { votos_actuales: 0, votos_proyectados: 0, pct_actual: 0, pct_proyectado: 0, win_probability: 0 },
    margin: { mean: 0, p5: 0, p95: 0 },
    regions: [],
    remaining_by_region: [],
    fuente_breakdown: {},
    history: [],
  };
  fs.writeFileSync(dest, JSON.stringify(placeholder, null, 2));
  process.exit(0);
}

fs.copyFileSync(src, dest);
const stats = fs.statSync(dest);
console.log(`[copy-data] ${src} -> ${dest} (${(stats.size / 1024).toFixed(1)} KB)`);
