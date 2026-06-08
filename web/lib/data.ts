// Server-side loader. Reads public/data/summary_2v.json synchronously at build/request time.

import fs from "node:fs";
import path from "node:path";
import type { Summary } from "./types";

export function loadSummary(): Summary {
  const file = path.join(process.cwd(), "public", "data", "summary_2v.json");
  const raw = fs.readFileSync(file, "utf-8");
  return JSON.parse(raw) as Summary;
}
