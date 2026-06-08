// Formatting helpers — keep style consistent across the dashboard.
// Numbers: en-US comma separators (e.g., 1,873,207).
// Percentages: 1 decimal + % sign (e.g., 50.8%).

const numberFmt = new Intl.NumberFormat("en-US");

export function fmtInt(n: number): string {
  if (!Number.isFinite(n)) return "—";
  return numberFmt.format(Math.round(n));
}

export function fmtPct(n: number, decimals = 1): string {
  if (!Number.isFinite(n)) return "—";
  return `${n.toFixed(decimals)}%`;
}

export function fmtSignedPct(n: number, decimals = 2): string {
  if (!Number.isFinite(n)) return "—";
  const sign = n > 0 ? "+" : "";
  return `${sign}${n.toFixed(decimals)}%`;
}

export function fmtTimestamp(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString("es-PE", {
    dateStyle: "medium",
    timeStyle: "short",
  });
}
