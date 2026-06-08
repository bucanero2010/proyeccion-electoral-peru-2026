import { fmtInt, fmtPct } from "@/lib/format";
import type { RemainingRow } from "@/lib/types";

export function RemainingActas({ rows }: { rows: RemainingRow[] }) {
  if (!rows || rows.length === 0) {
    return null;
  }

  return (
    <section className="rounded-xl border border-zinc-200 bg-white p-5">
      <h2 className="text-sm font-medium uppercase tracking-wider text-zinc-500">
        Actas restantes por región
      </h2>
      <p className="mt-1 text-xs text-zinc-500">
        Top 10 regiones con mayor cantidad de actas pendientes y a quién favorecen
      </p>

      <ul className="mt-4 divide-y divide-zinc-100">
        {rows.map((r) => (
          <li key={`${r.ambito}-${r.region}`} className="flex items-center justify-between py-2.5">
            <div className="min-w-0">
              <p className="truncate text-sm font-medium text-zinc-800">{r.region}</p>
              <p className="text-xs text-zinc-500">{r.ambito}</p>
            </div>
            <div className="flex items-center gap-3 text-right">
              <div>
                <p className="text-sm font-medium text-zinc-700">
                  {fmtInt(r.remaining_actas)}
                </p>
                <p className="text-xs text-zinc-500">actas</p>
              </div>
              <span
                className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium"
                style={{
                  background: r.favors === "FP" ? "rgba(249,115,22,0.12)" : "rgba(34,197,94,0.12)",
                  color: r.favors === "FP" ? "#c2410c" : "#15803d",
                }}
              >
                Favorece {r.favors} · {fmtPct(r.favors === "FP" ? r.fp_remaining_pct : 100 - r.fp_remaining_pct, 1)}
              </span>
            </div>
          </li>
        ))}
      </ul>
    </section>
  );
}
