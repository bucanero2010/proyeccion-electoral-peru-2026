import { fmtInt, fmtPct } from "@/lib/format";
import type { RemainingRow } from "@/lib/types";

export function RemainingActas({ rows }: { rows: RemainingRow[] }) {
  if (!rows || rows.length === 0) {
    return null;
  }

  return (
    <section className="rounded-xl border border-[var(--border)] bg-[var(--surface)] p-5">
      <h2 className="text-sm font-medium uppercase tracking-wider text-[var(--muted)]">
        Actas restantes por región
      </h2>
      <p className="mt-1 text-xs text-[var(--muted-2)]">
        Top 10 regiones con mayor cantidad de votos pendientes y a quién favorecen
      </p>

      <ul className="mt-4 divide-y divide-[var(--border)]">
        {rows.map((r) => (
          <li key={`${r.ambito}-${r.region}`} className="flex items-center justify-between py-2.5">
            <div className="min-w-0">
              <p className="truncate text-sm font-medium text-[var(--foreground)]">{r.region}</p>
              <p className="text-xs text-[var(--muted-2)]">{r.ambito}</p>
            </div>
            <div className="flex items-center gap-3 text-right">
              <div>
                <p className="text-sm font-medium text-[var(--foreground)]">
                  {fmtInt(r.remaining_votos)}
                </p>
                <p className="text-xs text-[var(--muted-2)]">
                  votos esperados · {fmtInt(r.remaining_actas)} actas
                </p>
              </div>
              <span
                className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium"
                style={{
                  background: r.favors === "FP" ? "rgba(249,115,22,0.16)" : "rgba(34,197,94,0.16)",
                  color: r.favors === "FP" ? "var(--fp)" : "var(--jp)",
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
