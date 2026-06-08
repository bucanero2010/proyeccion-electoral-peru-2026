import { fmtInt, fmtPct } from "@/lib/format";
import type { RegionRow } from "@/lib/types";

export function RegionDrilldown({ regions }: { regions: RegionRow[] }) {
  return (
    <details className="group rounded-xl border border-[var(--border)] bg-[var(--surface)]">
      <summary className="flex cursor-pointer list-none items-center justify-between gap-4 p-5">
        <div>
          <h2 className="text-sm font-medium uppercase tracking-wider text-[var(--muted)]">
            Resultado por región
          </h2>
          <p className="mt-1 text-xs text-[var(--muted-2)]">
            Proyección final por departamento — clic para expandir
          </p>
        </div>
        <span className="rounded-full bg-[var(--surface-2)] px-2.5 py-1 text-xs font-medium text-[var(--muted)] ring-1 ring-[var(--border-strong)] transition group-open:bg-[var(--foreground)] group-open:text-[var(--background)]">
          {regions.length} regiones
        </span>
      </summary>

      <div className="border-t border-[var(--border)]">
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="bg-[var(--surface-2)] text-xs uppercase tracking-wider text-[var(--muted)]">
              <tr>
                <th className="px-4 py-2 text-left font-medium">Región</th>
                <th className="px-4 py-2 text-right font-medium">FP</th>
                <th className="px-4 py-2 text-right font-medium">JP</th>
                <th className="px-4 py-2 text-right font-medium">% actas</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-[var(--border)]">
              {regions.map((r) => (
                <tr key={`${r.ambito}-${r.region}`}>
                  <td className="px-4 py-2.5">
                    <p className="font-medium text-[var(--foreground)]">{r.region}</p>
                    <p className="text-xs text-[var(--muted-2)]">{r.ambito}</p>
                  </td>
                  <td className="px-4 py-2.5 text-right">
                    <p className="font-medium text-[var(--fp)]">{fmtPct(r.fp_pct, 2)}</p>
                    <p className="text-xs text-[var(--muted-2)]">{fmtInt(r.fp_votos)}</p>
                  </td>
                  <td className="px-4 py-2.5 text-right">
                    <p className="font-medium text-[var(--jp)]">{fmtPct(r.jp_pct, 2)}</p>
                    <p className="text-xs text-[var(--muted-2)]">{fmtInt(r.jp_votos)}</p>
                  </td>
                  <td className="px-4 py-2.5 text-right text-[var(--muted)]">
                    {fmtPct(r.pct_actas, 1)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </details>
  );
}
