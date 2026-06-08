import { fmtInt, fmtPct } from "@/lib/format";
import type { RegionRow } from "@/lib/types";

export function RegionDrilldown({ regions }: { regions: RegionRow[] }) {
  return (
    <details className="group rounded-xl border border-zinc-200 bg-white">
      <summary className="flex cursor-pointer list-none items-center justify-between gap-4 p-5">
        <div>
          <h2 className="text-sm font-medium uppercase tracking-wider text-zinc-500">
            Resultado por región
          </h2>
          <p className="mt-1 text-xs text-zinc-500">
            Proyección final por departamento — clic para expandir
          </p>
        </div>
        <span className="rounded-full bg-zinc-100 px-2.5 py-1 text-xs font-medium text-zinc-700 transition group-open:bg-zinc-900 group-open:text-white">
          {regions.length} regiones
        </span>
      </summary>

      <div className="border-t border-zinc-100">
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="bg-zinc-50 text-xs uppercase tracking-wider text-zinc-500">
              <tr>
                <th className="px-4 py-2 text-left font-medium">Región</th>
                <th className="px-4 py-2 text-right font-medium">FP</th>
                <th className="px-4 py-2 text-right font-medium">JP</th>
                <th className="px-4 py-2 text-right font-medium">% actas</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-zinc-100">
              {regions.map((r) => (
                <tr key={`${r.ambito}-${r.region}`}>
                  <td className="px-4 py-2.5">
                    <p className="font-medium text-zinc-800">{r.region}</p>
                    <p className="text-xs text-zinc-500">{r.ambito}</p>
                  </td>
                  <td className="px-4 py-2.5 text-right">
                    <p className="font-medium text-[var(--fp)]">{fmtPct(r.fp_pct, 2)}</p>
                    <p className="text-xs text-zinc-500">{fmtInt(r.fp_votos)}</p>
                  </td>
                  <td className="px-4 py-2.5 text-right">
                    <p className="font-medium text-[var(--jp)]">{fmtPct(r.jp_pct, 2)}</p>
                    <p className="text-xs text-zinc-500">{fmtInt(r.jp_votos)}</p>
                  </td>
                  <td className="px-4 py-2.5 text-right text-zinc-600">
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
