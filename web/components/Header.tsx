import { fmtPct, fmtTimestamp } from "@/lib/format";

export function Header({ pctActas, timestamp }: { pctActas: number; timestamp: string }) {
  return (
    <header className="border-b border-zinc-200 bg-white">
      <div className="mx-auto max-w-5xl px-4 py-5 sm:px-6 sm:py-7">
        <div className="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
          <div>
            <p className="text-xs font-medium uppercase tracking-wider text-zinc-500">
              Elecciones generales 2026 — Perú
            </p>
            <h1 className="mt-1 text-2xl font-semibold tracking-tight text-zinc-900 sm:text-3xl">
              Segunda vuelta presidencial
            </h1>
            <p className="mt-1 text-sm text-zinc-600">
              Fuerza Popular vs Juntos por el Perú · Proyección al cierre de actas
            </p>
          </div>
          <div className="flex flex-col items-start gap-1 sm:items-end">
            <span className="inline-flex items-center gap-2 rounded-full bg-zinc-900 px-3 py-1 text-xs font-medium text-white">
              <span className="size-1.5 rounded-full bg-emerald-400" />
              {fmtPct(pctActas, 1)} actas contabilizadas
            </span>
            <span className="text-xs text-zinc-500">
              Actualizado: {fmtTimestamp(timestamp)}
            </span>
          </div>
        </div>
      </div>
    </header>
  );
}
