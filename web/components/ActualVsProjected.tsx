import { fmtInt, fmtPct } from "@/lib/format";
import type { Summary } from "@/lib/types";

export function ActualVsProjected({ summary }: { summary: Summary }) {
  const { fp, jp } = summary;

  return (
    <section className="rounded-xl border border-[var(--border)] bg-[var(--surface)] p-5">
      <h2 className="text-sm font-medium uppercase tracking-wider text-[var(--muted)]">
        Resultado actual vs proyectado
      </h2>
      <p className="mt-1 text-xs text-[var(--muted-2)]">
        Lo contado hasta ahora frente a la estimación final con todas las actas
      </p>

      <div className="mt-5 grid grid-cols-1 gap-4 sm:grid-cols-2">
        <Card
          label="Fuerza Popular"
          shortLabel="FP"
          accent="var(--fp)"
          actualVotes={fp.votos_actuales}
          actualPct={fp.pct_actual}
          projVotes={fp.votos_proyectados}
          projPct={fp.pct_proyectado}
        />
        <Card
          label="Juntos por el Perú"
          shortLabel="JP"
          accent="var(--jp)"
          actualVotes={jp.votos_actuales}
          actualPct={jp.pct_actual}
          projVotes={jp.votos_proyectados}
          projPct={jp.pct_proyectado}
        />
      </div>
    </section>
  );
}

function Card({
  label,
  shortLabel,
  accent,
  actualVotes,
  actualPct,
  projVotes,
  projPct,
}: {
  label: string;
  shortLabel: string;
  accent: string;
  actualVotes: number;
  actualPct: number;
  projVotes: number;
  projPct: number;
}) {
  const delta = projPct - actualPct;
  const deltaSign = delta > 0 ? "+" : "";

  return (
    <div className="rounded-lg border border-[var(--border)] bg-[var(--surface-2)]/40 p-4">
      <div className="flex items-baseline gap-2">
        <span
          className="inline-flex items-center justify-center rounded-md px-1.5 py-0.5 text-xs font-semibold text-zinc-950"
          style={{ backgroundColor: accent }}
        >
          {shortLabel}
        </span>
        <h3 className="text-sm font-medium text-[var(--foreground)]">{label}</h3>
      </div>

      <div className="mt-4 grid grid-cols-2 gap-4">
        <div>
          <p className="text-xs text-[var(--muted-2)]">Actual</p>
          <p className="text-lg font-semibold tracking-tight">
            {fmtPct(actualPct, 2)}
          </p>
          <p className="text-xs text-[var(--muted-2)]">{fmtInt(actualVotes)} votos</p>
        </div>
        <div>
          <p className="text-xs text-[var(--muted-2)]">Proyectado</p>
          <p className="text-lg font-semibold tracking-tight" style={{ color: accent }}>
            {fmtPct(projPct, 2)}
          </p>
          <p className="text-xs text-[var(--muted-2)]">{fmtInt(projVotes)} votos</p>
        </div>
      </div>

      <p className="mt-3 text-xs text-[var(--muted-2)]">
        Delta proyección: {deltaSign}
        {delta.toFixed(2)}%
      </p>
    </div>
  );
}
