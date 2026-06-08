import { fmtInt, fmtPct, fmtSignedPct } from "@/lib/format";
import type { Summary } from "@/lib/types";

export function Hero({ summary }: { summary: Summary }) {
  const { fp, jp, margin } = summary;
  const winner = fp.win_probability >= jp.win_probability ? "fp" : "jp";

  return (
    <section className="grid grid-cols-1 gap-4 lg:grid-cols-3">
      {/* Probability of winning */}
      <div className="rounded-xl border border-[var(--border)] bg-[var(--surface)] p-5 lg:col-span-2">
        <h2 className="text-sm font-medium uppercase tracking-wider text-[var(--muted)]">
          Probabilidad de ganar
        </h2>
        <p className="mt-1 text-xs text-[var(--muted-2)]">
          Estimada con simulación Monte Carlo (3 000 escenarios)
        </p>

        <div className="mt-5 space-y-4">
          <ProbBar
            label="Fuerza Popular"
            shortLabel="FP"
            value={fp.win_probability}
            color="bg-[var(--fp-strong)]"
            highlight={winner === "fp"}
          />
          <ProbBar
            label="Juntos por el Perú"
            shortLabel="JP"
            value={jp.win_probability}
            color="bg-[var(--jp-strong)]"
            highlight={winner === "jp"}
          />
        </div>
      </div>

      {/* Margin */}
      <div className="rounded-xl border border-[var(--border)] bg-[var(--surface)] p-5">
        <h2 className="text-sm font-medium uppercase tracking-wider text-[var(--muted)]">
          Margen FP vs JP
        </h2>
        <p className="mt-1 text-xs text-[var(--muted-2)]">Sobre votos válidos proyectados</p>

        <div className="mt-4">
          <p className="text-3xl font-semibold tracking-tight">
            {fmtSignedPct(margin.mean, 2)}
          </p>
          <p className="mt-1 text-xs text-[var(--muted-2)]">
            Intervalo 90%: {fmtSignedPct(margin.p5, 2)} a {fmtSignedPct(margin.p95, 2)}
          </p>
        </div>

        <dl className="mt-4 grid grid-cols-2 gap-3 border-t border-[var(--border)] pt-4 text-sm">
          <div>
            <dt className="text-xs text-[var(--muted-2)]">Votos FP proyectados</dt>
            <dd className="font-medium text-[var(--fp)]">{fmtInt(fp.votos_proyectados)}</dd>
            <dd className="text-xs text-[var(--muted-2)]">{fmtPct(fp.pct_proyectado, 2)}</dd>
          </div>
          <div>
            <dt className="text-xs text-[var(--muted-2)]">Votos JP proyectados</dt>
            <dd className="font-medium text-[var(--jp)]">{fmtInt(jp.votos_proyectados)}</dd>
            <dd className="text-xs text-[var(--muted-2)]">{fmtPct(jp.pct_proyectado, 2)}</dd>
          </div>
        </dl>
      </div>
    </section>
  );
}

function ProbBar({
  label,
  shortLabel,
  value,
  color,
  highlight,
}: {
  label: string;
  shortLabel: string;
  value: number;
  color: string;
  highlight: boolean;
}) {
  return (
    <div>
      <div className="mb-1.5 flex items-baseline justify-between gap-3">
        <div className="flex items-baseline gap-2">
          <span
            className={`inline-flex items-center justify-center rounded-md px-1.5 py-0.5 text-xs font-semibold text-zinc-950 ${color}`}
            aria-hidden
          >
            {shortLabel}
          </span>
          <span className="text-sm font-medium text-[var(--foreground)]">{label}</span>
        </div>
        <span
          className={`text-2xl font-semibold tracking-tight ${
            highlight ? "text-[var(--foreground)]" : "text-[var(--muted)]"
          }`}
        >
          {fmtPct(value, 2)}
        </span>
      </div>
      <div className="h-2 w-full overflow-hidden rounded-full bg-[var(--surface-2)]">
        <div
          className={`h-full rounded-full transition-[width] ${color}`}
          style={{ width: `${Math.min(value, 100)}%` }}
        />
      </div>
    </div>
  );
}
