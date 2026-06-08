"use client";

import { fmtInt } from "@/lib/format";
import { FP_COLOR, JP_COLOR, type RemainingRow } from "@/lib/types";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

const FUENTE_COLORS: Record<string, string> = {
  distrito: "#60a5fa",   // blue-400
  similitud: "#a78bfa",  // violet-400
  provincia: "#f472b6",  // pink-400
  region: "#fb923c",     // orange-400
  ambito: "#fbbf24",     // amber-400
  default: "#6b7280",    // gray-500
};

const FUENTE_LABELS: Record<string, string> = {
  distrito: "Distrito (datos directos)",
  similitud: "Distritos similares",
  provincia: "Promedio provincial",
  region: "Promedio regional",
  ambito: "Promedio ámbito",
  default: "Sin datos (50/50)",
};

// All possible fuente keys in order of precedence
const FUENTE_KEYS = ["distrito", "similitud", "provincia", "region", "ambito", "default"];

export function RemainingActas({ rows }: { rows: RemainingRow[] }) {
  if (!rows || rows.length === 0) return null;

  const withData = rows.filter((r) => r.remaining_votos > 0);

  // For each region, compute signed contribution per fuente.
  // If region favors FP (net_fp > 0): each fuente contributes positively proportional to its share.
  // If JP: each fuente contributes negatively.
  // The magnitude is: (fuente_votos / total_remaining_votos) * net_fp
  const chartData = withData.map((r) => {
    const entry: Record<string, string | number> = {
      name: r.ambito === "EXTRANJERO" ? `${r.region} (Ext)` : r.region,
      ambito: r.ambito,
      net_fp: r.net_fp,
      remaining_votos: r.remaining_votos,
      fp_votos: r.fp_remaining_votos,
      jp_votos: r.jp_remaining_votos,
      fp_pct: r.fp_remaining_pct,
    };

    // Distribute the net_fp across fuentes proportionally
    const totalFuente = Object.values(r.by_fuente).reduce((a, b) => a + b, 0);
    for (const key of FUENTE_KEYS) {
      const fuenteVotos = r.by_fuente[key] ?? 0;
      if (totalFuente > 0 && fuenteVotos > 0) {
        entry[`f_${key}`] = Math.round((fuenteVotos / totalFuente) * r.net_fp);
      } else {
        entry[`f_${key}`] = 0;
      }
    }
    return entry;
  });

  // Find which fuentes actually appear
  const activeFuentes = FUENTE_KEYS.filter((key) =>
    chartData.some((d) => (d[`f_${key}`] as number) !== 0)
  );

  const maxAbs = Math.max(...withData.map((r) => Math.abs(r.net_fp)));

  return (
    <section className="rounded-xl border border-[var(--border)] bg-[var(--surface)] p-5">
      <h2 className="text-sm font-medium uppercase tracking-wider text-[var(--muted)]">
        Contribución neta por región (votos restantes)
      </h2>
      <p className="mt-1 text-xs text-[var(--muted-2)]">
        Derecha (+ votos netos FP) · Izquierda (+ votos netos JP) · Coloreado por fuente de proyección
      </p>

      <div className="mt-4" style={{ height: withData.length * 22 + 60 }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            data={chartData}
            layout="vertical"
            margin={{ top: 5, right: 20, left: 0, bottom: 5 }}
            stackOffset="sign"
            barGap={0}
            barCategoryGap="15%"
          >
            <CartesianGrid stroke="#27272a" horizontal={false} />
            <XAxis
              type="number"
              tick={{ fill: "#a1a1aa", fontSize: 10 }}
              tickLine={false}
              axisLine={{ stroke: "#3f3f46" }}
              tickFormatter={(v) => {
                const abs = Math.abs(v as number);
                if (abs >= 1000) return `${((v as number) / 1000).toFixed(0)}k`;
                return String(v);
              }}
              domain={[-maxAbs * 1.15, maxAbs * 1.15]}
            />
            <YAxis
              type="category"
              dataKey="name"
              tick={{ fill: "#a1a1aa", fontSize: 9 }}
              tickLine={false}
              axisLine={false}
              width={110}
              interval={0}
            />
            <ReferenceLine x={0} stroke="#52525b" strokeWidth={1} />
            <Tooltip
              cursor={{ fill: "rgba(255,255,255,0.03)" }}
              contentStyle={{
                background: "#18181b",
                border: "1px solid #3f3f46",
                borderRadius: 8,
                fontSize: 12,
                color: "#fafafa",
              }}
              content={<CustomTooltip />}
            />
            <Legend
              wrapperStyle={{ fontSize: 11, paddingTop: 8, color: "#a1a1aa" }}
              formatter={(value: string) => FUENTE_LABELS[value] ?? value}
            />
            {activeFuentes.map((key) => (
              <Bar
                key={key}
                dataKey={`f_${key}`}
                name={FUENTE_LABELS[key] ?? key}
                stackId="a"
                fill={FUENTE_COLORS[key]}
                radius={0}
                maxBarSize={14}
              />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>

      <div className="mt-3 flex items-center justify-center gap-6 text-xs text-[var(--muted-2)]">
        <span className="flex items-center gap-1.5">
          <span className="inline-block h-2.5 w-5 rounded-sm" style={{ backgroundColor: FP_COLOR }} />
          ← Favorece FP
        </span>
        <span className="flex items-center gap-1.5">
          Favorece JP →
          <span className="inline-block h-2.5 w-5 rounded-sm" style={{ backgroundColor: JP_COLOR }} />
        </span>
      </div>
    </section>
  );
}

type TooltipEntry = {
  payload: Record<string, string | number>;
};

function CustomTooltip({
  active,
  payload,
}: {
  active?: boolean;
  payload?: TooltipEntry[];
}) {
  if (!active || !payload || !payload[0]) return null;
  const d = payload[0].payload;
  const netFp = d.net_fp as number;
  const fpVotos = d.fp_votos as number;
  const jpVotos = d.jp_votos as number;
  const fpPct = d.fp_pct as number;
  const remainingVotos = d.remaining_votos as number;

  return (
    <div className="space-y-2 rounded-lg border border-[var(--border-strong)] bg-[var(--surface)] p-3 text-xs shadow-lg">
      <p className="font-semibold text-[var(--foreground)]">
        {d.name as string}{" "}
        <span className="font-normal text-[var(--muted-2)]">({d.ambito as string})</span>
      </p>

      <div className="grid grid-cols-2 gap-x-4 gap-y-1">
        <span className="text-[var(--muted)]">Votos restantes</span>
        <span className="text-right font-medium">{fmtInt(remainingVotos)}</span>
        <span className="text-[var(--muted)]">FP esperado</span>
        <span className="text-right font-medium" style={{ color: FP_COLOR }}>
          {fmtInt(fpVotos)} ({fpPct}%)
        </span>
        <span className="text-[var(--muted)]">JP esperado</span>
        <span className="text-right font-medium" style={{ color: JP_COLOR }}>
          {fmtInt(jpVotos)} ({(100 - fpPct).toFixed(1)}%)
        </span>
        <span className="text-[var(--muted)]">Neto</span>
        <span
          className="text-right font-semibold"
          style={{ color: netFp >= 0 ? FP_COLOR : JP_COLOR }}
        >
          {netFp >= 0 ? "+" : ""}
          {fmtInt(netFp)} {netFp >= 0 ? "FP" : "JP"}
        </span>
      </div>

      <div className="border-t border-[var(--border)] pt-2">
        <p className="mb-1 text-[var(--muted)]">Por fuente de proyección:</p>
        {FUENTE_KEYS.filter((k) => (d[`f_${k}`] as number) !== 0).map((key) => (
          <div key={key} className="flex items-center justify-between gap-2">
            <span className="flex items-center gap-1.5">
              <span
                className="inline-block size-2 rounded-sm"
                style={{ backgroundColor: FUENTE_COLORS[key] }}
              />
              <span className="text-[var(--muted)]">{FUENTE_LABELS[key] ?? key}</span>
            </span>
            <span className="font-medium text-[var(--foreground)]">
              {fmtInt(Math.abs(d[`f_${key}`] as number))}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}
