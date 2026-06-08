"use client";

import { fmtInt } from "@/lib/format";
import { FP_COLOR, JP_COLOR, type RemainingRow } from "@/lib/types";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

const FUENTE_COLORS: Record<string, string> = {
  distrito: "#60a5fa", // blue-400
  similitud: "#a78bfa", // violet-400
  provincia: "#f472b6", // pink-400
  region: "#fb923c", // orange-400
  ambito: "#fbbf24", // amber-400
  default: "#6b7280", // gray-500
};

const FUENTE_LABELS: Record<string, string> = {
  distrito: "Distrito (datos directos)",
  similitud: "Distritos similares",
  provincia: "Promedio provincial",
  region: "Promedio regional",
  ambito: "Promedio ámbito",
  default: "Sin datos (50/50)",
};

export function RemainingActas({ rows }: { rows: RemainingRow[] }) {
  if (!rows || rows.length === 0) return null;

  // Filter to regions that actually have remaining votes
  const withData = rows.filter((r) => r.remaining_votos > 0);
  // Already sorted by net_fp desc from the backend

  const maxAbs = Math.max(...withData.map((r) => Math.abs(r.net_fp)));

  const chartData = withData.map((r) => ({
    region: r.region,
    ambito: r.ambito,
    net_fp: r.net_fp,
    remaining_votos: r.remaining_votos,
    fp_votos: r.fp_remaining_votos,
    jp_votos: r.jp_remaining_votos,
    fp_pct: r.fp_remaining_pct,
    by_fuente: r.by_fuente,
  }));

  return (
    <section className="rounded-xl border border-[var(--border)] bg-[var(--surface)] p-5">
      <h2 className="text-sm font-medium uppercase tracking-wider text-[var(--muted)]">
        Contribución neta por región (votos restantes)
      </h2>
      <p className="mt-1 text-xs text-[var(--muted-2)]">
        Positivo = favorece FP · Negativo = favorece JP · Ordenado por contribución neta
      </p>

      <div className="mt-4" style={{ height: withData.length * 28 + 40 }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            data={chartData}
            layout="vertical"
            margin={{ top: 5, right: 20, left: 0, bottom: 5 }}
          >
            <CartesianGrid stroke="#27272a" horizontal={false} />
            <XAxis
              type="number"
              tick={{ fill: "#a1a1aa", fontSize: 10 }}
              tickLine={false}
              axisLine={{ stroke: "#3f3f46" }}
              tickFormatter={(v) => {
                const abs = Math.abs(v);
                if (abs >= 1000) return `${(v / 1000).toFixed(0)}k`;
                return String(v);
              }}
              domain={[-maxAbs * 1.1, maxAbs * 1.1]}
            />
            <YAxis
              type="category"
              dataKey="region"
              tick={{ fill: "#a1a1aa", fontSize: 10 }}
              tickLine={false}
              axisLine={false}
              width={95}
            />
            <ReferenceLine x={0} stroke="#3f3f46" strokeWidth={1} />
            <Tooltip
              cursor={{ fill: "rgba(255,255,255,0.03)" }}
              contentStyle={{
                background: "#18181b",
                border: "1px solid #3f3f46",
                borderRadius: 8,
                fontSize: 12,
                color: "#fafafa",
              }}
              labelStyle={{ color: "#fafafa", fontWeight: 600, marginBottom: 4 }}
              content={<CustomTooltip />}
            />
            <Bar dataKey="net_fp" radius={[3, 3, 3, 3]} maxBarSize={20}>
              {chartData.map((entry, idx) => (
                <Cell
                  key={idx}
                  fill={entry.net_fp >= 0 ? FP_COLOR : JP_COLOR}
                  opacity={0.85}
                />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Legend for fuente colors */}
      <div className="mt-4 flex flex-wrap gap-3 border-t border-[var(--border)] pt-3">
        <span className="text-xs text-[var(--muted-2)]">Fuente de proyección:</span>
        {Object.entries(FUENTE_COLORS).map(([key, color]) => (
          <span key={key} className="flex items-center gap-1.5 text-xs text-[var(--muted)]">
            <span
              className="inline-block size-2.5 rounded-sm"
              style={{ backgroundColor: color }}
            />
            {FUENTE_LABELS[key] ?? key}
          </span>
        ))}
      </div>
    </section>
  );
}

type TooltipPayloadEntry = {
  payload: {
    region: string;
    ambito: string;
    net_fp: number;
    remaining_votos: number;
    fp_votos: number;
    jp_votos: number;
    fp_pct: number;
    by_fuente: Record<string, number>;
  };
};

function CustomTooltip({
  active,
  payload,
}: {
  active?: boolean;
  payload?: TooltipPayloadEntry[];
}) {
  if (!active || !payload || !payload[0]) return null;
  const d = payload[0].payload;

  return (
    <div className="space-y-2 rounded-lg border border-[var(--border-strong)] bg-[var(--surface)] p-3 text-xs shadow-lg">
      <div>
        <p className="font-semibold text-[var(--foreground)]">
          {d.region}{" "}
          <span className="font-normal text-[var(--muted-2)]">({d.ambito})</span>
        </p>
      </div>

      <div className="grid grid-cols-2 gap-x-4 gap-y-1">
        <span className="text-[var(--muted)]">Votos restantes</span>
        <span className="text-right font-medium">{fmtInt(d.remaining_votos)}</span>
        <span className="text-[var(--muted)]">FP esperado</span>
        <span className="text-right font-medium" style={{ color: FP_COLOR }}>
          {fmtInt(d.fp_votos)} ({d.fp_pct}%)
        </span>
        <span className="text-[var(--muted)]">JP esperado</span>
        <span className="text-right font-medium" style={{ color: JP_COLOR }}>
          {fmtInt(d.jp_votos)} ({(100 - d.fp_pct).toFixed(1)}%)
        </span>
        <span className="text-[var(--muted)]">Neto</span>
        <span
          className="text-right font-semibold"
          style={{ color: d.net_fp >= 0 ? FP_COLOR : JP_COLOR }}
        >
          {d.net_fp >= 0 ? "+" : ""}
          {fmtInt(d.net_fp)} {d.net_fp >= 0 ? "FP" : "JP"}
        </span>
      </div>

      {Object.keys(d.by_fuente).length > 0 && (
        <div className="border-t border-[var(--border)] pt-2">
          <p className="mb-1 text-[var(--muted)]">Por fuente de proyección:</p>
          {Object.entries(d.by_fuente)
            .sort((a, b) => b[1] - a[1])
            .map(([key, val]) => (
              <div key={key} className="flex items-center justify-between gap-2">
                <span className="flex items-center gap-1.5">
                  <span
                    className="inline-block size-2 rounded-sm"
                    style={{ backgroundColor: FUENTE_COLORS[key] ?? "#6b7280" }}
                  />
                  <span className="text-[var(--muted)]">{FUENTE_LABELS[key] ?? key}</span>
                </span>
                <span className="font-medium text-[var(--foreground)]">{fmtInt(val)}</span>
              </div>
            ))}
        </div>
      )}
    </div>
  );
}
