"use client";

import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
  Legend,
} from "recharts";
import { FP_COLOR, JP_COLOR, type HistoryRow } from "@/lib/types";

export function EvolutionChart({ history }: { history: HistoryRow[] }) {
  if (!history || history.length === 0) {
    return (
      <section className="rounded-xl border border-zinc-200 bg-white p-5">
        <h2 className="text-sm font-medium uppercase tracking-wider text-zinc-500">
          Evolución de la proyección
        </h2>
        <p className="mt-3 text-sm text-zinc-500">
          Aún no hay snapshots históricos. Se generarán a medida que se ejecute el scraper.
        </p>
      </section>
    );
  }

  const data = history.map((h) => ({
    label: shortTime(h.timestamp),
    pct_actas: h.pct_actas,
    fp: h.fp_pct_proyectado,
    jp: h.jp_pct_proyectado,
    fp_win: h.fp_win_prob,
  }));

  return (
    <section className="rounded-xl border border-zinc-200 bg-white p-5">
      <div className="flex items-baseline justify-between gap-4">
        <div>
          <h2 className="text-sm font-medium uppercase tracking-wider text-zinc-500">
            Evolución de la proyección
          </h2>
          <p className="mt-1 text-xs text-zinc-500">% proyectado por candidato a lo largo del conteo</p>
        </div>
        <span className="text-xs text-zinc-400">{history.length} snapshots</span>
      </div>

      <div className="mt-4 h-64 w-full">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data} margin={{ top: 5, right: 12, left: -16, bottom: 0 }}>
            <CartesianGrid stroke="#f1f5f9" vertical={false} />
            <XAxis
              dataKey="label"
              tick={{ fill: "#71717a", fontSize: 11 }}
              tickLine={false}
              axisLine={{ stroke: "#e4e4e7" }}
            />
            <YAxis
              tick={{ fill: "#71717a", fontSize: 11 }}
              tickLine={false}
              axisLine={{ stroke: "#e4e4e7" }}
              domain={[40, 60]}
              tickFormatter={(v) => `${v}%`}
            />
            <Tooltip
              contentStyle={{
                background: "white",
                border: "1px solid #e4e4e7",
                borderRadius: 6,
                fontSize: 12,
              }}
              formatter={(value) => {
                const n = typeof value === "number" ? value : Number(value);
                return Number.isFinite(n) ? `${n.toFixed(2)}%` : "—";
              }}
            />
            <Legend wrapperStyle={{ fontSize: 12, paddingTop: 8 }} />
            <Line
              type="monotone"
              dataKey="fp"
              name="FP proyectado"
              stroke={FP_COLOR}
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 4 }}
            />
            <Line
              type="monotone"
              dataKey="jp"
              name="JP proyectado"
              stroke={JP_COLOR}
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 4 }}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </section>
  );
}

function shortTime(iso: string): string {
  const d = new Date(iso.replace(" ", "T"));
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString("es-PE", {
    day: "2-digit",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
  });
}
