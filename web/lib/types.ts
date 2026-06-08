export type CandidateSummary = {
  votos_actuales: number;
  votos_proyectados: number;
  pct_actual: number;
  pct_proyectado: number;
  win_probability: number;
};

export type RegionRow = {
  ambito: string;
  region: string;
  fp_votos: number;
  jp_votos: number;
  fp_pct: number;
  jp_pct: number;
  pct_actas: number;
};

export type RemainingRow = {
  region: string;
  ambito: string;
  remaining_actas: number;
  remaining_votos: number;
  fp_remaining_votos: number;
  jp_remaining_votos: number;
  net_fp: number;
  fp_remaining_pct: number;
  favors: "FP" | "JP";
  by_fuente: Record<string, number>;
};

export type HistoryRow = {
  timestamp: string;
  pct_actas: number;
  fp_pct_actual: number;
  jp_pct_actual: number;
  fp_pct_proyectado: number;
  jp_pct_proyectado: number;
  fp_win_prob: number;
  jp_win_prob: number;
  margin_mean: number;
};

export type Summary = {
  timestamp: string;
  pct_actas: number;
  fp: CandidateSummary;
  jp: CandidateSummary;
  margin: {
    mean: number;
    p5: number;
    p95: number;
  };
  regions: RegionRow[];
  remaining_by_region: RemainingRow[];
  fuente_breakdown: Record<string, number>;
  history: HistoryRow[];
};

export const FP_COLOR = "#f97316"; // orange
export const JP_COLOR = "#22c55e"; // green
