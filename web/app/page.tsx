import { ActualVsProjected } from "@/components/ActualVsProjected";
import { EvolutionChart } from "@/components/EvolutionChart";
import { Header } from "@/components/Header";
import { Hero } from "@/components/Hero";
import { Methodology } from "@/components/Methodology";
import { RegionDrilldown } from "@/components/RegionDrilldown";
import { RemainingActas } from "@/components/RemainingActas";
import { loadSummary } from "@/lib/data";

// Re-read the summary on every request in dev; cached at build time on Vercel.
export const dynamic = "force-static";

export default function Home() {
  const summary = loadSummary();

  return (
    <div className="flex min-h-screen flex-col">
      <Header pctActas={summary.pct_actas} timestamp={summary.timestamp} />

      <main className="mx-auto w-full max-w-5xl flex-1 space-y-6 px-4 py-6 sm:px-6 sm:py-8">
        <Hero summary={summary} />
        <ActualVsProjected summary={summary} />
        <EvolutionChart history={summary.history} />
        <RemainingActas rows={summary.remaining_by_region} />
        <RegionDrilldown regions={summary.regions} />
        <Methodology fuentes={summary.fuente_breakdown} />
      </main>

      <footer className="border-t border-[var(--border)] bg-[var(--surface)]">
        <div className="mx-auto max-w-5xl px-4 py-4 text-xs text-[var(--muted-2)] sm:px-6">
          Datos: ONPE · Proyección: modelo propio (similitud + Monte Carlo) ·
          Resultado oficial pendiente al cierre del cómputo.
        </div>
      </footer>
    </div>
  );
}
