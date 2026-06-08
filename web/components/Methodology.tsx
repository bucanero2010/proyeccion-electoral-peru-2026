type CascadeStep = {
  id: string;
  title: string;
  description: string;
  condition: string;
  color: string;
};

const CASCADE: CascadeStep[] = [
  {
    id: "distrito",
    title: "1. Datos del distrito",
    description:
      "Si el distrito tiene ≥30 % de actas contabilizadas, se usa su propia proporción FP/JP y se escala al total esperado.",
    condition: "pct_actas ≥ 30 %",
    color: "from-emerald-500/40 to-emerald-500/10",
  },
  {
    id: "similitud",
    title: "2. Distritos similares",
    description:
      "Si no, buscamos los 100 distritos más parecidos según la 1ra vuelta de 2026 (similaridad coseno sobre el voto por partido). Promediamos las proporciones reales de FP y JP en 2v de hasta 10 vecinos con ≥30 % de actas, ponderado por el score. Requiere al menos 3 vecinos confiables.",
    condition: "≥ 3 vecinos confiables encontrados",
    color: "from-sky-500/40 to-sky-500/10",
  },
  {
    id: "provincia",
    title: "3. Promedio provincial",
    description:
      "Si la similitud no encuentra suficientes vecinos, usamos la proporción agregada de la provincia (siempre que tenga ≥30 % de actas).",
    condition: "provincia con pct_actas ≥ 30 %",
    color: "from-indigo-500/30 to-indigo-500/10",
  },
  {
    id: "region",
    title: "4. Promedio regional",
    description:
      "Si la provincia tampoco aplica, recurrimos a la región (departamento) con la misma regla.",
    condition: "región con pct_actas ≥ 30 %",
    color: "from-violet-500/30 to-violet-500/10",
  },
  {
    id: "ambito",
    title: "5. Promedio del ámbito",
    description:
      "Como último recurso geográfico se promedia el ámbito completo (Perú o Extranjero).",
    condition: "ámbito con pct_actas ≥ 30 %",
    color: "from-fuchsia-500/30 to-fuchsia-500/10",
  },
  {
    id: "default",
    title: "6. Default 50/50",
    description:
      "Si nada de lo anterior aplica (típico de distritos del extranjero al inicio del conteo), se asume 50/50. Estos suelen ser pocos votos y no mueven el resultado total.",
    condition: "ningún criterio anterior aplica",
    color: "from-zinc-500/30 to-zinc-500/10",
  },
];

export function Methodology({ fuentes }: { fuentes: Record<string, number> }) {
  const total = Object.values(fuentes).reduce((a, b) => a + b, 0);

  return (
    <section className="rounded-xl border border-[var(--border)] bg-[var(--surface)] p-5 sm:p-6">
      <h2 className="text-sm font-medium uppercase tracking-wider text-[var(--muted)]">
        Metodología
      </h2>
      <p className="mt-1 text-xs text-[var(--muted-2)]">
        Cómo se proyecta cada distrito desde los datos de ONPE
      </p>

      <div className="mt-6 space-y-3">
        <h3 className="text-xs font-semibold uppercase tracking-wider text-[var(--muted)]">
          Cascada de proyección por distrito
        </h3>
        <ol className="space-y-2">
          {CASCADE.map((step, idx) => (
            <li key={step.id} className="relative">
              <div
                className={`relative rounded-lg border border-[var(--border)] bg-gradient-to-r ${step.color} p-4`}
              >
                <div className="flex items-start gap-3">
                  <span className="mt-0.5 inline-flex size-6 shrink-0 items-center justify-center rounded-full bg-[var(--background)] text-xs font-semibold text-[var(--foreground)] ring-1 ring-[var(--border-strong)]">
                    {idx + 1}
                  </span>
                  <div className="min-w-0 flex-1">
                    <div className="flex flex-wrap items-baseline justify-between gap-x-3 gap-y-1">
                      <h4 className="text-sm font-semibold text-[var(--foreground)]">
                        {step.title.replace(/^\d+\.\s*/, "")}
                      </h4>
                      <span className="font-mono text-xs text-[var(--muted)]">
                        {step.condition}
                      </span>
                    </div>
                    <p className="mt-1 text-sm leading-6 text-[var(--foreground)]/85">
                      {step.description}
                    </p>
                    {fuentes[step.id] !== undefined && total > 0 && (
                      <p className="mt-2 text-xs font-mono text-[var(--muted-2)]">
                        {fuentes[step.id].toLocaleString("en-US")} distritos ·{" "}
                        {((fuentes[step.id] / total) * 100).toFixed(1)} %
                      </p>
                    )}
                  </div>
                </div>
              </div>
              {idx < CASCADE.length - 1 && (
                <div
                  className="ml-3 flex h-3 items-center justify-start"
                  aria-hidden
                >
                  <svg
                    width="12"
                    height="12"
                    viewBox="0 0 12 12"
                    className="text-[var(--border-strong)]"
                  >
                    <path
                      d="M6 1 V 11 M2 7 L 6 11 L 10 7"
                      stroke="currentColor"
                      strokeWidth="1.5"
                      fill="none"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                    />
                  </svg>
                </div>
              )}
            </li>
          ))}
        </ol>
      </div>

      <div className="mt-8 grid grid-cols-1 gap-5 border-t border-[var(--border)] pt-6 md:grid-cols-2">
        <div>
          <h3 className="text-xs font-semibold uppercase tracking-wider text-[var(--muted)]">
            Magnitud de votos
          </h3>
          <p className="mt-2 text-sm leading-6 text-[var(--foreground)]/85">
            Para estimar cuántos votos faltan en cada distrito usamos la cantidad de actas
            esperadas por el votos-válidos-por-acta. Si el distrito ya cuenta con datos en
            2v se usa su propio ratio. Si está al 0 % (típico del extranjero), se cae al
            promedio regional, y como último recurso al ratio del mismo distrito en la 1ra
            vuelta de 2026.
          </p>
        </div>

        <div>
          <h3 className="text-xs font-semibold uppercase tracking-wider text-[var(--muted)]">
            Probabilidad de victoria
          </h3>
          <p className="mt-2 text-sm leading-6 text-[var(--foreground)]/85">
            Se corren 3 000 simulaciones Monte Carlo distribuyendo las actas pendientes
            de cada distrito con una Dirichlet centrada en su proporción proyectada. La
            probabilidad de cada candidato es la fracción de simulaciones en que termina
            con más votos válidos.
          </p>
        </div>
      </div>

      <p className="mt-6 border-t border-[var(--border)] pt-4 text-xs text-[var(--muted-2)]">
        Datos: ONPE · Esta es una proyección no oficial. Resultados oficiales: ONPE.
      </p>
    </section>
  );
}
