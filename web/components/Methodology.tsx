export function Methodology({ fuentes }: { fuentes: Record<string, number> }) {
  const total = Object.values(fuentes).reduce((a, b) => a + b, 0);

  return (
    <details className="group rounded-xl border border-[var(--border)] bg-[var(--surface)]">
      <summary className="flex cursor-pointer list-none items-center justify-between gap-4 p-5">
        <div>
          <h2 className="text-sm font-medium uppercase tracking-wider text-[var(--muted)]">
            Metodología
          </h2>
          <p className="mt-1 text-xs text-[var(--muted-2)]">
            Cómo se calcula la proyección — clic para expandir
          </p>
        </div>
        <span className="rounded-full bg-[var(--surface-2)] px-2.5 py-1 text-xs font-medium text-[var(--muted)] ring-1 ring-[var(--border-strong)] transition group-open:bg-[var(--foreground)] group-open:text-[var(--background)]">
          Detalles
        </span>
      </summary>

      <div className="space-y-4 border-t border-[var(--border)] p-5 text-sm leading-6 text-[var(--foreground)]/90">
        <div>
          <h3 className="text-sm font-semibold text-[var(--foreground)]">Datos</h3>
          <p>
            Resultados oficiales scrapeados desde la API pública de ONPE
            (<code className="rounded bg-[var(--surface-2)] px-1 py-0.5 text-xs">resultadosegundavuelta.onpe.gob.pe</code>),
            agregados a nivel distrito.
          </p>
        </div>

        <div>
          <h3 className="text-sm font-semibold text-[var(--foreground)]">Proyección por distrito</h3>
          <p>
            Para cada distrito, si el porcentaje de actas contabilizadas supera el 30 %, se usa la proporción
            actual del distrito y se escala al total esperado de votos. Si está por debajo, se cae en cascada
            a similitud, provincia, región o ámbito según haya datos suficientes.
          </p>
        </div>

        <div>
          <h3 className="text-sm font-semibold text-[var(--foreground)]">Distritos similares</h3>
          <p>
            Para distritos con pocos datos en segunda vuelta, se buscan los 20 distritos
            más parecidos según el patrón de votación de la 1ra vuelta de 2026 (similitud
            coseno sobre el vector de votos por partido). Luego se promedian las proporciones
            <em> reales </em> de FP y JP en segunda vuelta de aquellos distritos similares
            que ya tienen al menos 30 % de actas contabilizadas, ponderando por el score de
            similitud. Solo se aplica si hay al menos 3 vecinos confiables.
          </p>
        </div>

        <div>
          <h3 className="text-sm font-semibold text-[var(--foreground)]">Simulación Monte Carlo</h3>
          <p>
            Se generan 3 000 escenarios usando una distribución Dirichlet sobre las actas pendientes de cada
            distrito. La probabilidad de victoria es la fracción de simulaciones en que cada candidato termina
            con más votos válidos.
          </p>
        </div>

        {total > 0 && (
          <div>
            <h3 className="text-sm font-semibold text-[var(--foreground)]">Fuente de proyección por distrito</h3>
            <ul className="mt-2 space-y-1 text-xs text-[var(--muted)]">
              {Object.entries(fuentes)
                .sort((a, b) => b[1] - a[1])
                .map(([k, v]) => (
                  <li
                    key={k}
                    className="flex items-center justify-between rounded bg-[var(--surface-2)] px-3 py-1.5"
                  >
                    <span className="font-medium text-[var(--foreground)]">{labelFuente(k)}</span>
                    <span className="text-[var(--muted-2)]">
                      {v.toLocaleString("en-US")} ({((v / total) * 100).toFixed(1)}%)
                    </span>
                  </li>
                ))}
            </ul>
          </div>
        )}

        <p className="border-t border-[var(--border)] pt-4 text-xs text-[var(--muted-2)]">
          Esta es una proyección no oficial. Resultados oficiales: ONPE.
        </p>
      </div>
    </details>
  );
}

function labelFuente(key: string): string {
  switch (key) {
    case "distrito":
      return "Distrito (datos directos)";
    case "similitud":
      return "Distritos similares (1ra vuelta 2026)";
    case "provincia":
      return "Promedio provincial";
    case "region":
      return "Promedio regional";
    case "ambito":
      return "Promedio nacional/extranjero";
    case "default":
      return "Sin datos (50/50)";
    default:
      return key;
  }
}
