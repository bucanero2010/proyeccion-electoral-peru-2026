export function Methodology({ fuentes }: { fuentes: Record<string, number> }) {
  const total = Object.values(fuentes).reduce((a, b) => a + b, 0);

  return (
    <details className="group rounded-xl border border-zinc-200 bg-white">
      <summary className="flex cursor-pointer list-none items-center justify-between gap-4 p-5">
        <div>
          <h2 className="text-sm font-medium uppercase tracking-wider text-zinc-500">
            Metodología
          </h2>
          <p className="mt-1 text-xs text-zinc-500">
            Cómo se calcula la proyección — clic para expandir
          </p>
        </div>
        <span className="rounded-full bg-zinc-100 px-2.5 py-1 text-xs font-medium text-zinc-700 transition group-open:bg-zinc-900 group-open:text-white">
          Detalles
        </span>
      </summary>

      <div className="space-y-4 border-t border-zinc-100 p-5 text-sm leading-6 text-zinc-700">
        <div>
          <h3 className="text-sm font-semibold text-zinc-900">Datos</h3>
          <p>
            Resultados oficiales scrapeados desde la API pública de ONPE
            (<code className="rounded bg-zinc-100 px-1 py-0.5 text-xs">resultadosegundavuelta.onpe.gob.pe</code>),
            agregados a nivel distrito.
          </p>
        </div>

        <div>
          <h3 className="text-sm font-semibold text-zinc-900">Proyección por distrito</h3>
          <p>
            Para cada distrito, si el porcentaje de actas contabilizadas supera el 30 %, se usa la proporción
            actual del distrito y se escala al total esperado de votos. Si está por debajo, se cae en cascada
            a similitud, provincia, región o ámbito según haya datos suficientes.
          </p>
        </div>

        <div>
          <h3 className="text-sm font-semibold text-zinc-900">Similitud con primera vuelta</h3>
          <p>
            Para distritos con pocos datos en segunda vuelta, se usa la proporción
            FP / (FP + JP) de la primera vuelta como aproximación inicial. Esto evita
            sesgos por el orden en que llegan las actas.
          </p>
        </div>

        <div>
          <h3 className="text-sm font-semibold text-zinc-900">Simulación Monte Carlo</h3>
          <p>
            Se generan 3 000 escenarios usando una distribución Dirichlet sobre las actas pendientes de cada
            distrito. La probabilidad de victoria es la fracción de simulaciones en que cada candidato termina
            con más votos válidos.
          </p>
        </div>

        {total > 0 && (
          <div>
            <h3 className="text-sm font-semibold text-zinc-900">Fuente de proyección por distrito</h3>
            <ul className="mt-2 space-y-1 text-xs text-zinc-600">
              {Object.entries(fuentes)
                .sort((a, b) => b[1] - a[1])
                .map(([k, v]) => (
                  <li key={k} className="flex items-center justify-between rounded bg-zinc-50 px-3 py-1.5">
                    <span className="font-medium text-zinc-700">{labelFuente(k)}</span>
                    <span className="text-zinc-500">
                      {v.toLocaleString("en-US")} ({((v / total) * 100).toFixed(1)}%)
                    </span>
                  </li>
                ))}
            </ul>
          </div>
        )}

        <p className="border-t border-zinc-100 pt-4 text-xs text-zinc-500">
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
    case "1v_similitud":
      return "Similitud con primera vuelta";
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
