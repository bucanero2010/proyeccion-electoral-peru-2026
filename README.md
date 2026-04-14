# 🗳️ Proyección Electoral Presidencial Perú 2026

Herramienta para extraer, proyectar y visualizar los resultados electorales presidenciales de Perú 2026 a partir de los datos oficiales de la ONPE.

**[Ver dashboard en vivo →](https://proyeccion-elecciones-2026.streamlit.app/)**

## ¿Qué hace?

Con ~2,100 distritos electorales y actas que se contabilizan gradualmente, esta herramienta proyecta el resultado final antes de que el 100% de las actas estén contadas.

1. **Scraper** — Extrae resultados a nivel de distrito desde la API de la ONPE (paralelo, ~2,100 distritos)
2. **Proyección** — Estima el resultado final extrapolando las actas pendientes
3. **Dashboard** — Visualización interactiva con Streamlit: resultado proyectado vs actual, explorador por región, evolución temporal

## Metodología de proyección

Para cada distrito se estima el total de votos escalando proporcionalmente:

```
votos_proyectados = votos_contados × (total_actas / actas_contabilizadas)
```

La proporción de votos por candidato se elige según la confiabilidad de los datos del distrito:

| Condición | Fuente de proporción |
|---|---|
| ≥ 30% actas contabilizadas | Proporción del propio distrito |
| < 30% + datos 2021 disponibles | **Similitud 2021**: promedio ponderado de distritos que votaron similar en 2021 (cosine similarity) |
| < 30% sin similitud | Cascada geográfica: provincia → región → país |
| Extranjero < 30% | Total EXTRANJERO directamente |

### Similitud basada en elecciones 2021

Para distritos con pocas actas, en lugar de usar promedios geográficos genéricos, se buscan los distritos que votaron de forma similar en las elecciones 2021:

- Se construye un vector de participación por partido para cada distrito (18 dimensiones, elección 2021)
- Se calcula la similitud coseno entre todos los pares de distritos
- Para proyectar, se usa el promedio ponderado de los vecinos más similares que ya tengan suficientes actas en 2026

Esto captura patrones de voto que no son puramente geográficos (ej: distritos rurales andinos de diferentes regiones que votan parecido).

### Tracking temporal

Cada ejecución del scraper guarda un snapshot con el % de actas y los votos por candidato. El dashboard muestra cómo evoluciona la proyección conforme se contabilizan más actas, permitiendo evaluar si el resultado está convergiendo.

## Instalación

```bash
git clone https://github.com/bucanero2010/proyeccion-electoral-peru-2026.git
cd proyeccion-electoral-peru-2026
pip install -r requirements.txt
```

## Uso

### 1. Extraer datos de la ONPE

```bash
python3 -m src.scraper
```

Extrae resultados a nivel de distrito con 3 threads concurrentes. Toma ~30-60 min dependiendo de la API de la ONPE. Guarda el CSV en `data/` y un snapshot automático para tracking temporal.

### 2. Ver el dashboard

```bash
python3 -m streamlit run src/app.py
```

### 3. Actualizar datos

Vuelve a ejecutar el scraper periódicamente. Cada ejecución genera un nuevo CSV y agrega un snapshot al historial. El dashboard siempre usa el archivo más reciente.

## Estructura del proyecto

```
├── src/
│   ├── app.py          # Dashboard Streamlit
│   ├── scraper.py      # Extracción de datos de la ONPE (concurrente)
│   ├── projection.py   # Lógica de proyección (CLI)
│   ├── similarity.py   # Similitud entre distritos basada en 2021
│   └── snapshot.py     # Tracking temporal de snapshots
├── data/
│   ├── resultados_presidenciales_*.csv   # Datos crudos por ejecución
│   ├── snapshots.csv                     # Historial de snapshots
│   └── 2021_presidencial-*.csv           # Datos históricos 2021
├── requirements.txt
└── README.md
```

## Fuentes de datos

- [ONPE - Resultados Electorales 2026](https://resultadoelectoral.onpe.gob.pe/main/presidenciales)
- [Datos abiertos elecciones 2021](https://github.com/jmcastagnetto/2021-elecciones-generales-peru-datos-de-onpe) (para similitud entre distritos)

## Limitaciones

- Asume que las actas pendientes se distribuyen en la misma proporción que las ya contadas en el nivel geográfico elegido
- La similitud 2021 captura patrones históricos que pueden no repetirse con candidatos diferentes
- No incorpora encuestas, boca de urna, ni conteos rápidos
- Los distritos del extranjero tienen datos escasos y se proyectan con el promedio total de EXTRANJERO

## Licencia

MIT
