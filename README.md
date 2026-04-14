# 🗳️ Proyección Electoral Presidencial Perú 2026

Herramienta para extraer, proyectar y visualizar los resultados electorales presidenciales de Perú 2026 a partir de los datos oficiales de la ONPE.

## ¿Qué hace?

1. **Scraper** — Extrae resultados a nivel de distrito desde la API de la ONPE
2. **Proyección** — Estima el resultado final extrapolando las actas pendientes
3. **Dashboard** — Visualización interactiva con Streamlit

## Instalación

```bash
git clone https://github.com/<tu-usuario>/proyeccion-electoral-peru-2026.git
cd proyeccion-electoral-peru-2026
pip install -r requirements.txt
```

## Uso

### 1. Extraer datos de la ONPE

```bash
python -m src.scraper
```

Genera un CSV con los resultados a nivel de distrito en `data/`.
Toma ~2-3 horas dependiendo de la velocidad de la API de la ONPE.

### 2. Proyectar resultados

```bash
python -m src.projection
```

Genera CSVs con la proyección por distrito y el resumen nacional en `data/`.

### 3. Dashboard interactivo

```bash
python -m streamlit run src/app.py
```

Abre el navegador en `http://localhost:8501`.

## Metodología

Para cada distrito, se proyectan los votos faltantes asumiendo que las actas pendientes
se distribuyen en la misma proporción que las ya contabilizadas:

- Si el distrito tiene ≥ 30% de actas contabilizadas → usa su propia proporción
- Si < 30% → usa la proporción de la provincia, región o país (el primer nivel que supere el 30%)
- Excepción: distritos del extranjero usan directamente la proporción total de EXTRANJERO

El umbral del 30% es configurable desde el dashboard.

## Estructura del proyecto

```
├── src/
│   ├── scraper.py      # Extracción de datos de la ONPE
│   ├── projection.py   # Lógica de proyección electoral
│   └── app.py          # Dashboard Streamlit
├── data/               # CSVs generados (gitignored)
├── requirements.txt
└── README.md
```

## Fuente de datos

[ONPE - Resultados Electorales](https://resultadoelectoral.onpe.gob.pe/main/presidenciales)

## Licencia

MIT
