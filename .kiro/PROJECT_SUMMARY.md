# Proyección Electoral Perú 2026 — Handoff / Project Summary

> Living context document for any AI agent (or human) picking up this project.
> Written 2026-06-11. Covers both the 1ra vuelta (Streamlit) and 2da vuelta
> (Next.js/Vercel) systems, the methodology, every bug we fixed, and the open
> items. Read this top-to-bottom before touching code.

---

## 1. What this project is

A live election-forecasting toolkit for Peru's 2026 presidential election. It
scrapes ONPE's official results API at the district level (~2,102 districts),
projects the final outcome before 100% of actas are counted, and visualizes it.

There are **two independent rounds / two dashboards**:

| Round | Status | Scraper | Projection | Dashboard | Hosting |
|---|---|---|---|---|---|
| **1ra vuelta** | Final (100%) | `src/scraper.py` | `src/projection.py` + `src/montecarlo.py` | `src/app.py` (Streamlit) | Streamlit Cloud |
| **2da vuelta** | Live (~98%+) | `src/scraper_2v.py` | `src/projection_2v.py` | `web/` (Next.js) | Vercel |

- **1ra vuelta result:** FP 1st (~17%), JP 2nd (~12%), RP 3rd (~11.9%). FP vs JP
  advanced to the runoff.
- **2da vuelta (FP vs JP):** as of last update at 98.24% actas, FP ~50.14% /
  JP ~49.86%, FP win probability ~100% in the Monte Carlo. FP (Keiko Fujimori)
  is the projected winner.

GitHub: `https://github.com/bucanero2010/proyeccion-electoral-peru-2026`
Working dir: `/Users/sebalin/Documents/Projects/elecciones-2026`

---

## 2. Candidates & colors (use consistently)

- **FP** = Fuerza Popular (Keiko Fujimori). Color **orange `#f97316`** (light `#fb923c`).
- **JP** = Juntos por el Perú (Roberto Sánchez). Color **green `#22c55e`** (light `#4ade80`).

---

## 3. Repository layout

```
src/
  scraper.py          # 1ra vuelta scraper (Streamlit pipeline)
  projection.py       # 1ra vuelta projection
  montecarlo.py       # 1ra vuelta Monte Carlo
  similarity.py       # 2021-based cosine similarity (used by 1ra vuelta)
  snapshot.py         # 1ra vuelta snapshot tracking
  app.py              # Streamlit dashboard (1ra vuelta)
  scraper_2v.py       # 2da vuelta scraper (cache-aware, see §6)
  projection_2v.py    # 2da vuelta projection (similarity + Monte Carlo)
  snapshot_2v.py      # 2da vuelta snapshot tracking -> data/snapshots_2v.csv
  rescrape_1v.py      # one-off: re-scrape 1ra vuelta from live API (see §7)
web/                  # Next.js + Tailwind + Recharts dashboard (2da vuelta, dark theme)
  app/page.tsx        # server component, loads summary_2v.json
  components/         # Header, Hero, ActualVsProjected, EvolutionChart,
                      # RemainingActas (bar chart), RegionDrilldown, Methodology
  lib/                # types.ts, format.ts, data.ts
  scripts/copy-data.mjs  # prebuild/predev: copies ../data/summary_2v.json -> public/data/
data/
  resultados_2v_*.csv            # latest 2da vuelta scrape (only newest kept)
  scrape_cache_2v.csv            # persistent best-known per-district cache (see §6)
  summary_2v.json                # consumed by the Next.js dashboard
  snapshots_2v.csv               # projection evolution over time
  resultados_presidenciales_*.csv  # 1ra vuelta data (re-scraped, see §7)
  ubigeo_cache.json              # district hierarchy, shared across rounds
  2021_*.csv                     # 2021 historical data (1ra vuelta similarity)
run_loop_2v.sh        # 10-min loop: scrape -> project -> commit -> push
run_loop.sh           # 1ra vuelta equivalent (30-min, legacy)
```

---

## 4. 2da vuelta methodology (current, authoritative)

Per district, estimate total votes by scaling counted votes:
`estimated_total = counted × (total_actas / actas_contabilizadas)`.

The FP/JP **proportion** for each district is chosen by a cascade (see
`project_2v` in `src/projection_2v.py` and the on-dashboard Methodology section):

1. **distrito** — if district has ≥30% actas counted, use its own proportion.
2. **similitud** — else find the most similar districts by **2026 1ra vuelta**
   vote-share vectors (cosine similarity, top-100 candidates, average up to 10
   neighbors that already have ≥30% actas in 2v, weighted by similarity score;
   requires ≥3 reliable neighbors). NOTE: similarity is built from **2026 1ra
   vuelta** now, not 2021 (that change was deliberate — more recent = better).
3. **provincia → region → ambito** — geographic fallbacks.
4. **default 50/50** — last resort (only tiny zero-data extranjero districts).

**Vote magnitude fallback** for districts at 0% actas (typical of extranjero):
own data → regional vpa → per-district 1v vpa → ambito-wide 1v vpa. `vpa` =
valid votes per acta from 2026 1ra vuelta (`build_1v_vpa`).

**Monte Carlo (3000 sims):** for each remaining-acta district, draw a Dirichlet
centered on its projected proportion. Concentration `alpha = props × n_effective`
where `n_effective = clip(pct_actas/100 × 50, 1, 100)`. Win probability = fraction
of sims where a candidate ends with more valid votes. P5/P95 give the 90% interval
on the margin.

Key constants in `projection_2v.py`: `THRESHOLD = 30.0`, `SIM_K = 100`,
`SIM_USE = 10`, `SIM_MIN_NEIGHBORS = 3`, `N_SIM = 3000`, seed 42.

---

## 5. summary_2v.json schema (what the dashboard reads)

```
timestamp, pct_actas
fp / jp: { votos_actuales, votos_proyectados, pct_actual, pct_proyectado, win_probability }
margin: { mean, p5, p95 }                # margin = FP% - 50, from Monte Carlo
regions: [ { ambito, region, fp_votos, jp_votos, fp_pct, jp_pct, pct_actas } ]
remaining_by_region: [ {                 # ALL 30 regions, sorted by net_fp desc
   region, ambito, remaining_actas, remaining_votos,
   fp_remaining_votos, jp_remaining_votos, net_fp,   # net_fp = fp_rem - jp_rem
   fp_remaining_pct, favors, by_fuente: { distrito, similitud, ... }
} ]
fuente_breakdown: { distrito, similitud, default, ... }   # district counts
history: [ snapshot rows from snapshots_2v.csv ]
```

---

## 6. The big scraper bug we fixed (READ THIS)

**Symptom:** the projection appeared to swing wildly toward JP (e.g. a fake
"JP 99.5%" reading, and a fake JP lead at 16:14 on 2026-06-08).

**Root cause (two compounding bugs in the old `scraper_2v.py`):**
1. `api_get` only retried on exceptions, not on HTTP errors / `success=false`.
   A 500 from ONPE returned `[]` silently, indistinguishable from "no data".
2. The skip-completed logic read only the most recent CSV, so any district
   dropped in one cycle stayed dropped forever. Trujillo's ~137k FP votes
   vanished this way, swinging the projection ~70 points.

**Fix (current `scraper_2v.py`):**
- `api_get` does real exponential backoff (5 attempts) and returns a sentinel
  on hard failure so callers can tell "failed" from "empty".
- `fetch_distrito` returns `(rows, ok)`; `ok=False` means keep cached data.
- **Persistent cache** `data/scrape_cache_2v.csv` holds the best-known state per
  district across all runs. Each cycle: skip districts at 100% in the cache,
  re-scrape the rest, only replace a district's cache rows when the scrape
  returned non-empty data with **≥** as many actas as before. Transient
  failures never cause regressions.
- Run output reports `Cache updates / Cache preserved / API failures`.
- `--full` flag forces a complete re-scrape.

**Lesson for the next agent:** if you ever see total counted votes DECREASE
between snapshots, that's a scrape bug, not real data. Votes only go up.

---

## 7. The 1ra vuelta zero-votes bug we fixed

The original 1v CSV (April scrape) had **0 votes for the entire extranjero**
(scrape captured actas-counted but votes not yet published). This broke the
similarity model (zero vectors → no neighbors) and vpa (0 votes/acta).

**Fix:** `src/rescrape_1v.py` re-scrapes 1ra vuelta from the still-live API
(`resultadoelectoral.onpe.gob.pe`) using the shared `ubigeo_cache.json` so
ubigeos align between rounds. Result: Madrid 0 → 45,231 votos; 197/210
extranjero districts now have real data. This raised FP win prob meaningfully
because extranjero (65%+ FP) is now correctly modeled.

NOTE: the 1v API and the 2v API use the **same** ubigeo scheme as our cache
(e.g. Madrid = `940910`). The old broken scrape had used a different scheme
(`922802`). The PCM file `data/Resultados_1ra_vuelta_Version_PCM.csv` uses yet
another layout (mesa-level, `;`-separated, latin-1, encoding issues) and does
NOT cleanly cover extranjero — use the API rescrape, not the PCM file, for 1v.

---

## 8. The automation loop

`run_loop_2v.sh [interval_seconds]` (default 600 = 10 min):
scrape (cache-aware) → trim old CSVs → project → stage data/ → commit → push.
Vercel auto-deploys on push. Bails cleanly when nothing changed.

To run unattended despite laptop sleep:
```bash
nohup caffeinate -i ./run_loop_2v.sh > loop.log 2>&1 &
echo $! > loop.pid     # kill $(cat loop.pid) to stop
```
`caffeinate -i` prevents idle sleep but NOT lid-close sleep (macOS hardware
trigger). For lid closed: external display+keyboard+power, or Amphetamine app.
`loop.log` / `loop.pid` are gitignored.

ONPE throttles bursts; the loop logs `N fails` per cycle but the cache absorbs
them so data integrity holds. WORKERS=5 in the scraper.

---

## 9. Dashboard (web/) notes

- Next.js static-ish app, dark theme, fixed palette (FP orange / JP green).
- Sections: Header → Hero (win prob bars + margin) → Actual vs Projected →
  Evolution line chart (Recharts) → **Remaining-by-region net-contribution bar
  chart** → Region drilldown (collapsible) → Methodology (permanent, with a
  6-step cascade flowchart).
- The remaining-by-region chart: horizontal diverging bars, X = net FP votes
  (positive=FP/orange right, negative=JP/green left), each bar **stacked by
  fuente** (distrito=blue, similitud=violet, default=gray) with a legend.
  - GOTCHA we hit: a data field named `region` collided with a fuente key
    `region`, making the Y axis render "0". Fixed by renaming the category to
    `name` and prefixing fuente keys with `f_`. Don't reintroduce that.
- `scripts/copy-data.mjs` runs on predev/prebuild to copy
  `../data/summary_2v.json` into `web/public/data/`. `public/data/` is gitignored.
- Deploy: Vercel root = `web`, framework auto-detected, no env vars.

---

## 10. Formatting conventions (user preference)

- Percentages: 1 decimal + `%` sign by default (2 decimals where precision
  matters, e.g. the tight FP/JP margins).
- Numbers: comma thousands separators (e.g. `1,873,207`).
- Reply to the user in their language (often Spanish for analysis explanations).

---

## 11. Analysis / discussion highlights (context, not code)

- **Why FP wins despite trailing in counted votes:** at ~95-98% actas the
  remaining pool is dominated by FP-leaning Lima stragglers + extranjero
  (~65% FP). The JP strongholds left (Cusco, Ayacucho, Amazonas) have far
  fewer remaining votes. Net remaining ~56% FP vs a breakeven of ~52%.
- **Breakeven:** at the current pool size, FP needs ≥~52% of remaining valid
  votes to win; the model projects ~55-56% → ~4pt cushion → high win prob.
- **vs naive trend charts:** a viral chart used straight-line linear
  extrapolation of FP%-vs-%actas to predict JP winning. That's flawed: the
  FP decline over the count is a *composition effect* (urban FP areas reported
  first), not a trend that continues. Our district model projects by geography
  of what's left, not by extrapolating the slope.
- **Known model criticism (valid, from an external reviewer):**
  1. Independence between districts in the Monte Carlo → intervals likely too
     narrow → win prob (≈100% now, was ~88% earlier) is probably overconfident.
     The single highest-value improvement: add a shared national/regional swing
     term per simulation (correlated draws). NOT yet implemented.
  2. Within-district non-stationarity (late mesas differ from early). Mostly a
     between-district effect, which we already capture; residual is small at
     high % actas. Our live data is district-level only — no mesa granularity —
     so we can't detect rural-vs-urban of the *remaining* mesas directly. The
     PCM file has 1v mesa-level data that could characterize within-district
     dispersion if someone wants to pursue this.
  3. The 30% threshold and Dirichlet `n_effective=50` are not historically
     calibrated. We now have ~100 snapshot readings in `snapshots_2v.csv` that
     could be used to calibrate the Dirichlet concentration to observed
     volatility.

---

## 12. Open / suggested next steps (ranked)

1. **Correlated Monte Carlo** (highest leverage): replace independent per-
   district Dirichlet draws with a shared swing term so win probability and the
   P5/P95 band become honest. Directly answers the main statistical critique.
2. **Calibrate Dirichlet concentration** against `snapshots_2v.csv` history.
3. Optional: port the breakeven-vs-turnout analysis (padrón ~25.29M, turnout
   ~78%) into a permanent dashboard component or `src/` script. It was built
   ad-hoc and not committed.
4. Optional: within-district dispersion model from PCM mesa data.

---

## 13. Environment / gotchas

- Python 3 with `requests`, `pandas`, `numpy`, `streamlit`, `plotly`,
  `matplotlib` installed via `--break-system-packages`. Use `python3 -m pip`.
- Node v25 / npm 11. `web/` builds with `npm run build`.
- ONPE API requires headers `Sec-Fetch-Dest/Mode/Site` or it returns HTML.
- 2v domain: `resultadosegundavuelta.onpe.gob.pe`; 1v domain:
  `resultadoelectoral.onpe.gob.pe`. Both `idEleccion=10`, `idAmbitoGeografico`
  1=PERÚ, 2=EXTRANJERO.
- Git: push to `main`, Vercel + Streamlit auto-deploy. Never force-push.
