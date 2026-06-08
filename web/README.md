# Dashboard segunda vuelta — FP vs JP

Next.js dashboard for the segunda vuelta presidential projection (Perú 2026).
Reads `../data/summary_2v.json` produced by `python3 -m src.projection_2v` and
renders win probability, projected vs actual votes, evolution chart, remaining
actas, region drilldown, and methodology.

## Local development

```bash
npm install
npm run dev
```

The `predev`/`prebuild` script copies the latest `../data/summary_2v.json`
into `public/data/` so the page can read it as a static asset.

## Workflow

1. Run the scraper locally:
   ```bash
   python3 -m src.scraper_2v
   ```
2. Run the projection (writes `data/summary_2v.json` and appends a row to
   `data/snapshots_2v.csv`):
   ```bash
   python3 -m src.projection_2v
   ```
3. Commit the updated JSON/CSV and `git push`. Vercel auto-deploys on push.

## Deploying to Vercel

- **Root directory**: `web`
- **Framework preset**: Next.js (auto-detected)
- **Build command**: `npm run build` (runs `scripts/copy-data.mjs` first via the `prebuild` hook)
- **Output directory**: `.next` (default)
- **Install command**: `npm install` (default)

No environment variables required.

## Project layout

```
web/
├── app/
│   ├── layout.tsx        # Root layout, fonts, metadata
│   ├── page.tsx          # Server component, loads summary_2v.json
│   └── globals.css       # Tailwind + brand colors (FP orange, JP green)
├── components/
│   ├── Header.tsx
│   ├── Hero.tsx
│   ├── ActualVsProjected.tsx
│   ├── EvolutionChart.tsx       # client component (Recharts)
│   ├── RemainingActas.tsx
│   ├── RegionDrilldown.tsx      # collapsible <details>
│   └── Methodology.tsx          # collapsible <details>
├── lib/
│   ├── types.ts          # Summary / RegionRow / HistoryRow / ...
│   ├── format.ts         # fmtInt / fmtPct / fmtTimestamp
│   └── data.ts           # loadSummary() (server-only)
├── scripts/
│   └── copy-data.mjs     # Copies ../data/summary_2v.json → public/data/
└── public/data/          # generated, git-ignored
```

## Brand colors

- **FP** (Fuerza Popular): `#f97316` (orange)
- **JP** (Juntos por el Perú): `#22c55e` (green)
