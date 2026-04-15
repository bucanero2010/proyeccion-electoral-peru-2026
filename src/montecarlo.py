#!/usr/bin/env python3
"""
Monte Carlo simulation for election projection uncertainty.
Run after the scraper to pre-compute position probabilities and confidence intervals.
Saves results to data/montecarlo.json.
"""

import os
import sys
import json
import glob
import numpy as np
import pandas as pd
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
SPECIAL = {"VOTOS EN BLANCO", "VOTOS NULOS"}
N_SIM = 5000


def run(threshold=30):
    from app import load_data, build_hierarchy, project
    from similarity import build_similarity_index

    files = sorted(glob.glob(os.path.join(DATA_DIR, "resultados_presidenciales_*.csv")))
    if not files:
        print("No data file found.")
        return

    filename = files[-1]
    print(f"Loading: {os.path.basename(filename)}")

    df = load_data(filename)
    h = build_hierarchy(df)

    sim_file = os.path.join(DATA_DIR, "2021_presidencial-resultados-partidos.csv")
    sim_index = None
    if os.path.exists(sim_file):
        sim_index, _ = build_similarity_index(k=20)

    proj = project(df, h, threshold, sim_index=sim_index)

    meta = {"ambito", "region", "provincia", "distrito", "total_actas",
            "actas_contabilizadas", "pct_actas", "fuente"}
    partido_cols = sorted([c for c in proj.columns if c not in meta])
    non_special = [p for p in partido_cols if p not in SPECIAL]
    n_partidos = len(partido_cols)

    # Projected and counted votes matrices
    projected = proj[partido_cols].fillna(0).values.astype(float)

    votos_pivot = df.pivot_table(
        index=["ambito", "region", "provincia", "distrito"],
        columns="partido", values="votos", aggfunc="sum", fill_value=0
    ).reindex(columns=partido_cols, fill_value=0)
    proj_keys = list(zip(proj["ambito"], proj["region"], proj["provincia"], proj["distrito"]))
    votos_pivot = votos_pivot.reindex(pd.MultiIndex.from_tuples(proj_keys), fill_value=0)
    counted = votos_pivot.values.astype(float)

    remaining = np.clip(projected.sum(axis=1) - counted.sum(axis=1), 0, None)
    proj_totals = projected.sum(axis=1, keepdims=True)
    proj_props = np.where(proj_totals > 0, projected / proj_totals, 0)
    pct_actas = proj["pct_actas"].values.astype(float)
    n_effective = np.clip(pct_actas / 100 * 50, 1, 100)

    sim_mask = np.where(remaining > 10)[0]
    fixed_totals = counted.sum(axis=0)
    s_props = proj_props[sim_mask]
    s_remaining = remaining[sim_mask]
    s_n_eff = n_effective[sim_mask]

    print(f"Running {N_SIM:,} simulations for {len(sim_mask)} distritos...")
    np.random.seed(42)
    sim_results = np.zeros((N_SIM, n_partidos))
    for s in range(N_SIM):
        extra = np.zeros(n_partidos)
        for i in range(len(sim_mask)):
            alpha = s_props[i] * s_n_eff[i] + 0.001
            extra += np.random.dirichlet(alpha) * s_remaining[i]
        sim_results[s] = fixed_totals + extra

    # Compute stats for non-special candidates
    non_special_idx = [partido_cols.index(p) for p in non_special]
    stats = {}
    for r in range(len(non_special)):
        idx = non_special_idx[r]
        vals = sim_results[:, idx]
        stats[non_special[r]] = {
            "mean": round(float(vals.mean())),
            "std": round(float(vals.std())),
            "p5": round(float(np.percentile(vals, 5))),
            "p95": round(float(np.percentile(vals, 95))),
        }

    # Position probabilities
    positions = {1: {}, 2: {}, 3: {}}
    pos_counts = {1: defaultdict(int), 2: defaultdict(int), 3: defaultdict(int)}
    for s in range(N_SIM):
        vals = [(non_special[i], sim_results[s, non_special_idx[i]]) for i in range(len(non_special))]
        vals.sort(key=lambda x: -x[1])
        for pos in [1, 2, 3]:
            pos_counts[pos][vals[pos - 1][0]] += 1

    for pos in [1, 2, 3]:
        for p, c in pos_counts[pos].items():
            prob = round(c / N_SIM * 100, 2)
            if prob >= 0.1:
                positions[pos][p] = prob

    # Sort stats by mean
    stats = dict(sorted(stats.items(), key=lambda x: -x[1]["mean"]))

    result = {
        "n_sim": N_SIM,
        "threshold": threshold,
        "source_file": os.path.basename(filename),
        "stats": stats,
        "positions": {str(k): v for k, v in positions.items()},
    }

    out_file = os.path.join(DATA_DIR, "montecarlo.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"Saved: {out_file}")

    # Print summary
    print(f"\n{'Partido':<50} {'Media':>12} {'[P5':>12} {'P95]':>12}")
    for p, s in list(stats.items())[:8]:
        print(f"{p:<50} {s['mean']:>12,} {s['p5']:>12,} {s['p95']:>12,}")

    print(f"\n2do lugar:")
    for p, prob in sorted(positions[2].items(), key=lambda x: -x[1]):
        print(f"  {p}: {prob:.2f}%")


if __name__ == "__main__":
    run()
