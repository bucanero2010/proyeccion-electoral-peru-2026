#!/usr/bin/env python3
"""
Snapshot manager — saves a summary row each time the scraper runs.
Tracks how the projection evolves as more actas come in.
"""

import os
import csv
import json
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
SNAPSHOT_FILE = os.path.join(DATA_DIR, "snapshots.csv")
SPECIAL = {"VOTOS EN BLANCO", "VOTOS NULOS"}

SNAPSHOT_FIELDS = [
    "timestamp", "pct_actas_global", "total_distritos", "distritos_100pct",
    "total_votos_validos", "total_votos_emitidos", "top_candidates_json",
]


def save_snapshot(df):
    """
    Save a snapshot row from the raw scraped data (before projection).
    df: pandas DataFrame of the scraped results.
    """
    import pandas as pd

    pct_actas = 0
    actas_d = df.drop_duplicates(subset=["ubigeo_distrito"])
    total_actas = actas_d["total_actas"].sum()
    contab = actas_d["actas_contabilizadas"].sum()
    if total_actas > 0:
        pct_actas = round(contab / total_actas * 100, 2)

    n_distritos = len(actas_d)
    n_100 = len(actas_d[actas_d["pct_actas_contabilizadas"] >= 100])

    # Actual vote totals (not projected)
    by_partido = df.groupby("partido")["votos"].sum().sort_values(ascending=False)
    candidatos = by_partido[[p for p in by_partido.index if p not in SPECIAL]]
    total_validos = int(candidatos.sum())
    total_emitidos = int(by_partido.sum())

    # Top 10 candidates with pct
    top = []
    for partido, votos in candidatos.head(10).items():
        top.append({
            "partido": partido,
            "votos": int(votos),
            "pct": round(votos / total_validos * 100, 3) if total_validos > 0 else 0,
        })

    row = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "pct_actas_global": pct_actas,
        "total_distritos": n_distritos,
        "distritos_100pct": n_100,
        "total_votos_validos": total_validos,
        "total_votos_emitidos": total_emitidos,
        "top_candidates_json": json.dumps(top, ensure_ascii=False),
    }

    file_exists = os.path.exists(SNAPSHOT_FILE)
    with open(SNAPSHOT_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SNAPSHOT_FIELDS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

    print(f"  Snapshot guardado: {pct_actas}% actas, {n_100}/{n_distritos} distritos al 100%")
    return row
