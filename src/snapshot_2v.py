#!/usr/bin/env python3
"""
Snapshot manager segunda vuelta — saves a row each time projection_2v runs.
Tracks how the FP vs JP projection evolves as more actas come in.
"""

import os
import csv
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
SNAPSHOT_FILE = os.path.join(DATA_DIR, "snapshots_2v.csv")

SNAPSHOT_FIELDS = [
    "timestamp",
    "pct_actas",
    "fp_votos_actuales",
    "jp_votos_actuales",
    "fp_pct_actual",
    "jp_pct_actual",
    "fp_votos_proyectados",
    "jp_votos_proyectados",
    "fp_pct_proyectado",
    "jp_pct_proyectado",
    "fp_win_prob",
    "jp_win_prob",
    "margin_mean",
    "margin_p5",
    "margin_p95",
]


def save_2v_snapshot(
    pct_actas,
    actual_fp,
    actual_jp,
    fp_total,
    jp_total,
    fp_win_prob,
    margin_stats,
):
    """Append a row to snapshots_2v.csv summarizing the current run."""
    actual_total = actual_fp + actual_jp
    proj_total = fp_total + jp_total

    row = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "pct_actas": round(pct_actas, 2),
        "fp_votos_actuales": int(actual_fp),
        "jp_votos_actuales": int(actual_jp),
        "fp_pct_actual": round(actual_fp / actual_total * 100, 3) if actual_total > 0 else 0,
        "jp_pct_actual": round(actual_jp / actual_total * 100, 3) if actual_total > 0 else 0,
        "fp_votos_proyectados": int(fp_total),
        "jp_votos_proyectados": int(jp_total),
        "fp_pct_proyectado": round(fp_total / proj_total * 100, 3) if proj_total > 0 else 0,
        "jp_pct_proyectado": round(jp_total / proj_total * 100, 3) if proj_total > 0 else 0,
        "fp_win_prob": round(fp_win_prob, 2),
        "jp_win_prob": round(100 - fp_win_prob, 2),
        "margin_mean": margin_stats.get("mean", 0),
        "margin_p5": margin_stats.get("p5", 0),
        "margin_p95": margin_stats.get("p95", 0),
    }

    file_exists = os.path.exists(SNAPSHOT_FILE)
    with open(SNAPSHOT_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SNAPSHOT_FIELDS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

    print(f"  Snapshot 2v guardado: {pct_actas:.1f}% actas, FP {row['fp_pct_proyectado']}% / JP {row['jp_pct_proyectado']}%, FP win {row['fp_win_prob']}%")
    return row


def load_snapshots():
    """Load all snapshots as a list of dicts (for embedding in summary JSON)."""
    if not os.path.exists(SNAPSHOT_FILE):
        return []
    rows = []
    with open(SNAPSHOT_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            try:
                rows.append({
                    "timestamp": r["timestamp"],
                    "pct_actas": float(r["pct_actas"]),
                    "fp_pct_actual": float(r["fp_pct_actual"]),
                    "jp_pct_actual": float(r["jp_pct_actual"]),
                    "fp_pct_proyectado": float(r["fp_pct_proyectado"]),
                    "jp_pct_proyectado": float(r["jp_pct_proyectado"]),
                    "fp_win_prob": float(r["fp_win_prob"]),
                    "jp_win_prob": float(r["jp_win_prob"]),
                    "margin_mean": float(r["margin_mean"]),
                })
            except (KeyError, ValueError):
                continue
    return rows
