#!/usr/bin/env python3
"""
Proyección segunda vuelta presidencial - ONPE Perú 2026
FP vs JP — uses 2026 1ra vuelta vote-share vectors to find similar
districts, then averages the actual 2v proportions of those neighbors
that already have enough actas counted.
"""

import os
import sys
import glob
import csv
import json
import numpy as np
import pandas as pd
from datetime import datetime
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

from snapshot_2v import save_2v_snapshot, load_snapshots  # noqa: E402

THRESHOLD = 30.0
SIM_K = 100  # candidate pool of similar districts (we walk this list)
SIM_USE = 10  # max reliable neighbors to actually average
SIM_MIN_NEIGHBORS = 3  # minimum reliable neighbors required to use similarity


def load_2v_data():
    files = sorted(glob.glob(os.path.join(DATA_DIR, "resultados_2v_*.csv")))
    if not files:
        print("No 2v data. Run: python3 -m src.scraper_2v")
        return None
    f = files[-1]
    print(f"Loading 2v: {os.path.basename(f)}")
    df = pd.read_csv(f, dtype={"ubigeo_departamento": str, "ubigeo_provincia": str, "ubigeo_distrito": str})
    for col in ["votos", "total_actas", "actas_contabilizadas", "pct_actas_contabilizadas"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def load_1v_final():
    """Load 1st round final results to use as similarity basis."""
    files = sorted(glob.glob(os.path.join(DATA_DIR, "resultados_presidenciales_*.csv")),
                   key=lambda f: os.path.getsize(f), reverse=True)
    if not files:
        return None
    df = pd.read_csv(files[0], dtype={"ubigeo_departamento": str, "ubigeo_provincia": str, "ubigeo_distrito": str})
    df["votos"] = pd.to_numeric(df["votos"], errors="coerce").fillna(0)
    return df


def build_1v_similarity_index(df_1v, k=SIM_K):
    """
    Build a similarity index from the 2026 1ra vuelta results.

    For each distrito, compute its vote-share vector across all 1v
    parties (excluding blancos/nulos), then return the top-K most
    similar distritos by cosine similarity.

    Returns:
        sim_index: dict {ubigeo: [(neighbor_ubigeo, score), ...]}
    """
    special = {"VOTOS EN BLANCO", "VOTOS NULOS"}
    df = df_1v[~df_1v["partido"].isin(special)].copy()

    # Pivot to (ubigeo x partido) vote totals
    pivot = df.pivot_table(
        index="ubigeo_distrito",
        columns="partido",
        values="votos",
        aggfunc="sum",
        fill_value=0,
    )
    ubigeos = pivot.index.tolist()
    vectors = pivot.values.astype(float)

    # Normalize each row to a vote-share vector (rows summing to 1)
    totals = vectors.sum(axis=1, keepdims=True)
    totals = np.where(totals == 0, 1.0, totals)
    shares = vectors / totals

    # Cosine similarity
    norms = np.linalg.norm(shares, axis=1, keepdims=True)
    norms = np.where(norms == 0, 1.0, norms)
    normalized = shares / norms
    sim = normalized @ normalized.T
    np.nan_to_num(sim, copy=False, nan=0.0, posinf=1.0, neginf=0.0)

    # Top-K neighbors per distrito (excluding self)
    sim_index = {}
    for i, ub in enumerate(ubigeos):
        scores = sim[i].copy()
        scores[i] = -1.0
        # Take more than K just in case k > n_districts
        n_take = min(k, len(ubigeos) - 1)
        top = np.argpartition(-scores, n_take - 1)[:n_take]
        # Sort that subset descending
        top = top[np.argsort(-scores[top])]
        sim_index[ub] = [(ubigeos[j], float(scores[j])) for j in top if scores[j] > 0]
    return sim_index


def build_1v_vpa(df_1v):
    """
    Compute valid votes per counted acta for every district from 1ra vuelta 2026.
    Used as a magnitude estimator for districts where the 2v counted=0
    and the regional vpa is also zero (typical of extranjero at 0% actas).
    """
    special = {"VOTOS EN BLANCO", "VOTOS NULOS"}
    valid = df_1v[~df_1v["partido"].isin(special)]
    votes = valid.groupby("ubigeo_distrito")["votos"].sum()
    actas = df_1v.groupby("ubigeo_distrito")["actas_contabilizadas"].max()
    vpa = (votes / actas).replace([np.inf, -np.inf], np.nan).fillna(0)
    return vpa.to_dict()


def lookup_similarity_props(target_ubigeo, sim_index, district_props_2v, district_pcts_2v, threshold):
    """
    Average the actual 2v FP/JP proportions across the most similar
    districts that already have enough 2v actas counted.

    Args:
        target_ubigeo: ubigeo we want to project
        sim_index: precomputed similarity index from 1v 2026
        district_props_2v: {ubigeo: {partido: prop}} for 2v districts
        district_pcts_2v: {ubigeo: pct_actas} for 2v districts
        threshold: minimum pct_actas to treat a neighbor as reliable

    Returns:
        ({"FUERZA POPULAR": x, "JUNTOS POR EL PERÚ": y}, n_used) or (None, 0)
    """
    if target_ubigeo not in sim_index:
        return None, 0

    weighted = defaultdict(float)
    total_weight = 0.0
    n_used = 0
    # Walk the candidate list (sorted by descending similarity) and
    # collect up to SIM_USE neighbors that already have enough 2v actas.
    for neighbor_ub, score in sim_index[target_ubigeo]:
        if score <= 0:
            break
        if district_pcts_2v.get(neighbor_ub, 0) < threshold:
            continue
        props = district_props_2v.get(neighbor_ub)
        if not props:
            continue
        for partido, prop in props.items():
            weighted[partido] += prop * score
        total_weight += score
        n_used += 1
        if n_used >= SIM_USE:
            break

    if total_weight == 0 or n_used < SIM_MIN_NEIGHBORS:
        return None, n_used

    return {p: v / total_weight for p, v in weighted.items()}, n_used


def project_2v(df_2v, sim_index=None, vpa_1v=None):
    """Project 2nd round results using similarity from 1ra vuelta 2026.

    For low-actas districts, the cascade is:
        similar districts (1v 2026 vectors, ≥3 reliable 2v neighbors)
        → provincia → region → ambito → 50/50 default.

    Vote magnitudes (votes per acta) are estimated from the district's
    own counted actas first, then regional avg, then 1ra vuelta 2026 vpa
    (so we still get a count for districts at 0%).
    """
    geo = ["ambito", "region", "provincia", "distrito"]

    # Distrito-level actas
    actas_d = df_2v.groupby(geo + ["ubigeo_distrito"]).agg(
        total_actas=("total_actas", "first"),
        actas_contab=("actas_contabilizadas", "first"),
        pct_actas=("pct_actas_contabilizadas", "first"),
    ).reset_index()

    # Votes by partido at distrito level
    votos_d = df_2v.pivot_table(index=geo + ["ubigeo_distrito"], columns="partido",
                                 values="votos", aggfunc="sum", fill_value=0).reset_index()

    # Merge
    merged = actas_d.merge(votos_d, on=geo + ["ubigeo_distrito"])

    # Identify candidates (exclude blancos/nulos)
    special = {"VOTOS EN BLANCO", "VOTOS NULOS"}
    candidates = [c for c in votos_d.columns if c not in geo + ["ubigeo_distrito"] and c not in special]

    # Precompute actual 2v proportions per district (for similarity averaging)
    district_props_2v = {}
    district_pcts_2v = {}
    for _, row in merged.iterrows():
        ub = row["ubigeo_distrito"]
        district_pcts_2v[ub] = row["pct_actas"]
        total_valid = sum(row.get(c, 0) for c in candidates)
        if total_valid > 0:
            district_props_2v[ub] = {c: row.get(c, 0) / total_valid for c in candidates}

    # Geographic aggregations for fallback
    def compute_pct_and_props(df_sub, group_cols):
        agg = df_sub.groupby(group_cols).agg(
            total_actas=("total_actas", "sum"),
            actas_contab=("actas_contab", "sum"),
        ).reset_index()
        agg["pct_actas"] = (agg["actas_contab"] / agg["total_actas"] * 100).fillna(0)

        # Proportions
        for c in candidates:
            if c in df_sub.columns:
                agg[c] = df_sub.groupby(group_cols)[c].sum().values
        total_c = agg[candidates].sum(axis=1)
        for c in candidates:
            agg[f"prop_{c}"] = (agg[c] / total_c).fillna(0)
        return agg

    actas_p = compute_pct_and_props(merged, ["ambito", "region", "provincia"])
    actas_r = compute_pct_and_props(merged, ["ambito", "region"])
    actas_a = compute_pct_and_props(merged, ["ambito"])

    # Avg votes per acta at various levels
    total_votos = df_2v.groupby(geo + ["ubigeo_distrito"])["votos"].sum().reset_index()
    total_votos.columns = geo + ["ubigeo_distrito", "total_votos_dist"]
    merged = merged.merge(total_votos, on=geo + ["ubigeo_distrito"], how="left")

    results = []
    for _, row in merged.iterrows():
        pct = row["pct_actas"]
        total_actas = row["total_actas"]
        actas_contab = row["actas_contab"]
        ub = row["ubigeo_distrito"]

        # Determine proportions source
        if pct >= 100 or total_actas == 0 or pct >= THRESHOLD:
            source = "distrito"
            total_valid = sum(row.get(c, 0) for c in candidates)
            props = {c: row.get(c, 0) / total_valid if total_valid > 0 else 0.5 for c in candidates}
        else:
            # Cascade: provincia → region → ambito, or use 1st round share
            key_p = (row["ambito"], row["region"], row["provincia"])
            key_r = (row["ambito"], row["region"])
            key_a = (row["ambito"],)

            p_row = actas_p[(actas_p["ambito"] == key_p[0]) & (actas_p["region"] == key_p[1]) & (actas_p["provincia"] == key_p[2])]
            r_row = actas_r[(actas_r["ambito"] == key_r[0]) & (actas_r["region"] == key_r[1])]
            a_row = actas_a[actas_a["ambito"] == key_a[0]]

            sim_props = None
            if sim_index is not None:
                sim_props, _ = lookup_similarity_props(
                    ub, sim_index, district_props_2v, district_pcts_2v, THRESHOLD
                )

            if sim_props is not None:
                # Average actual 2v proportions of similar districts
                # (similarity from 2026 1ra vuelta vectors)
                props = {c: sim_props.get(c, 0.0) for c in candidates}
                source = "similitud"
            elif len(p_row) > 0 and p_row.iloc[0]["pct_actas"] >= THRESHOLD:
                props = {c: p_row.iloc[0][f"prop_{c}"] for c in candidates}
                source = "provincia"
            elif len(r_row) > 0 and r_row.iloc[0]["pct_actas"] >= THRESHOLD:
                props = {c: r_row.iloc[0][f"prop_{c}"] for c in candidates}
                source = "region"
            elif len(a_row) > 0 and a_row.iloc[0]["pct_actas"] >= THRESHOLD:
                props = {c: a_row.iloc[0][f"prop_{c}"] for c in candidates}
                source = "ambito"
            else:
                props = {c: 0.5 for c in candidates}
                source = "default"

        # Estimate total votes
        counted = sum(row.get(c, 0) for c in candidates)
        if actas_contab > 0 and total_actas > 0:
            estimated_total = counted * (total_actas / actas_contab)
        elif total_actas > 0 and counted == 0:
            # Cascade for vote-magnitude estimate when this district has 0 counted actas.
            # 1) regional vpa from 2v if any 2v acta in the region was counted
            r_sub = merged[(merged["ambito"] == row["ambito"]) & (merged["region"] == row["region"])]
            r_contab = r_sub["actas_contab"].sum()
            if r_contab > 0:
                avg_vpa = r_sub["total_votos_dist"].sum() / r_contab
                estimated_total = avg_vpa * total_actas
            elif vpa_1v is not None and vpa_1v.get(ub, 0) > 0:
                # 2) per-district vpa from 1ra vuelta 2026 (covers 0%-actas regions like extranjero)
                estimated_total = vpa_1v[ub] * total_actas
            elif vpa_1v is not None:
                # 3) last resort: ambito-wide vpa from 1ra vuelta 2026
                ambito_ubigeos = merged[merged["ambito"] == row["ambito"]]["ubigeo_distrito"].tolist()
                ambito_vpas = [vpa_1v.get(u, 0) for u in ambito_ubigeos if vpa_1v.get(u, 0) > 0]
                avg_vpa = sum(ambito_vpas) / len(ambito_vpas) if ambito_vpas else 0
                estimated_total = avg_vpa * total_actas
            else:
                estimated_total = 0
        else:
            estimated_total = counted

        result_row = {
            "ambito": row["ambito"], "region": row["region"],
            "provincia": row["provincia"], "distrito": row["distrito"],
            "ubigeo_distrito": ub,
            "total_actas": int(total_actas) if pd.notna(total_actas) else 0,
            "actas_contabilizadas": int(actas_contab) if pd.notna(actas_contab) else 0,
            "pct_actas": round(pct, 2) if pd.notna(pct) else 0.0,
            "fuente": source,
        }
        for c in candidates:
            result_row[c] = round(estimated_total * props.get(c, 0)) if pd.notna(estimated_total) else 0
        results.append(result_row)

    return pd.DataFrame(results)


def main():
    df_2v = load_2v_data()
    if df_2v is None:
        return

    df_1v = load_1v_final()
    sim_index = None
    vpa_1v = None
    if df_1v is not None:
        sim_index = build_1v_similarity_index(df_1v, k=SIM_K)
        avg_neighbors = np.mean([len(v) for v in sim_index.values()])
        print(f"1v similarity index built: {len(sim_index)} districts, avg {avg_neighbors:.1f} neighbors each")
        vpa_1v = build_1v_vpa(df_1v)
        print(f"1v vpa loaded for {len(vpa_1v)} districts")

    proj = project_2v(df_2v, sim_index, vpa_1v)

    # Results
    candidates = ["FUERZA POPULAR", "JUNTOS POR EL PERÚ"]
    fp_total = proj["FUERZA POPULAR"].sum()
    jp_total = proj["JUNTOS POR EL PERÚ"].sum()
    total_valid = fp_total + jp_total

    pct_actas = proj["actas_contabilizadas"].sum() / proj["total_actas"].sum() * 100

    print(f"\n{'='*60}")
    print(f"PROYECCIÓN SEGUNDA VUELTA ({pct_actas:.1f}% actas)")
    print(f"{'='*60}")
    print(f"  FUERZA POPULAR:     {int(fp_total):>12,}  ({fp_total/total_valid*100:.2f}%)")
    print(f"  JUNTOS POR EL PERÚ: {int(jp_total):>12,}  ({jp_total/total_valid*100:.2f}%)")
    print(f"  Gap FP-JP:          {int(fp_total-jp_total):>+12,}")
    print(f"\n  Fuentes:")
    print(proj["fuente"].value_counts().to_string())

    # Monte Carlo
    print(f"\n  Running Monte Carlo (3000 sims)...")
    remaining_per = np.clip(
        proj[candidates].values - df_2v.pivot_table(
            index=["ambito","region","provincia","distrito"],
            columns="partido", values="votos", aggfunc="sum", fill_value=0
        ).reindex(columns=candidates, fill_value=0).reindex(
            pd.MultiIndex.from_frame(proj[["ambito","region","provincia","distrito"]]),
            fill_value=0
        ).values,
        0, None
    )
    remaining_total = remaining_per.sum(axis=1)
    rem_props = np.where(remaining_total[:, None] > 0, remaining_per / remaining_total[:, None], 0.5)
    pct_actas_arr = proj["pct_actas"].values
    n_effective = np.clip(pct_actas_arr / 100 * 50, 1, 100)

    counted = df_2v[df_2v["partido"].isin(candidates)].pivot_table(
        index=["ambito","region","provincia","distrito"],
        columns="partido", values="votos", aggfunc="sum", fill_value=0
    ).reindex(columns=candidates, fill_value=0).reindex(
        pd.MultiIndex.from_frame(proj[["ambito","region","provincia","distrito"]]),
        fill_value=0
    ).values
    fixed = counted.sum(axis=0)

    sim_mask = np.where(remaining_total > 10)[0]
    N_SIM = 3000
    np.random.seed(42)
    fp_wins = 0
    margins = []

    for s in range(N_SIM):
        extra = np.zeros(2)
        for i in sim_mask:
            alpha = rem_props[i] * n_effective[i] + 0.001
            extra += np.random.dirichlet(alpha) * remaining_total[i]
        totals = fixed + extra
        fp_pct = totals[0] / totals.sum() * 100
        margin = fp_pct - 50
        margins.append(margin)
        if totals[0] > totals[1]:
            fp_wins += 1

    margins = np.array(margins)
    fp_win_prob = fp_wins / N_SIM * 100
    margin_stats = {
        "mean": round(float(margins.mean()), 3),
        "p5": round(float(np.percentile(margins, 5)), 3),
        "p95": round(float(np.percentile(margins, 95)), 3),
    }
    print(f"\n  FP wins: {fp_win_prob:.2f}%")
    print(f"  JP wins: {(N_SIM-fp_wins)/N_SIM*100:.2f}%")
    print(f"  FP margin: mean={margin_stats['mean']:.2f}%, P5={margin_stats['p5']:.2f}%, P95={margin_stats['p95']:.2f}%")

    save_summary_json(proj, df_2v, fp_total, jp_total, pct_actas, fp_win_prob, margin_stats)


def save_summary_json(proj, df_2v, fp_total, jp_total, pct_actas, fp_win_prob, margin_stats):
    """Save a JSON summary for the Next.js dashboard."""
    candidates = ["FUERZA POPULAR", "JUNTOS POR EL PERÚ"]

    # Region drilldown — projected totals per region
    region_data = []
    for (ambito, region), grp in proj.groupby(["ambito", "region"]):
        fp = grp["FUERZA POPULAR"].sum()
        jp = grp["JUNTOS POR EL PERÚ"].sum()
        total = fp + jp
        if total > 0:
            region_data.append({
                "ambito": ambito,
                "region": region,
                "fp_votos": int(fp),
                "jp_votos": int(jp),
                "fp_pct": round(fp / total * 100, 2),
                "jp_pct": round(jp / total * 100, 2),
                "pct_actas": round(grp["pct_actas"].mean(), 1),
            })
    region_data.sort(key=lambda x: -(x["fp_votos"] + x["jp_votos"]))

    # Actual counted votes
    actual_fp = df_2v[df_2v["partido"] == "FUERZA POPULAR"]["votos"].sum()
    actual_jp = df_2v[df_2v["partido"] == "JUNTOS POR EL PERÚ"]["votos"].sum()
    actual_total = actual_fp + actual_jp

    # Persist snapshot row before building the summary so history is up to date
    save_2v_snapshot(pct_actas, actual_fp, actual_jp, fp_total, jp_total, fp_win_prob, margin_stats)
    snapshot_history = load_snapshots()

    # Remaining actas by region — with per-fuente breakdown
    proj_with_remaining = proj.copy()
    proj_with_remaining["remaining"] = proj_with_remaining["total_actas"] - proj_with_remaining["actas_contabilizadas"]

    # Precompute actual counted votes per district for FP and JP
    actual_by_dist = df_2v[df_2v["partido"].isin(candidates)].pivot_table(
        index=["ambito", "region", "provincia", "distrito"],
        columns="partido", values="votos", aggfunc="sum", fill_value=0
    ).reindex(columns=candidates, fill_value=0)

    # Join actuals into proj for fast subtraction
    proj_with_remaining = proj_with_remaining.merge(
        actual_by_dist.reset_index().rename(columns={c: f"actual_{c}" for c in candidates}),
        on=["ambito", "region", "provincia", "distrito"],
        how="left"
    )
    for c in candidates:
        proj_with_remaining[f"remaining_{c}"] = (
            proj_with_remaining[c] - proj_with_remaining[f"actual_{c}"].fillna(0)
        ).clip(lower=0)

    remaining_by_region = []
    for (ambito, region), grp in proj_with_remaining.groupby(["ambito", "region"]):
        sub_fp_rem = grp["remaining_FUERZA POPULAR"].sum()
        sub_jp_rem = grp["remaining_JUNTOS POR EL PERÚ"].sum()
        tot_rem = sub_fp_rem + sub_jp_rem
        rem_actas = grp["remaining"].sum()

        # Per-fuente breakdown of remaining votes in this region
        fuente_breakdown = {}
        for fuente, fgrp in grp[grp["remaining"] > 0].groupby("fuente"):
            f_fp = fgrp["remaining_FUERZA POPULAR"].sum()
            f_jp = fgrp["remaining_JUNTOS POR EL PERÚ"].sum()
            fuente_breakdown[fuente] = int(f_fp + f_jp)

        # Net contribution: positive = helps FP, negative = helps JP
        net_fp = sub_fp_rem - sub_jp_rem

        remaining_by_region.append({
            "region": region,
            "ambito": ambito,
            "remaining_actas": int(rem_actas),
            "remaining_votos": int(tot_rem),
            "fp_remaining_votos": int(sub_fp_rem),
            "jp_remaining_votos": int(sub_jp_rem),
            "net_fp": int(net_fp),
            "fp_remaining_pct": round(sub_fp_rem / tot_rem * 100, 1) if tot_rem > 0 else 50,
            "favors": "FP" if net_fp > 0 else "JP",
            "by_fuente": fuente_breakdown,
        })
    # Sort by net FP contribution: most FP-favorable first, most JP-favorable last
    remaining_by_region.sort(key=lambda x: -x["net_fp"])

    summary = {
        "timestamp": datetime.now().isoformat(),
        "pct_actas": round(pct_actas, 2),
        "fp": {
            "votos_actuales": int(actual_fp),
            "votos_proyectados": int(fp_total),
            "pct_actual": round(actual_fp / actual_total * 100, 2) if actual_total > 0 else 0,
            "pct_proyectado": round(fp_total / (fp_total + jp_total) * 100, 2),
            "win_probability": round(fp_win_prob, 2),
        },
        "jp": {
            "votos_actuales": int(actual_jp),
            "votos_proyectados": int(jp_total),
            "pct_actual": round(actual_jp / actual_total * 100, 2) if actual_total > 0 else 0,
            "pct_proyectado": round(jp_total / (fp_total + jp_total) * 100, 2),
            "win_probability": round(100 - fp_win_prob, 2),
        },
        "margin": {
            "mean": margin_stats["mean"],
            "p5": margin_stats["p5"],
            "p95": margin_stats["p95"],
        },
        "regions": region_data,
        "remaining_by_region": remaining_by_region,
        "fuente_breakdown": proj["fuente"].value_counts().to_dict(),
        "history": snapshot_history,
    }

    out_file = os.path.join(DATA_DIR, "summary_2v.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(f"\nSaved: {out_file}")


if __name__ == "__main__":
    main()
