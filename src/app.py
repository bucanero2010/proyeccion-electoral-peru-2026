#!/usr/bin/env python3
"""
Streamlit app — Proyección de Resultados Electorales Presidenciales Perú 2026
"""

import os
import sys

# Ensure src/ is on the path for both local and Streamlit Cloud
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import streamlit as st
import pandas as pd
import plotly.express as px
import glob
from collections import defaultdict
import numpy as np

st.set_page_config(
    page_title="Proyección Electoral Perú 2026",
    page_icon="🗳️",
    layout="wide",
)

# Mobile-friendly CSS
st.markdown("""
<style>
/* Tighter padding on mobile */
@media (max-width: 768px) {
    .block-container { padding: 1rem 0.5rem !important; }
    [data-testid="stMetric"] { padding: 0.5rem 0.3rem; }
    [data-testid="stMetricLabel"] { font-size: 0.75rem !important; }
    [data-testid="stMetricValue"] { font-size: 1.1rem !important; }
    h1 { font-size: 1.4rem !important; }
    h2, h3 { font-size: 1.1rem !important; }
}
/* Scrollable tables on mobile */
[data-testid="stDataFrame"] { overflow-x: auto !important; }
</style>
""", unsafe_allow_html=True)

SPECIAL = {"VOTOS EN BLANCO", "VOTOS NULOS"}
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")


@st.cache_data
def load_data(filename):
    df = pd.read_csv(filename, dtype={"ubigeo_departamento": str, "ubigeo_provincia": str, "ubigeo_distrito": str})
    for col in ["votos", "total_actas", "actas_contabilizadas", "pct_actas_contabilizadas"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    # Ensure ubigeo columns are zero-padded 6-digit strings
    for col in ["ubigeo_departamento", "ubigeo_provincia", "ubigeo_distrito"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.zfill(6)
    return df


def build_hierarchy(df):
    geo_d = ["ambito", "region", "provincia", "distrito"]
    geo_p = ["ambito", "region", "provincia"]
    geo_r = ["ambito", "region"]
    geo_a = ["ambito"]

    actas_d = df.groupby(geo_d).agg(
        total_actas=("total_actas", "first"),
        actas_contab=("actas_contabilizadas", "first"),
        pct_actas=("pct_actas_contabilizadas", "first"),
    ).reset_index()

    votos_d = df.groupby(geo_d + ["partido"])["votos"].sum().reset_index()

    def agg_actas(actas_src, geo):
        a = actas_src.groupby(geo).agg(total_actas=("total_actas", "sum"), actas_contab=("actas_contab", "sum")).reset_index()
        a["pct_actas"] = (a["actas_contab"] / a["total_actas"] * 100).fillna(0)
        return a

    actas_p = agg_actas(actas_d, geo_p)
    actas_r = agg_actas(actas_d, geo_r)
    actas_a = agg_actas(actas_d, geo_a)

    def compute_props(votos_df, geo):
        totals = votos_df.groupby(geo)["votos"].sum().rename("total_votos")
        m = votos_df.merge(totals, on=geo)
        m["prop"] = (m["votos"] / m["total_votos"]).fillna(0)
        return m

    props_d = compute_props(votos_d, geo_d)
    props_p = compute_props(df.groupby(geo_p + ["partido"])["votos"].sum().reset_index(), geo_p)
    props_r = compute_props(df.groupby(geo_r + ["partido"])["votos"].sum().reset_index(), geo_r)
    props_a = compute_props(df.groupby(geo_a + ["partido"])["votos"].sum().reset_index(), geo_a)
    vt = df.groupby("partido")["votos"].sum()
    props_total = (vt / vt.sum()).to_dict() if vt.sum() > 0 else {}

    return {
        "actas_d": actas_d, "actas_p": actas_p, "actas_r": actas_r, "actas_a": actas_a,
        "props_d": props_d, "props_p": props_p, "props_r": props_r, "props_a": props_a,
        "props_total": props_total,
    }


def project(df, hierarchy, threshold, sim_index=None):
    h = hierarchy
    results = []

    actas_d = h["actas_d"].set_index(["ambito", "region", "provincia", "distrito"])
    actas_p = h["actas_p"].set_index(["ambito", "region", "provincia"])
    actas_r = h["actas_r"].set_index(["ambito", "region"])
    actas_a = h["actas_a"].set_index(["ambito"])

    def props_to_dict(props_df, geo_cols):
        d = {}
        for _, row in props_df.iterrows():
            key = tuple(row[c] for c in geo_cols)
            d.setdefault(key, {})[row["partido"]] = row["prop"]
        return d

    pd_d = props_to_dict(h["props_d"], ["ambito", "region", "provincia", "distrito"])
    pd_p = props_to_dict(h["props_p"], ["ambito", "region", "provincia"])
    pd_r = props_to_dict(h["props_r"], ["ambito", "region"])
    pd_a = props_to_dict(h["props_a"], ["ambito"])
    pd_t = h["props_total"]

    votos_by_d = df.groupby(["ambito", "region", "provincia", "distrito", "partido"])["votos"].sum()

    # Precompute avg votes-per-acta at each geographic level (for 0% actas districts)
    total_votos_by_d = df.groupby(["ambito", "region", "provincia", "distrito"])["votos"].sum()
    avg_vpa = {}  # {key: avg_votes_per_acta}
    for key_d in actas_d.index:
        row_a = actas_d.loc[key_d]
        if row_a["actas_contab"] > 0:
            avg_vpa[key_d] = total_votos_by_d.get(key_d, 0) / row_a["actas_contab"]

    # Avg VPA by provincia
    avg_vpa_p = {}
    for key_p in actas_p.index:
        row_p = actas_p.loc[key_p]
        if row_p["actas_contab"] > 0:
            mask = (df["ambito"] == key_p[0]) & (df["region"] == key_p[1]) & (df["provincia"] == key_p[2])
            avg_vpa_p[key_p] = df.loc[mask, "votos"].sum() / row_p["actas_contab"]

    # Avg VPA by region
    avg_vpa_r = {}
    for key_r in actas_r.index:
        row_r = actas_r.loc[key_r]
        if row_r["actas_contab"] > 0:
            mask = (df["ambito"] == key_r[0]) & (df["region"] == key_r[1])
            avg_vpa_r[key_r] = df.loc[mask, "votos"].sum() / row_r["actas_contab"]

    # Avg VPA by ambito
    avg_vpa_a = {}
    for amb in df["ambito"].unique():
        mask = df["ambito"] == amb
        contab = h["actas_a"][h["actas_a"]["ambito"] == amb]["actas_contab"].sum()
        if contab > 0:
            avg_vpa_a[amb] = df.loc[mask, "votos"].sum() / contab

    # Global avg VPA
    total_contab = h["actas_a"]["actas_contab"].sum()
    global_avg_vpa = df["votos"].sum() / total_contab if total_contab > 0 else 0

    # Build ubigeo-keyed lookups for similarity fallback
    ubigeo_to_key = {}
    ubigeo_props = {}
    ubigeo_pcts = {}
    for key_d in actas_d.index:
        row_a = actas_d.loc[key_d]
        ub = df.loc[(df["ambito"] == key_d[0]) & (df["region"] == key_d[1]) &
                    (df["provincia"] == key_d[2]) & (df["distrito"] == key_d[3]),
                    "ubigeo_distrito"].iloc[0] if "ubigeo_distrito" in df.columns else None
        if ub:
            ubigeo_to_key[ub] = key_d
            ubigeo_props[ub] = pd_d.get(key_d, {})
            ubigeo_pcts[ub] = row_a["pct_actas"]

    for key_d, row_a in actas_d.iterrows():
        ambito, region, provincia, distrito = key_d
        total_actas = row_a["total_actas"]
        actas_contab = row_a["actas_contab"]
        pct = row_a["pct_actas"]
        key_p, key_r, key_a = (ambito, region, provincia), (ambito, region), (ambito,)

        # Find ubigeo for this distrito
        ub_match = df.loc[(df["ambito"] == ambito) & (df["region"] == region) &
                          (df["provincia"] == provincia) & (df["distrito"] == distrito),
                          "ubigeo_distrito"]
        current_ubigeo = ub_match.iloc[0] if len(ub_match) > 0 and "ubigeo_distrito" in df.columns else None

        if ambito == "EXTRANJERO":
            if pct >= 100.0 or total_actas == 0 or pct >= threshold:
                source, props = "distrito", pd_d.get(key_d, {})
            elif key_p in actas_p.index and actas_p.loc[key_p, "pct_actas"] >= threshold:
                # País level (e.g., all of Chile)
                source, props = "ext_pais", pd_p.get(key_p, {})
            elif key_r in actas_r.index and actas_r.loc[key_r, "pct_actas"] >= threshold:
                # Continente level (e.g., all of AMÉRICA)
                source, props = "ext_continente", pd_r.get(key_r, {})
            else:
                # Total EXTRANJERO
                source, props = "extranjero", pd_a.get(key_a, {})
        elif pct >= 100.0 or total_actas == 0 or pct >= threshold:
            source, props = "distrito", pd_d.get(key_d, {})
        else:
            # Try similarity-based fallback first
            sim_props = None
            if sim_index and current_ubigeo:
                import sys as _sys
                _sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
                from similarity import get_similar_district_proportions
                sim_props = get_similar_district_proportions(
                    current_ubigeo, sim_index, ubigeo_props, ubigeo_pcts, threshold)

            if sim_props:
                source, props = "similitud", sim_props
            elif key_p in actas_p.index and actas_p.loc[key_p, "pct_actas"] >= threshold:
                source, props = "provincia", pd_p.get(key_p, {})
            elif key_r in actas_r.index and actas_r.loc[key_r, "pct_actas"] >= threshold:
                source, props = "region", pd_r.get(key_r, {})
            elif key_a in actas_a.index and actas_a.loc[key_a, "pct_actas"] >= threshold:
                source, props = "pais", pd_a.get(key_a, {})
            else:
                source, props = "total", pd_t

        counted = sum(votos_by_d.get((*key_d, p), 0) for p in props)
        if actas_contab > 0 and total_actas > 0:
            estimated_total = counted * (total_actas / actas_contab)
        elif total_actas > 0 and counted == 0:
            # No votes counted — estimate volume from avg votes-per-acta
            if source == "similitud" and sim_index and current_ubigeo:
                neighbors = sim_index.get(current_ubigeo, [])
                vpa_vals = [avg_vpa[ubigeo_to_key[n]] for n, _ in neighbors
                            if n in ubigeo_to_key and ubigeo_to_key[n] in avg_vpa][:5]
                estimated_total = (sum(vpa_vals) / len(vpa_vals) * total_actas) if vpa_vals else 0
            elif source == "provincia":
                estimated_total = avg_vpa_p.get(key_p, 0) * total_actas
            elif source == "region":
                estimated_total = avg_vpa_r.get(key_r, 0) * total_actas
            elif source in ("pais", "extranjero"):
                estimated_total = avg_vpa_a.get(ambito, 0) * total_actas
            elif source == "ext_pais":
                estimated_total = avg_vpa_p.get(key_p, avg_vpa_a.get(ambito, 0)) * total_actas
            elif source == "ext_continente":
                estimated_total = avg_vpa_r.get(key_r, avg_vpa_a.get(ambito, 0)) * total_actas
            else:
                estimated_total = global_avg_vpa * total_actas
        else:
            estimated_total = counted

        row_out = {
            "ambito": ambito, "region": region, "provincia": provincia,
            "distrito": distrito, "total_actas": int(total_actas),
            "actas_contabilizadas": int(actas_contab),
            "pct_actas": round(pct, 2), "fuente": source,
        }
        for partido, prop in props.items():
            row_out[partido] = round(estimated_total * prop)
        results.append(row_out)

    return pd.DataFrame(results)


@st.cache_data
def load_similarity_index():
    import sys as _sys
    _sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from similarity import build_similarity_index
    sim_index, _ = build_similarity_index(k=20)
    return sim_index


def main():
    st.title("🗳️ Proyección Electoral Presidencial Perú 2026")

    files = sorted(glob.glob(os.path.join(DATA_DIR, "resultados_presidenciales_*.csv")))
    if not files:
        st.error("No se encontró archivo de resultados. Ejecuta primero `python -m src.scraper`")
        return

    with st.sidebar:
        st.header("⚙️ Configuración")
        selected_file = st.selectbox("Archivo de datos", files, index=len(files) - 1,
                                     format_func=lambda x: os.path.basename(x))
        threshold = st.slider("Umbral mínimo % actas", 5, 80, 30, 5,
                              help="Si un distrito tiene menos de este %, se usa el nivel geográfico superior")
        use_similarity = st.checkbox("Usar similitud 2021", value=True,
                                     help="Para distritos con pocas actas, usar la proporción de distritos que votaron similar en 2021")

    df = load_data(selected_file)
    hierarchy = build_hierarchy(df)

    # Extract refresh timestamp from filename
    import re
    ts_match = re.search(r"(\d{8})_(\d{6})", os.path.basename(selected_file))
    if ts_match:
        d, t = ts_match.group(1), ts_match.group(2)
        refresh_str = f"{d[:4]}-{d[4:6]}-{d[6:8]} {t[:2]}:{t[2:4]}:{t[4:6]}"
    else:
        refresh_str = "desconocida"

    # Load similarity index if enabled
    sim_index = None
    if use_similarity:
        sim_file = os.path.join(DATA_DIR, "2021_presidencial-resultados-partidos.csv")
        if os.path.exists(sim_file):
            sim_index = load_similarity_index()
        else:
            st.sidebar.warning("No se encontró datos 2021. Similitud desactivada.")

    proj = project(df, hierarchy, threshold, sim_index=sim_index)

    meta_cols = {"ambito", "region", "provincia", "distrito", "total_actas",
                 "actas_contabilizadas", "pct_actas", "fuente"}
    partido_cols = [c for c in proj.columns if c not in meta_cols]

    totals = proj[partido_cols].sum().sort_values(ascending=False)
    candidatos = totals[[p for p in totals.index if p not in SPECIAL]]
    especiales = totals[[p for p in totals.index if p in SPECIAL]]
    total_validos = candidatos.sum()
    total_emitidos = totals.sum()

    n_distritos = len(proj)
    pct_global = proj["actas_contabilizadas"].sum() / proj["total_actas"].sum() * 100 if proj["total_actas"].sum() > 0 else 0

    # Compute actual (current counted) totals from raw data
    actual_by_partido = df.groupby("partido")["votos"].sum().sort_values(ascending=False)
    actual_candidatos = actual_by_partido[[p for p in actual_by_partido.index if p not in SPECIAL]]
    actual_validos = actual_candidatos.sum()

    # ── Projection banner ──
    st.warning(
        f"⚠️ **PROYECCIÓN** — Basada en {pct_global:.2f}% de actas contabilizadas. "
        f"No es el resultado oficial. Última actualización: {refresh_str}",
        icon="⚠️",
    )

    # KPIs — 2x2 grid works better on mobile than 4 columns
    r1c1, r1c2 = st.columns(2)
    r1c1.metric("Distritos", f"{n_distritos:,}")
    r1c2.metric("Actas contabilizadas", f"{pct_global:.2f}%")
    r2c1, r2c2 = st.columns(2)
    r2c1.metric("Votos válidos (proy.)", f"{int(total_validos):,}")
    r2c2.metric("Votos emitidos (proy.)", f"{int(total_emitidos):,}")

    st.divider()

    # Bar chart
    st.subheader("Resultado proyectado")
    top_n = st.slider("Top N candidatos", 5, len(candidatos), min(10, len(candidatos)), key="topn")
    top = candidatos.head(top_n)
    pcts = (top / total_validos * 100).round(2)

    chart_df = pd.DataFrame({"Partido": top.index, "Votos": top.values.astype(int), "% Válidos": pcts.values})
    fig = px.bar(chart_df, x="% Válidos", y="Partido", orientation="h", text="% Válidos",
                 color="% Válidos", color_continuous_scale="RdYlGn")
    fig.update_layout(yaxis={"categoryorder": "total ascending"}, height=max(400, top_n * 45),
                      showlegend=False, coloraxis_showscale=False,
                      margin=dict(l=10, r=10, t=10, b=10))
    fig.update_traces(texttemplate="%{text:.2f}%", textposition="inside",
                      insidetextanchor="start")
    st.plotly_chart(fig, use_container_width=True)

    # Summary table
    st.subheader("Tabla resumen")
    summary_df = pd.DataFrame({
        "Partido": candidatos.index,
        "Votos actuales": [f"{int(actual_candidatos.get(p, 0)):,}" for p in candidatos.index],
        "Votos proyectados": [f"{int(v):,}" for v in candidatos.values],
        "% Votos válidos": [f"{v:.2f}%" for v in (candidatos / total_validos * 100).round(2).values],
    }).reset_index(drop=True)
    summary_df.index += 1
    st.dataframe(summary_df, use_container_width=True, height=min(600, len(summary_df) * 38 + 40))

    cb, cn = st.columns(2)
    cb.metric("Votos en blanco", f"{int(especiales.get('VOTOS EN BLANCO', 0)):,}")
    cn.metric("Votos nulos", f"{int(especiales.get('VOTOS NULOS', 0)):,}")

    st.divider()

    # ── Actual vs Proyectado ──
    st.subheader("Actual vs Proyectado")

    # Build comparison dataframe for top N
    top_partidos = candidatos.head(top_n).index
    compare_df = pd.DataFrame({
        "Partido": top_partidos,
        "Votos actuales": [f"{int(actual_candidatos.get(p, 0)):,}" for p in top_partidos],
        "% Actual": [f"{(actual_candidatos.get(p, 0) / actual_validos * 100):.2f}%" if actual_validos > 0 else "0.00%" for p in top_partidos],
        "Votos proyectados": [f"{int(candidatos.get(p, 0)):,}" for p in top_partidos],
        "% Proyectado": [f"{(candidatos.get(p, 0) / total_validos * 100):.2f}%" if total_validos > 0 else "0.00%" for p in top_partidos],
    }).reset_index(drop=True)
    # Compute numeric delta for the chart
    compare_df["Δ %"] = [
        f"{((candidatos.get(p, 0) / total_validos * 100) - (actual_candidatos.get(p, 0) / actual_validos * 100)):.2f}%"
        if total_validos > 0 and actual_validos > 0 else "0.00%"
        for p in top_partidos
    ]
    compare_df.index += 1

    st.dataframe(compare_df, use_container_width=True, height=min(500, len(compare_df) * 38 + 40))

    # Grouped bar chart: actual vs projected %
    import plotly.graph_objects as go
    pct_actual_vals = [(actual_candidatos.get(p, 0) / actual_validos * 100) if actual_validos > 0 else 0 for p in top_partidos]
    pct_proy_vals = [(candidatos.get(p, 0) / total_validos * 100) if total_validos > 0 else 0 for p in top_partidos]
    chart_partidos = list(top_partidos)

    fig_cmp = go.Figure()
    fig_cmp.add_trace(go.Bar(
        y=chart_partidos, x=pct_actual_vals,
        name="% Actual", orientation="h", marker_color="#636EFA",
        text=pct_actual_vals, texttemplate="%{text:.2f}%", textposition="inside",
        insidetextanchor="start",
    ))
    fig_cmp.add_trace(go.Bar(
        y=chart_partidos, x=pct_proy_vals,
        name="% Proyectado", orientation="h", marker_color="#00CC96",
        text=pct_proy_vals, texttemplate="%{text:.2f}%", textposition="inside",
        insidetextanchor="start",
    ))
    fig_cmp.update_layout(
        barmode="group", yaxis={"categoryorder": "total ascending"},
        height=max(400, top_n * 55),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
        margin=dict(l=10, r=10, t=30, b=10),
    )
    st.plotly_chart(fig_cmp, use_container_width=True)

    st.divider()

    # ── Monte Carlo simulation ──
    st.subheader("🎲 Probabilidad por posición (Monte Carlo)")
    mc_file = os.path.join(DATA_DIR, "montecarlo.json")
    if os.path.exists(mc_file):
        import json as _json
        with open(mc_file, encoding="utf-8") as _f:
            mc_results = _json.load(_f)

        non_special_names = [p for p in partido_cols if p not in SPECIAL]

        # Position probabilities table
        pos_labels = {"1": "1er lugar", "2": "2do lugar", "3": "3er lugar"}
        mc_table_rows = []
        for pos in ["1", "2", "3"]:
            for p, prob in sorted(mc_results["positions"].get(pos, {}).items(), key=lambda x: -x[1]):
                if prob >= 0.1:
                    mc_table_rows.append({"Posición": pos_labels[pos], "Partido": p, "Probabilidad": f"{prob:.2f}%"})

        if mc_table_rows:
            mc_df = pd.DataFrame(mc_table_rows)
            st.dataframe(mc_df, use_container_width=True, hide_index=True,
                         height=min(400, len(mc_table_rows) * 38 + 40))

        # Confidence intervals for top candidates
        st.caption("Intervalos de confianza (P5 — P95) para los principales candidatos:")
        ci_rows = []
        for p, s in list(mc_results["stats"].items())[:10]:
            ci_rows.append({
                "Partido": p,
                "Proyección": f"{s['mean']:,}",
                "P5": f"{s['p5']:,}",
                "P95": f"{s['p95']:,}",
                "± Std": f"{s['std']:,}",
            })
        if ci_rows:
            st.dataframe(pd.DataFrame(ci_rows), use_container_width=True, hide_index=True)

        st.caption(f"Basado en {mc_results['n_sim']:,} simulaciones Monte Carlo usando distribución Dirichlet.")
    else:
        st.info("No hay datos de Monte Carlo. Ejecuta `python3 -m src.montecarlo` para generarlos.")

    st.divider()

    # Source pie
    st.subheader("Fuente de proporción por distrito")
    src = proj["fuente"].value_counts()
    st.plotly_chart(px.pie(values=src.values, names=src.index), use_container_width=True)

    st.divider()

    # ── Region progress ──
    st.subheader("📊 Progreso por región")
    region_progress = proj.groupby(["ambito", "region"]).agg(
        total_actas=("total_actas", "sum"),
        actas_contab=("actas_contabilizadas", "sum"),
        n_distritos=("distrito", "count"),
    ).reset_index()
    # count is per-partido row, divide by unique distritos
    region_progress["n_distritos"] = proj.groupby(["ambito", "region"])["distrito"].nunique().values
    region_progress["pct_actas"] = (region_progress["actas_contab"] / region_progress["total_actas"] * 100).fillna(0).round(2)
    region_progress = region_progress.sort_values("pct_actas", ascending=True)

    fig_rp = px.bar(
        region_progress, x="pct_actas", y="region", orientation="h",
        text="pct_actas", color="pct_actas",
        color_continuous_scale=["#ff4b4b", "#ffa600", "#00cc96"],
        range_color=[0, 100],
        labels={"pct_actas": "% Actas", "region": "Región"},
    )
    fig_rp.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
    fig_rp.update_layout(
        height=max(400, len(region_progress) * 25),
        showlegend=False, coloraxis_showscale=False,
        margin=dict(l=10, r=40, t=10, b=10),
    )
    st.plotly_chart(fig_rp, use_container_width=True)

    st.divider()

    # Region explorer
    st.subheader("Explorar por región")
    regiones = sorted(proj["region"].unique())
    sel = st.selectbox("Región", ["TODAS"] + regiones)
    filtered = proj if sel == "TODAS" else proj[proj["region"] == sel]
    display = ["ambito", "region", "provincia", "distrito", "pct_actas", "fuente"]
    for p in candidatos.head(5).index:
        if p in filtered.columns:
            display.append(p)
    st.dataframe(filtered[display].sort_values(["region", "provincia", "distrito"]),
                 use_container_width=True, height=500)

    # ── Time-series trend ──
    st.divider()
    st.subheader("📈 Evolución de la proyección")
    snapshot_file = os.path.join(DATA_DIR, "snapshots.csv")
    if os.path.exists(snapshot_file):
        import json
        snaps = pd.read_csv(snapshot_file)
        if len(snaps) >= 1:
            # Parse top candidates from each snapshot
            trend_rows = []
            for _, snap in snaps.iterrows():
                try:
                    top_cands = json.loads(snap["top_candidates_json"])
                    for c in top_cands:
                        trend_rows.append({
                            "timestamp": snap["timestamp"],
                            "pct_actas": snap["pct_actas_global"],
                            "partido": c["partido"],
                            "pct_votos": c["pct"],
                        })
                except:
                    pass

            if trend_rows:
                trend_df = pd.DataFrame(trend_rows)
                # Show top 6 candidates from latest snapshot
                latest = snaps.iloc[-1]
                try:
                    latest_top = [c["partido"] for c in json.loads(latest["top_candidates_json"])[:6]]
                except:
                    latest_top = []

                if latest_top:
                    trend_filtered = trend_df[trend_df["partido"].isin(latest_top)]
                    fig_trend = px.line(
                        trend_filtered, x="pct_actas", y="pct_votos",
                        color="partido", markers=True,
                        labels={"pct_actas": "% Actas contabilizadas", "pct_votos": "% Votos válidos",
                                "partido": "Partido"},
                    )
                    fig_trend.update_traces(texttemplate="%{y:.2f}%")
                    fig_trend.update_layout(
                        height=400, yaxis_ticksuffix="%", xaxis_ticksuffix="%",
                        legend=dict(orientation="h", yanchor="bottom", y=-0.4, xanchor="center", x=0.5),
                        margin=dict(l=10, r=10, t=10, b=10),
                    )
                    st.plotly_chart(fig_trend, use_container_width=True)

                # Actas progress
                fig_actas = px.line(
                    snaps, x="timestamp", y="pct_actas_global", markers=True,
                    labels={"timestamp": "Fecha/hora", "pct_actas_global": "% Actas contabilizadas"},
                )
                fig_actas.update_layout(height=250, yaxis_ticksuffix="%", margin=dict(l=10, r=10, t=10, b=10))
                st.plotly_chart(fig_actas, use_container_width=True)

                st.caption(f"{len(snaps)} snapshots registrados. Ejecuta el scraper periódicamente para más datos.")
            else:
                st.info("Snapshots sin datos de candidatos.")
        else:
            st.info("Solo 1 snapshot. Ejecuta el scraper de nuevo para ver la evolución.")
    else:
        st.info("No hay snapshots aún. Ejecuta el scraper para empezar a trackear la evolución.")

    # Methodology
    st.divider()
    st.subheader("📐 Metodología")
    st.markdown(f"""
Esta proyección estima el resultado final extrapolando los votos faltantes bajo el supuesto
de que las actas pendientes se distribuyen en la misma proporción que las ya contadas.

**Pasos:**

1. **Datos fuente:** Resultados oficiales parciales de la ONPE a nivel de distrito,
   incluyendo total de actas y actas contabilizadas.

2. **Selección de proporción por distrito:**
   - **≥ {int(threshold)}% actas** → proporción del propio distrito
   - **< {int(threshold)}%** → cascada con prioridad:
     1. **Similitud 2021** {"(activado ✅)" if sim_index else "(desactivado)"}: promedio ponderado
        de distritos que votaron de forma similar en las elecciones 2021, usando similitud
        coseno sobre los vectores de participación por partido. Solo usa vecinos que ya
        tengan ≥ {int(threshold)}% de actas en 2026.
     2. **Provincia** → **Región** → **País** (cascada geográfica tradicional)
   - **Extranjero:** cascada: ciudad → **país** (ej: todo Chile) → **continente** (ej: toda AMÉRICA) → total EXTRANJERO

3. **Proyección:** `votos_contados × (total_actas / actas_contabilizadas)`,
   distribuidos según la proporción seleccionada.

4. **Agregación:** Suma de todos los distritos = resultado nacional estimado.

**Limitaciones:** Asume distribución uniforme de actas pendientes. La similitud 2021
captura patrones históricos que pueden no repetirse. No incorpora encuestas ni conteos rápidos.

**Fuente:** [ONPE - Resultados Electorales](https://resultadoelectoral.onpe.gob.pe/main/presidenciales)
| [Datos 2021](https://github.com/jmcastagnetto/2021-elecciones-generales-peru-datos-de-onpe)
""")

    # Downloads
    st.divider()
    st.subheader("Descargar datos")
    st.download_button("📥 Proyección por distrito (CSV)", proj.to_csv(index=False).encode("utf-8"),
                       "proyeccion_por_distrito.csv", "text/csv", use_container_width=True)
    st.download_button("📥 Resumen nacional (CSV)", summary_df.to_csv(index=False).encode("utf-8"),
                       "resumen_proyeccion.csv", "text/csv", use_container_width=True)


if __name__ == "__main__":
    main()
