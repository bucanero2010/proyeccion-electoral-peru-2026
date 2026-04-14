#!/usr/bin/env python3
"""
Streamlit app — Proyección de Resultados Electorales Presidenciales Perú 2026
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import glob
from collections import defaultdict

st.set_page_config(
    page_title="Proyección Electoral Perú 2026",
    page_icon="🗳️",
    layout="wide",
)

SPECIAL = {"VOTOS EN BLANCO", "VOTOS NULOS"}
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")


@st.cache_data
def load_data(filename):
    df = pd.read_csv(filename)
    for col in ["votos", "total_actas", "actas_contabilizadas", "pct_actas_contabilizadas"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
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


def project(df, hierarchy, threshold):
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

    for key_d, row_a in actas_d.iterrows():
        ambito, region, provincia, distrito = key_d
        total_actas = row_a["total_actas"]
        actas_contab = row_a["actas_contab"]
        pct = row_a["pct_actas"]
        key_p, key_r, key_a = (ambito, region, provincia), (ambito, region), (ambito,)

        if ambito == "EXTRANJERO":
            if pct >= 100.0 or total_actas == 0 or pct >= threshold:
                source, props = "distrito", pd_d.get(key_d, {})
            else:
                source, props = "extranjero", pd_a.get(key_a, {})
        elif pct >= 100.0 or total_actas == 0 or pct >= threshold:
            source, props = "distrito", pd_d.get(key_d, {})
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

    df = load_data(selected_file)
    hierarchy = build_hierarchy(df)
    proj = project(df, hierarchy, threshold)

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

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Distritos", f"{n_distritos:,}")
    c2.metric("Actas contabilizadas", f"{pct_global:.1f}%")
    c3.metric("Votos válidos (proy.)", f"{int(total_validos):,}")
    c4.metric("Votos emitidos (proy.)", f"{int(total_emitidos):,}")

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
                      showlegend=False, coloraxis_showscale=False)
    fig.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
    st.plotly_chart(fig, use_container_width=True)

    # Summary table
    st.subheader("Tabla resumen")
    summary_df = pd.DataFrame({
        "Partido": candidatos.index,
        "Votos actuales": [int(actual_candidatos.get(p, 0)) for p in candidatos.index],
        "Votos proyectados": candidatos.values.astype(int),
        "% Votos válidos": (candidatos / total_validos * 100).round(3).values,
    }).reset_index(drop=True)
    summary_df.index += 1
    st.dataframe(summary_df, use_container_width=True, height=min(600, len(summary_df) * 38 + 40))

    cb, cn = st.columns(2)
    cb.metric("Votos en blanco (proy.)", f"{int(especiales.get('VOTOS EN BLANCO', 0)):,}")
    cn.metric("Votos nulos (proy.)", f"{int(especiales.get('VOTOS NULOS', 0)):,}")

    st.divider()

    # ── Actual vs Proyectado ──
    st.subheader("Actual vs Proyectado")

    # Build comparison dataframe for top N
    top_partidos = candidatos.head(top_n).index
    compare_df = pd.DataFrame({
        "Partido": top_partidos,
        "Votos actuales": [int(actual_candidatos.get(p, 0)) for p in top_partidos],
        "% Actual": [(actual_candidatos.get(p, 0) / actual_validos * 100).round(2) if actual_validos > 0 else 0 for p in top_partidos],
        "Votos proyectados": [int(candidatos.get(p, 0)) for p in top_partidos],
        "% Proyectado": [(candidatos.get(p, 0) / total_validos * 100).round(2) if total_validos > 0 else 0 for p in top_partidos],
    }).reset_index(drop=True)
    compare_df["Diferencia votos"] = compare_df["Votos proyectados"] - compare_df["Votos actuales"]
    compare_df["Δ %"] = (compare_df["% Proyectado"] - compare_df["% Actual"]).round(3)
    compare_df.index += 1

    st.dataframe(compare_df, use_container_width=True, height=min(500, len(compare_df) * 38 + 40))

    # Grouped bar chart: actual vs projected %
    import plotly.graph_objects as go
    fig_cmp = go.Figure()
    fig_cmp.add_trace(go.Bar(
        y=compare_df["Partido"], x=compare_df["% Actual"],
        name="% Actual", orientation="h", marker_color="#636EFA",
        text=compare_df["% Actual"], texttemplate="%{text:.2f}%", textposition="outside",
    ))
    fig_cmp.add_trace(go.Bar(
        y=compare_df["Partido"], x=compare_df["% Proyectado"],
        name="% Proyectado", orientation="h", marker_color="#00CC96",
        text=compare_df["% Proyectado"], texttemplate="%{text:.2f}%", textposition="outside",
    ))
    fig_cmp.update_layout(
        barmode="group", yaxis={"categoryorder": "total ascending"},
        height=max(400, top_n * 55), legend=dict(orientation="h", yanchor="bottom", y=1.02),
        margin=dict(r=80),
    )
    st.plotly_chart(fig_cmp, use_container_width=True)

    st.divider()

    # Source pie
    st.subheader("Fuente de proporción por distrito")
    src = proj["fuente"].value_counts()
    st.plotly_chart(px.pie(values=src.values, names=src.index), use_container_width=True)

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
   - **< {int(threshold)}%** → cascada: provincia → región → país (el primero que supere el umbral)
   - **Extranjero:** si < {int(threshold)}%, usa directamente el total de EXTRANJERO

3. **Proyección:** `votos_contados × (total_actas / actas_contabilizadas)`,
   distribuidos según la proporción seleccionada.

4. **Agregación:** Suma de todos los distritos = resultado nacional estimado.

**Limitaciones:** Asume distribución uniforme de actas pendientes. No incorpora encuestas
ni conteos rápidos. Los resultados cambian conforme la ONPE avanza.

**Fuente:** [ONPE - Resultados Electorales](https://resultadoelectoral.onpe.gob.pe/main/presidenciales)
""")

    # Downloads
    st.divider()
    st.subheader("Descargar datos")
    d1, d2 = st.columns(2)
    with d1:
        st.download_button("📥 Proyección por distrito", proj.to_csv(index=False).encode("utf-8"),
                           "proyeccion_por_distrito.csv", "text/csv")
    with d2:
        st.download_button("📥 Resumen nacional", summary_df.to_csv(index=False).encode("utf-8"),
                           "resumen_proyeccion.csv", "text/csv")


if __name__ == "__main__":
    main()
