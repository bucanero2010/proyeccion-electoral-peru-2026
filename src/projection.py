#!/usr/bin/env python3
"""
Proyección de resultados electorales presidenciales - ONPE Perú 2026

Metodología:
- Distrito con ≥ 30% actas → usa su propia proporción de votos
- Distrito con < 30% actas → cascada: provincia → región → país
- Extranjero: si < 30%, usa directamente el total EXTRANJERO
"""

import os
import csv
import glob
import sys
from collections import defaultdict
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
THRESHOLD = 30.0
SPECIAL = {"VOTOS EN BLANCO", "VOTOS NULOS"}


def load_data(filename):
    with open(filename, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def build_hierarchy(rows):
    agg = {
        "distrito": defaultdict(lambda: {"votos": defaultdict(float), "total_actas": 0, "actas_contab": 0, "pct_actas": 0}),
        "provincia": defaultdict(lambda: {"votos": defaultdict(float), "total_actas": 0, "actas_contab": 0}),
        "region": defaultdict(lambda: {"votos": defaultdict(float), "total_actas": 0, "actas_contab": 0}),
        "pais": defaultdict(lambda: {"votos": defaultdict(float), "total_actas": 0, "actas_contab": 0}),
        "total": {"votos": defaultdict(float), "total_actas": 0, "actas_contab": 0},
    }
    seen = set()

    for row in rows:
        partido = row["partido"]
        votos = float(row["votos"] or 0)
        t_actas = float(row["total_actas"] or 0)
        a_contab = float(row["actas_contabilizadas"] or 0)
        pct = float(row["pct_actas_contabilizadas"] or 0)

        key_d = (row["ambito"], row["region"], row["provincia"], row["distrito"])
        key_p = (row["ambito"], row["region"], row["provincia"])
        key_r = (row["ambito"], row["region"])
        key_a = (row["ambito"],)

        agg["distrito"][key_d]["votos"][partido] += votos
        agg["distrito"][key_d]["pct_actas"] = pct

        if key_d not in seen:
            agg["distrito"][key_d]["total_actas"] = t_actas
            agg["distrito"][key_d]["actas_contab"] = a_contab
            for level, key in [("provincia", key_p), ("region", key_r), ("pais", key_a)]:
                agg[level][key]["total_actas"] += t_actas
                agg[level][key]["actas_contab"] += a_contab
            agg["total"]["total_actas"] += t_actas
            agg["total"]["actas_contab"] += a_contab
            seen.add(key_d)

        for level, key in [("provincia", key_p), ("region", key_r), ("pais", key_a)]:
            agg[level][key]["votos"][partido] += votos
        agg["total"]["votos"][partido] += votos

    def add_proportions(entry):
        total_v = sum(entry["votos"].values())
        entry["proportions"] = {p: v / total_v for p, v in entry["votos"].items()} if total_v > 0 else {}
        t, c = entry["total_actas"], entry["actas_contab"]
        entry["pct_actas"] = (c / t * 100) if t > 0 else 0

    for level in ["provincia", "region", "pais"]:
        for entry in agg[level].values():
            add_proportions(entry)
    add_proportions(agg["total"])
    for entry in agg["distrito"].values():
        total_v = sum(entry["votos"].values())
        entry["proportions"] = {p: v / total_v for p, v in entry["votos"].items()} if total_v > 0 else {}

    return agg


def project_distrito(key_d, agg, threshold=THRESHOLD):
    d = agg["distrito"][key_d]
    ambito, region, provincia, distrito = key_d
    key_p = (ambito, region, provincia)
    key_r = (ambito, region)
    key_a = (ambito,)

    total_actas = d["total_actas"]
    actas_contab = d["actas_contab"]
    pct = d["pct_actas"]
    actual_votos = d["votos"]

    if pct >= 100.0 or total_actas == 0:
        return actual_votos, pct, "distrito"

    if ambito == "EXTRANJERO":
        if pct >= threshold:
            props, source = d["proportions"], "distrito"
        elif agg["provincia"][key_p]["pct_actas"] >= threshold:
            props, source = agg["provincia"][key_p]["proportions"], "ext_pais"
        elif agg["region"][key_r]["pct_actas"] >= threshold:
            props, source = agg["region"][key_r]["proportions"], "ext_continente"
        else:
            props, source = agg["pais"][key_a]["proportions"], "extranjero"
    elif pct >= threshold:
        props, source = d["proportions"], "distrito"
    elif agg["provincia"][key_p]["pct_actas"] >= threshold:
        props, source = agg["provincia"][key_p]["proportions"], "provincia"
    elif agg["region"][key_r]["pct_actas"] >= threshold:
        props, source = agg["region"][key_r]["proportions"], "region"
    elif agg["pais"][key_a]["pct_actas"] >= threshold:
        props, source = agg["pais"][key_a]["proportions"], "pais"
    else:
        props, source = agg["total"]["proportions"], "total"

    total_counted = sum(actual_votos.values())
    if actas_contab > 0 and total_actas > 0:
        estimated_total = total_counted * (total_actas / actas_contab)
    elif total_actas > 0 and total_counted == 0:
        # No votes counted — estimate using avg votes-per-acta from the source level
        level_key = {"provincia": key_p, "region": key_r, "pais": key_a, "extranjero": key_a, "total": None}
        src_key = level_key.get(source)
        src_level = {"provincia": "provincia", "region": "region", "pais": "pais", "extranjero": "pais", "total": "total"}
        lvl = src_level.get(source, "total")
        if lvl != "total" and src_key and src_key in agg[lvl]:
            entry = agg[lvl][src_key]
            if entry["actas_contab"] > 0:
                avg_vpa = sum(entry["votos"].values()) / entry["actas_contab"]
                estimated_total = avg_vpa * total_actas
            else:
                estimated_total = 0
        elif lvl == "total" and agg["total"]["actas_contab"] > 0:
            avg_vpa = sum(agg["total"]["votos"].values()) / agg["total"]["actas_contab"]
            estimated_total = avg_vpa * total_actas
        else:
            estimated_total = 0
    else:
        estimated_total = total_counted

    projected = {}
    for partido in set(actual_votos.keys()) | set(props.keys()):
        projected[partido] = round(estimated_total * props.get(partido, 0))

    return projected, pct, source


def main():
    files = sorted(glob.glob(os.path.join(DATA_DIR, "resultados_presidenciales_*.csv")))
    if not files:
        print("No se encontró archivo de resultados. Ejecuta primero: python -m src.scraper")
        sys.exit(1)

    filename = files[-1]
    print(f"Cargando: {filename}")

    rows = load_data(filename)
    agg = build_hierarchy(rows)
    n_distritos = len(agg["distrito"])
    print(f"  {len(rows)} filas, {n_distritos} distritos")

    all_partidos = sorted({p for d in agg["distrito"].values() for p in d["votos"]})
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    pivot_file = os.path.join(DATA_DIR, f"proyeccion_por_distrito_{timestamp}.csv")
    pivot_fields = [
        "ambito", "region", "provincia", "distrito",
        "total_actas", "actas_contabilizadas", "pct_actas", "fuente_proporcion",
    ] + [f"votos_{p}" for p in all_partidos]

    resumen = defaultdict(float)
    source_counts = defaultdict(int)

    with open(pivot_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=pivot_fields)
        writer.writeheader()
        for key_d in sorted(agg["distrito"].keys()):
            projected, pct, source = project_distrito(key_d, agg)
            ambito, region, provincia, distrito = key_d
            d = agg["distrito"][key_d]
            row = {
                "ambito": ambito, "region": region, "provincia": provincia,
                "distrito": distrito, "total_actas": int(d["total_actas"]),
                "actas_contabilizadas": int(d["actas_contab"]),
                "pct_actas": round(pct, 2), "fuente_proporcion": source,
            }
            for p in all_partidos:
                row[f"votos_{p}"] = projected.get(p, 0)
                resumen[p] += projected.get(p, 0)
            source_counts[source] += 1
            writer.writerow(row)

    # Resumen
    resumen_file = os.path.join(DATA_DIR, f"resumen_proyeccion_{timestamp}.csv")
    candidatos = sorted([(p, v) for p, v in resumen.items() if p not in SPECIAL], key=lambda x: -x[1])
    especiales = [(p, v) for p, v in resumen.items() if p in SPECIAL]
    total_validos = sum(v for _, v in candidatos)

    with open(resumen_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["partido", "votos_proyectados", "pct_votos_validos"])
        for p, v in candidatos:
            writer.writerow([p, int(v), round(v / total_validos * 100, 3) if total_validos else 0])
        writer.writerow([])
        for p, v in especiales:
            writer.writerow([p, int(v), ""])

    print(f"\n{'='*70}")
    print(f"PROYECCIÓN DE RESULTADOS ELECTORALES")
    print(f"{'='*70}")
    print(f"{'Partido':<55} {'Votos':>12} {'%':>7}")
    print(f"{'-'*70}")
    for p, v in candidatos[:10]:
        pct = v / total_validos * 100 if total_validos else 0
        print(f"{p:<55} {int(v):>12,} {pct:>6.2f}%")
    print(f"{'='*70}")
    print(f"\nArchivos: {pivot_file}\n         {resumen_file}")


if __name__ == "__main__":
    main()
