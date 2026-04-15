#!/usr/bin/env python3
"""
Scraper de resultados electorales presidenciales - ONPE Perú 2026
Extrae datos a nivel DISTRITO desde la API de la ONPE.
Usa ThreadPoolExecutor para paralelizar requests por distrito.
"""

import os
import requests
import csv
import time
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

BASE = "https://resultadoelectoral.onpe.gob.pe/presentacion-backend"
ID_ELECCION = 10
WORKERS = 3  # concurrent requests — more than this triggers ONPE throttling
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

_local = threading.local()
request_count = 0
_lock = threading.Lock()

REQUEST_HEADERS = {
    "Accept": "*/*",
    "Content-Type": "application/json",
    "Referer": "https://resultadoelectoral.onpe.gob.pe/main/presidenciales",
    "Origin": "https://resultadoelectoral.onpe.gob.pe",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}


def get_session():
    """Thread-local session for connection pooling."""
    if not hasattr(_local, "session"):
        s = requests.Session()
        s.headers.update(REQUEST_HEADERS)
        retry = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
        s.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=5, pool_maxsize=5))
        _local.session = s
    return _local.session


def api_get(path, params=None):
    global request_count
    url = f"{BASE}{path}"
    for attempt in range(3):
        try:
            r = get_session().get(url, params=params, timeout=45)
            with _lock:
                request_count += 1
            if r.status_code == 200 and r.text.strip():
                data = r.json()
                if data.get("success"):
                    return data.get("data", [])
            elif r.status_code == 204:
                return []
        except Exception as e:
            wait = 2 * (attempt + 1)
            time.sleep(wait)
            if attempt == 2:
                return []
    return []


def fetch_distrito(id_ambito, ambito_name, nom_d, ub_d, nom_p, ub_p, dist):
    """Fetch totales + participantes for a single distrito. Returns list of row dicts."""
    ub_dt = dist["ubigeo"]
    nom_dt = dist["nombre"]

    totales = api_get("/resumen-general/totales", {
        "idEleccion": ID_ELECCION, "idAmbitoGeografico": id_ambito,
        "tipoFiltro": "ubigeo_nivel_03",
        "idUbigeoDepartamento": ub_d, "idUbigeoProvincia": ub_p,
        "idUbigeoDistrito": ub_dt})
    t_actas = totales.get("totalActas", "") if isinstance(totales, dict) else ""
    t_contab = totales.get("contabilizadas", "") if isinstance(totales, dict) else ""
    t_pct = totales.get("actasContabilizadas", "") if isinstance(totales, dict) else ""

    data = api_get("/eleccion-presidencial/participantes-ubicacion-geografica-nombre", {
        "tipoFiltro": "ubigeo_nivel_03", "idAmbitoGeografico": id_ambito,
        "ubigeoNivel1": ub_d, "ubigeoNivel2": ub_p,
        "ubigeoNivel3": ub_dt, "idEleccion": ID_ELECCION})

    rows = []
    if data:
        for p in data:
            rows.append({
                "ambito": ambito_name, "region": nom_d,
                "provincia": nom_p, "distrito": nom_dt,
                "ubigeo_departamento": ub_d, "ubigeo_provincia": ub_p,
                "ubigeo_distrito": ub_dt,
                "total_actas": t_actas, "actas_contabilizadas": t_contab,
                "pct_actas_contabilizadas": t_pct,
                "partido": p.get("nombreAgrupacionPolitica", ""),
                "codigo_partido": str(p.get("codigoAgrupacionPolitica", "")),
                "candidato": p.get("nombreCandidato", ""),
                "dni_candidato": p.get("dniCandidato", ""),
                "votos": p.get("totalVotosValidos", 0),
                "pct_votos_validos": p.get("porcentajeVotosValidos", 0),
                "pct_votos_emitidos": p.get("porcentajeVotosEmitidos", 0),
            })
    return rows


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(DATA_DIR, f"resultados_presidenciales_{timestamp}.csv")

    fieldnames = [
        "ambito", "region", "provincia", "distrito",
        "ubigeo_departamento", "ubigeo_provincia", "ubigeo_distrito",
        "total_actas", "actas_contabilizadas", "pct_actas_contabilizadas",
        "partido", "codigo_partido", "candidato", "dni_candidato",
        "votos", "pct_votos_validos", "pct_votos_emitidos",
    ]

    # Load previous data — keep 100% districts, re-fetch the rest
    import glob
    prev_files = sorted(glob.glob(os.path.join(DATA_DIR, "resultados_presidenciales_*.csv")))
    completed_districts = {}  # {ubigeo_distrito: [rows]}
    if prev_files:
        import pandas as pd
        prev = pd.read_csv(prev_files[-1], dtype=str)
        prev["pct_actas_contabilizadas"] = pd.to_numeric(prev["pct_actas_contabilizadas"], errors="coerce").fillna(0)
        complete = prev[prev["pct_actas_contabilizadas"] >= 100.0]
        for ub, grp in complete.groupby("ubigeo_distrito"):
            completed_districts[ub] = grp.to_dict("records")
        print(f"Loaded {len(completed_districts)} completed districts from previous run")

    total_rows = 0
    total_distritos = 0
    skipped = 0
    t0 = time.time()

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        # Write completed districts from previous run
        for ub, rows in completed_districts.items():
            for row in rows:
                writer.writerow({k: row.get(k, "") for k in fieldnames})
            total_rows += len(rows)
            skipped += 1

        for id_ambito, ambito_name in [(1, "PERÚ"), (2, "EXTRANJERO")]:
            deptos = api_get("/ubigeos/departamentos", {
                "idEleccion": ID_ELECCION, "idAmbitoGeografico": id_ambito})

            for depto in deptos:
                ub_d = depto["ubigeo"]
                nom_d = depto["nombre"]
                provs = api_get("/ubigeos/provincias", {
                    "idEleccion": ID_ELECCION, "idAmbitoGeografico": id_ambito,
                    "idUbigeoDepartamento": ub_d})

                for prov in provs:
                    ub_p = prov["ubigeo"]
                    nom_p = prov["nombre"]
                    dists = api_get("/ubigeos/distritos", {
                        "idEleccion": ID_ELECCION, "idAmbitoGeografico": id_ambito,
                        "idUbigeoDepartamento": ub_d, "idUbigeoProvincia": ub_p})

                    # Filter out already-completed districts
                    to_fetch = [d for d in dists if d["ubigeo"] not in completed_districts]

                    if to_fetch:
                        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
                            futures = {
                                pool.submit(fetch_distrito, id_ambito, ambito_name,
                                            nom_d, ub_d, nom_p, ub_p, d): d
                                for d in to_fetch
                            }
                            for future in as_completed(futures):
                                rows = future.result()
                                for row in rows:
                                    writer.writerow(row)
                                total_rows += len(rows)
                                total_distritos += 1

                    f.flush()
                    time.sleep(0.5)
                    elapsed = time.time() - t0
                    rate = total_distritos / elapsed if elapsed > 0 else 0
                    n_skipped = len(dists) - len(to_fetch)
                    print(f"  {ambito_name} > {nom_d} > {nom_p}: "
                          f"{len(to_fetch)} fetched, {n_skipped} cached "
                          f"[{total_distritos + skipped} total, {rate:.1f} new/s]")

            print(f"\n{ambito_name} completado.\n")

    elapsed = time.time() - t0
    print(f"\n{'='*60}")
    print(f"Completado: {total_rows} filas, {total_distritos} nuevos + {skipped} cached")
    print(f"Tiempo: {elapsed/60:.1f} min")
    print(f"Archivo: {filename}")
    print(f"API requests: {request_count}")
    print(f"{'='*60}")

    # Save snapshot for time-series tracking
    try:
        import pandas as pd
        import sys
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from snapshot import save_snapshot
        snap_df = pd.read_csv(filename, dtype={"ubigeo_departamento": str, "ubigeo_provincia": str, "ubigeo_distrito": str})
        for col in ["votos", "total_actas", "actas_contabilizadas", "pct_actas_contabilizadas"]:
            snap_df[col] = pd.to_numeric(snap_df[col], errors="coerce").fillna(0)
        save_snapshot(snap_df)
    except Exception as e:
        print(f"  Warning: no se pudo guardar snapshot: {e}")


if __name__ == "__main__":
    main()
