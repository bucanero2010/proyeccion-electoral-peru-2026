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

    total_rows = 0
    total_distritos = 0
    t0 = time.time()

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

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

                    # Fetch all distritos in this provincia concurrently
                    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
                        futures = {
                            pool.submit(fetch_distrito, id_ambito, ambito_name,
                                        nom_d, ub_d, nom_p, ub_p, d): d
                            for d in dists
                        }
                        for future in as_completed(futures):
                            rows = future.result()
                            for row in rows:
                                writer.writerow(row)
                            total_rows += len(rows)
                            total_distritos += 1

                    f.flush()
                    time.sleep(0.5)  # brief pause between provincias
                    elapsed = time.time() - t0
                    rate = total_distritos / elapsed if elapsed > 0 else 0
                    print(f"  {ambito_name} > {nom_d} > {nom_p}: {len(dists)} dist "
                          f"[{total_distritos} total, {rate:.1f} dist/s]")

            print(f"\n{ambito_name} completado.\n")

    elapsed = time.time() - t0
    print(f"\n{'='*60}")
    print(f"Completado: {total_rows} filas, {total_distritos} distritos")
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
