#!/usr/bin/env python3
"""
Re-scrape 1ra vuelta results from the still-live ONPE API.
Uses ubigeo_cache.json (shared with 2v) so ubigeos align between rounds.
Writes to data/resultados_presidenciales_rescrape.csv
"""

import os
import csv
import json
import time
import requests
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

BASE = "https://resultadoelectoral.onpe.gob.pe/presentacion-backend"
ID_ELECCION = 10
WORKERS = 5
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
UBIGEO_CACHE = os.path.join(DATA_DIR, "ubigeo_cache.json")

_local = threading.local()
request_count = 0
_lock = threading.Lock()

HEADERS = {
    "Accept": "*/*",
    "Content-Type": "application/json",
    "Referer": "https://resultadoelectoral.onpe.gob.pe/main/presidenciales",
    "Origin": "https://resultadoelectoral.onpe.gob.pe",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}


def get_session():
    if not hasattr(_local, "session"):
        s = requests.Session()
        s.headers.update(HEADERS)
        retry = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
        s.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=5, pool_maxsize=5))
        _local.session = s
    return _local.session


def api_get(path, params=None):
    global request_count
    url = f"{BASE}{path}"
    for attempt in range(5):
        try:
            r = get_session().get(url, params=params, timeout=45)
            with _lock:
                request_count += 1
            if r.status_code == 200 and r.text.strip() and not r.text.startswith("<!"):
                data = r.json()
                if data.get("success"):
                    return data.get("data", [])
            elif r.status_code == 204:
                return []
        except Exception:
            pass
        if attempt < 4:
            time.sleep(min(0.5 * (2 ** attempt), 16))
    return []


def fetch_distrito(id_ambito, ambito_name, dep_name, ub_d, prov_name, ub_p, dist):
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
                "ambito": ambito_name, "region": dep_name,
                "provincia": prov_name, "distrito": nom_dt,
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
    with open(UBIGEO_CACHE) as f:
        hierarchy = json.load(f)
    print(f"Ubigeo cache loaded")

    filename = os.path.join(DATA_DIR, "resultados_presidenciales_rescrape.csv")
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

        for ambito in hierarchy:
            id_ambito = ambito["id"]
            ambito_name = ambito["name"]

            for dep in ambito["deptos"]:
                ub_d = dep["ubigeo"]
                dep_name = dep["nombre"]

                for prov in dep["provs"]:
                    ub_p = prov["ubigeo"]
                    prov_name = prov["nombre"]
                    dists = prov["dists"]

                    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
                        futures = {
                            pool.submit(fetch_distrito, id_ambito, ambito_name,
                                        dep_name, ub_d, prov_name, ub_p, d): d
                            for d in dists
                        }
                        for future in as_completed(futures):
                            rows = future.result()
                            for row in rows:
                                writer.writerow(row)
                            total_rows += len(rows)
                            total_distritos += 1

                    f.flush()
                    elapsed = time.time() - t0
                    rate = total_distritos / elapsed if elapsed > 0 else 0
                    print(f"  {ambito_name} > {dep_name} > {prov_name}: {len(dists)} dist [{total_distritos} total, {rate:.1f}/s]")

            print(f"\n{ambito_name} completado.\n")

    elapsed = time.time() - t0
    print(f"\n{'='*60}")
    print(f"1RA VUELTA RE-SCRAPE")
    print(f"Completado: {total_rows} filas, {total_distritos} distritos")
    print(f"Tiempo: {elapsed/60:.1f} min")
    print(f"Archivo: {filename}")
    print(f"API requests: {request_count}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
