#!/usr/bin/env python3
"""
Scraper segunda vuelta presidencial - ONPE Perú 2026
FP vs JP at distrito level.
"""

import os
import argparse
import glob
import requests
import csv
import json
import time
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

BASE = "https://resultadosegundavuelta.onpe.gob.pe/presentacion-backend"
ID_ELECCION = 10
WORKERS = 5
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
UBIGEO_CACHE = os.path.join(DATA_DIR, "ubigeo_cache.json")

_local = threading.local()
request_count = 0
_lock = threading.Lock()

REQUEST_HEADERS = {
    "Accept": "*/*",
    "Content-Type": "application/json",
    "Referer": "https://resultadosegundavuelta.onpe.gob.pe/main/presidenciales",
    "Origin": "https://resultadosegundavuelta.onpe.gob.pe",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}


def get_session():
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
            time.sleep(2 * (attempt + 1))
            if attempt == 2:
                return []
    return []


def fetch_distrito(id_ambito, ambito_name, nom_d, ub_d, nom_p, ub_p, dist):
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


def load_completed_districts(skip_completed):
    """Load rows from the most recent 2v CSV for districts already at 100%.

    Returns:
        completed_rows: list of dicts ready to write to the new CSV
        completed_ubigeos: set of ubigeo_distrito strings to skip
    """
    if not skip_completed:
        return [], set()

    files = sorted(glob.glob(os.path.join(DATA_DIR, "resultados_2v_*.csv")))
    if not files:
        print("No prior CSV found. Will scrape all districts.")
        return [], set()

    src = files[-1]
    completed_rows = []
    completed_ubigeos = set()
    with open(src, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                pct = float(row.get("pct_actas_contabilizadas", "") or 0)
            except ValueError:
                pct = 0
            if pct >= 100:
                completed_rows.append(row)
                completed_ubigeos.add(row["ubigeo_distrito"])

    print(f"Reusing {len(completed_ubigeos)} districts at 100% from {os.path.basename(src)} ({len(completed_rows)} rows)")
    return completed_rows, completed_ubigeos


def main():
    parser = argparse.ArgumentParser(description="Scraper segunda vuelta presidencial Perú 2026")
    parser.add_argument(
        "--full",
        action="store_true",
        help="Re-scrape every district (default skips districts already at 100%% from the previous run).",
    )
    args = parser.parse_args()
    skip_completed = not args.full

    os.makedirs(DATA_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(DATA_DIR, f"resultados_2v_{timestamp}.csv")

    fieldnames = [
        "ambito", "region", "provincia", "distrito",
        "ubigeo_departamento", "ubigeo_provincia", "ubigeo_distrito",
        "total_actas", "actas_contabilizadas", "pct_actas_contabilizadas",
        "partido", "codigo_partido", "candidato", "dni_candidato",
        "votos", "pct_votos_validos", "pct_votos_emitidos",
    ]

    # Load ubigeo hierarchy (reuse from 1ra vuelta)
    if os.path.exists(UBIGEO_CACHE):
        with open(UBIGEO_CACHE) as f:
            hierarchy = json.load(f)
        print(f"Ubigeo cache loaded")
    else:
        print("ERROR: No ubigeo cache. Run 1ra vuelta scraper first.")
        return

    completed_rows, skip_ubigeos = load_completed_districts(skip_completed)

    total_rows = 0
    total_distritos = 0
    skipped_distritos = 0
    t0 = time.time()

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        # Carry forward rows for districts already at 100%
        for row in completed_rows:
            # Only keep the columns we declare (drop extras if older CSV had them)
            writer.writerow({k: row.get(k, "") for k in fieldnames})
        total_rows += len(completed_rows)
        skipped_distritos = len(skip_ubigeos)
        total_distritos = skipped_distritos

        for ambito in hierarchy:
            id_ambito = ambito["id"]
            ambito_name = ambito["name"]

            for dep in ambito["deptos"]:
                ub_d = dep["ubigeo"]
                nom_d = dep["nombre"]

                for prov in dep["provs"]:
                    ub_p = prov["ubigeo"]
                    nom_p = prov["nombre"]
                    dists_to_fetch = [d for d in prov["dists"] if d["ubigeo"] not in skip_ubigeos]

                    if not dists_to_fetch:
                        continue

                    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
                        futures = {
                            pool.submit(fetch_distrito, id_ambito, ambito_name,
                                        nom_d, ub_d, nom_p, ub_p, d): d
                            for d in dists_to_fetch
                        }
                        for future in as_completed(futures):
                            rows = future.result()
                            for row in rows:
                                writer.writerow(row)
                            total_rows += len(rows)
                            total_distritos += 1

                    f.flush()
                    elapsed = time.time() - t0
                    rate = (total_distritos - skipped_distritos) / elapsed if elapsed > 0 else 0
                    skipped_in_prov = len(prov["dists"]) - len(dists_to_fetch)
                    skipped_note = f" (+{skipped_in_prov} cached)" if skipped_in_prov else ""
                    print(f"  {ambito_name} > {nom_d} > {nom_p}: {len(dists_to_fetch)} dist{skipped_note} [{total_distritos} total, {rate:.1f}/s scraped]")

            print(f"\n{ambito_name} completado.\n")

    elapsed = time.time() - t0
    print(f"\n{'='*60}")
    print(f"SEGUNDA VUELTA")
    print(f"Completado: {total_rows} filas, {total_distritos} distritos")
    if skip_completed:
        scraped = total_distritos - skipped_distritos
        print(f"  Reusados (100%): {skipped_distritos} | Scrapeados: {scraped}")
    print(f"Tiempo: {elapsed/60:.1f} min")
    print(f"Archivo: {filename}")
    print(f"API requests: {request_count}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
