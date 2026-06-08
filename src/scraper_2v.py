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
from collections import defaultdict

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


_API_SENTINEL = object()  # distinct from "real empty" so callers can detect failures


def api_get(path, params=None, max_attempts=5):
    """Hit the ONPE backend with a real exponential backoff.

    Returns the response payload (could be a list, dict, or empty list when the
    server explicitly returns 204). Returns _API_SENTINEL when every attempt
    failed — callers should treat that as "do not write a row, do not assume
    empty data, retry on next cycle".
    """
    global request_count
    url = f"{BASE}{path}"
    last_error = None
    for attempt in range(max_attempts):
        try:
            r = get_session().get(url, params=params, timeout=45)
            with _lock:
                request_count += 1
            if r.status_code == 200 and r.text.strip():
                try:
                    payload = r.json()
                except ValueError as e:
                    last_error = f"bad json: {e}"
                    payload = None
                if payload is not None:
                    if payload.get("success"):
                        return payload.get("data", [])
                    last_error = f"success=false: {str(payload)[:120]}"
            elif r.status_code == 204:
                return []
            else:
                last_error = f"status {r.status_code}"
        except Exception as e:
            last_error = f"exception: {e}"
        # Exponential backoff with jitter; cap at ~16s
        if attempt < max_attempts - 1:
            time.sleep(min(0.5 * (2 ** attempt), 16))
    print(f"  [api_get] giving up on {path} params={params}: {last_error}")
    return _API_SENTINEL


def fetch_distrito(id_ambito, ambito_name, nom_d, ub_d, nom_p, ub_p, dist):
    """Fetch a single district. Returns (rows, ok).

    ok=False signals a hard API failure for the participantes endpoint —
    the caller must NOT replace cached data for this district when ok is False.
    """
    ub_dt = dist["ubigeo"]
    nom_dt = dist["nombre"]

    totales = api_get("/resumen-general/totales", {
        "idEleccion": ID_ELECCION, "idAmbitoGeografico": id_ambito,
        "tipoFiltro": "ubigeo_nivel_03",
        "idUbigeoDepartamento": ub_d, "idUbigeoProvincia": ub_p,
        "idUbigeoDistrito": ub_dt})
    if totales is _API_SENTINEL or not isinstance(totales, dict):
        t_actas = t_contab = t_pct = ""
    else:
        t_actas = totales.get("totalActas", "")
        t_contab = totales.get("contabilizadas", "")
        t_pct = totales.get("actasContabilizadas", "")

    data = api_get("/eleccion-presidencial/participantes-ubicacion-geografica-nombre", {
        "tipoFiltro": "ubigeo_nivel_03", "idAmbitoGeografico": id_ambito,
        "ubigeoNivel1": ub_d, "ubigeoNivel2": ub_p,
        "ubigeoNivel3": ub_dt, "idEleccion": ID_ELECCION})

    if data is _API_SENTINEL:
        return [], False

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
    # An empty list means the API responded successfully but had nothing — that's
    # legitimate for districts that haven't published any actas yet. Caller is
    # free to replace cached data for them.
    return rows, True


CACHE_FILE = os.path.join(DATA_DIR, "scrape_cache_2v.csv")


def _row_pct(row):
    try:
        return float(row.get("pct_actas_contabilizadas", "") or 0)
    except ValueError:
        return 0.0


def _row_contab(row):
    try:
        return float(row.get("actas_contabilizadas", "") or 0)
    except ValueError:
        return 0.0


def load_cache():
    """Load the persistent scrape cache. Returns dict[ubigeo_distrito] -> list[row]."""
    cache = defaultdict(list)
    if not os.path.exists(CACHE_FILE):
        # Bootstrap from the most recent CSV so we don't need a full re-scrape on first migration
        files = sorted(glob.glob(os.path.join(DATA_DIR, "resultados_2v_*.csv")))
        if files:
            src = files[-1]
            print(f"No cache yet; bootstrapping from {os.path.basename(src)}")
            with open(src, "r", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    cache[row["ubigeo_distrito"]].append(row)
        return cache
    with open(CACHE_FILE, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            cache[row["ubigeo_distrito"]].append(row)
    print(f"Cache loaded: {len(cache)} districts")
    return cache


def save_cache(cache, fieldnames):
    """Write the cache atomically."""
    tmp = CACHE_FILE + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for ub in sorted(cache.keys()):
            for row in cache[ub]:
                writer.writerow({k: row.get(k, "") for k in fieldnames})
    os.replace(tmp, CACHE_FILE)


def cache_district_pct(cache, ub):
    """Return the highest pct_actas_contabilizadas seen for a district in the cache."""
    rows = cache.get(ub, [])
    if not rows:
        return 0.0
    return max(_row_pct(r) for r in rows)


def cache_district_contab(cache, ub):
    rows = cache.get(ub, [])
    if not rows:
        return 0.0
    return max(_row_contab(r) for r in rows)


FIELDNAMES = [
    "ambito", "region", "provincia", "distrito",
    "ubigeo_departamento", "ubigeo_provincia", "ubigeo_distrito",
    "total_actas", "actas_contabilizadas", "pct_actas_contabilizadas",
    "partido", "codigo_partido", "candidato", "dni_candidato",
    "votos", "pct_votos_validos", "pct_votos_emitidos",
]


def main():
    parser = argparse.ArgumentParser(description="Scraper segunda vuelta presidencial Perú 2026")
    parser.add_argument(
        "--full",
        action="store_true",
        help="Re-scrape every district (default skips districts already at 100%% in the cache).",
    )
    args = parser.parse_args()
    skip_completed = not args.full

    os.makedirs(DATA_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(DATA_DIR, f"resultados_2v_{timestamp}.csv")

    # Load ubigeo hierarchy (reuse from 1ra vuelta)
    if os.path.exists(UBIGEO_CACHE):
        with open(UBIGEO_CACHE) as f:
            hierarchy = json.load(f)
        print(f"Ubigeo cache loaded")
    else:
        print("ERROR: No ubigeo cache. Run 1ra vuelta scraper first.")
        return

    # Persistent district cache: best-known state per district across runs.
    # Districts at 100% in the cache are NEVER re-fetched (unless --full).
    cache = load_cache()
    skip_ubigeos = set()
    if skip_completed:
        for ub in cache:
            if cache_district_pct(cache, ub) >= 100:
                skip_ubigeos.add(ub)
        print(f"Skipping {len(skip_ubigeos)} districts at 100% from cache")

    t0 = time.time()
    n_done = 0
    n_failed = 0
    n_updated = 0  # cache rows replaced with newer data
    n_kept = 0     # cache rows kept (because new data was empty/older or scrape failed)

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
                        d = futures[future]
                        ub = d["ubigeo"]
                        rows, ok = future.result()
                        if not ok:
                            # API hard failure — keep whatever cache has
                            n_failed += 1
                            n_kept += 1
                            continue
                        new_contab = max((_row_contab(r) for r in rows), default=0.0)
                        prev_contab = cache_district_contab(cache, ub)
                        if rows and new_contab >= prev_contab:
                            cache[ub] = rows
                            n_updated += 1
                        else:
                            # Empty or regressed — keep cache
                            n_kept += 1
                        n_done += 1

                elapsed = time.time() - t0
                rate = n_done / elapsed if elapsed > 0 else 0
                skipped_in_prov = len(prov["dists"]) - len(dists_to_fetch)
                skipped_note = f" (+{skipped_in_prov} cached)" if skipped_in_prov else ""
                print(f"  {ambito_name} > {nom_d} > {nom_p}: {len(dists_to_fetch)} dist{skipped_note} [{n_done} scraped @ {rate:.1f}/s, {n_failed} fails]")

        print(f"\n{ambito_name} completado.\n")

    # Persist the cache and emit the per-cycle CSV from it
    save_cache(cache, FIELDNAMES)
    total_rows = 0
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        for ub in sorted(cache.keys()):
            for row in cache[ub]:
                writer.writerow({k: row.get(k, "") for k in FIELDNAMES})
                total_rows += 1

    elapsed = time.time() - t0
    print(f"\n{'='*60}")
    print(f"SEGUNDA VUELTA")
    print(f"Cache districts: {len(cache)}")
    print(f"  Skipped (100%):  {len(skip_ubigeos)}")
    print(f"  Re-scraped:      {n_done}")
    print(f"  API failures:    {n_failed}")
    print(f"  Cache updates:   {n_updated}")
    print(f"  Cache preserved: {n_kept}")
    print(f"Tiempo: {elapsed/60:.1f} min  |  API requests: {request_count}")
    print(f"Archivo: {filename}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
