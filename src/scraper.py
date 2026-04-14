#!/usr/bin/env python3
"""
Scraper de resultados electorales presidenciales - ONPE Perú 2026
Extrae datos a nivel DISTRITO desde la API de la ONPE.
"""

import os
import requests
import csv
import time
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE = "https://resultadoelectoral.onpe.gob.pe/presentacion-backend"
ID_ELECCION = 10
DELAY = 1.0
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")

session = requests.Session()
session.headers.update({
    "Accept": "application/json",
    "Referer": "https://resultadoelectoral.onpe.gob.pe/main/presidenciales",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
})
retry_strategy = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=1, pool_maxsize=1)
session.mount("https://", adapter)

request_count = 0


def api_get(path, params=None, retries=3):
    global request_count
    url = f"{BASE}{path}"
    for attempt in range(retries):
        try:
            r = session.get(url, params=params, timeout=45)
            request_count += 1
            if r.status_code == 200 and r.text.strip():
                data = r.json()
                if data.get("success"):
                    return data.get("data", [])
            elif r.status_code == 204:
                return []
        except Exception as e:
            wait = 3 * (attempt + 1)
            print(f"  RETRY {attempt+1}/{retries}: {e} (waiting {wait}s)")
            time.sleep(wait)
            if attempt == retries - 1:
                print(f"  ERROR: {path}")
                return []
    return []


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

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for id_ambito, ambito_name in [(1, "PERÚ"), (2, "EXTRANJERO")]:
            deptos = api_get("/ubigeos/departamentos", {
                "idEleccion": ID_ELECCION, "idAmbitoGeografico": id_ambito})
            time.sleep(DELAY)

            for depto in deptos:
                ub_d = depto["ubigeo"]
                nom_d = depto["nombre"]

                provs = api_get("/ubigeos/provincias", {
                    "idEleccion": ID_ELECCION, "idAmbitoGeografico": id_ambito,
                    "idUbigeoDepartamento": ub_d})
                time.sleep(DELAY)

                for prov in provs:
                    ub_p = prov["ubigeo"]
                    nom_p = prov["nombre"]

                    dists = api_get("/ubigeos/distritos", {
                        "idEleccion": ID_ELECCION, "idAmbitoGeografico": id_ambito,
                        "idUbigeoDepartamento": ub_d, "idUbigeoProvincia": ub_p})
                    time.sleep(DELAY)

                    for dist in dists:
                        ub_dt = dist["ubigeo"]
                        nom_dt = dist["nombre"]

                        totales = api_get("/resumen-general/totales", {
                            "idEleccion": ID_ELECCION,
                            "idAmbitoGeografico": id_ambito,
                            "tipoFiltro": "ubigeo_nivel_03",
                            "idUbigeoDepartamento": ub_d,
                            "idUbigeoProvincia": ub_p,
                            "idUbigeoDistrito": ub_dt})
                        t_actas = totales.get("totalActas", "") if isinstance(totales, dict) else ""
                        t_contab = totales.get("contabilizadas", "") if isinstance(totales, dict) else ""
                        t_pct = totales.get("actasContabilizadas", "") if isinstance(totales, dict) else ""
                        time.sleep(DELAY)

                        data = api_get(
                            "/eleccion-presidencial/participantes-ubicacion-geografica-nombre", {
                                "tipoFiltro": "ubigeo_nivel_03",
                                "idAmbitoGeografico": id_ambito,
                                "ubigeoNivel1": ub_d, "ubigeoNivel2": ub_p,
                                "ubigeoNivel3": ub_dt, "idEleccion": ID_ELECCION})

                        if data:
                            for p in data:
                                writer.writerow({
                                    "ambito": ambito_name,
                                    "region": nom_d,
                                    "provincia": nom_p,
                                    "distrito": nom_dt,
                                    "ubigeo_departamento": ub_d,
                                    "ubigeo_provincia": ub_p,
                                    "ubigeo_distrito": ub_dt,
                                    "total_actas": t_actas,
                                    "actas_contabilizadas": t_contab,
                                    "pct_actas_contabilizadas": t_pct,
                                    "partido": p.get("nombreAgrupacionPolitica", ""),
                                    "codigo_partido": str(p.get("codigoAgrupacionPolitica", "")),
                                    "candidato": p.get("nombreCandidato", ""),
                                    "dni_candidato": p.get("dniCandidato", ""),
                                    "votos": p.get("totalVotosValidos", 0),
                                    "pct_votos_validos": p.get("porcentajeVotosValidos", 0),
                                    "pct_votos_emitidos": p.get("porcentajeVotosEmitidos", 0),
                                })
                            total_rows += len(data)

                        total_distritos += 1
                        time.sleep(DELAY)

                    f.flush()
                    print(f"  {ambito_name} > {nom_d} > {nom_p}: {len(dists)} distritos")

            print(f"\n{ambito_name} completado.\n")

    print(f"\n{'='*60}")
    print(f"Completado: {total_rows} filas, {total_distritos} distritos")
    print(f"Archivo: {filename}")
    print(f"API requests: {request_count}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
