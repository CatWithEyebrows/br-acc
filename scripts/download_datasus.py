#!/usr/bin/env python3
"""Download CNES health facility data from DATASUS Open Data API.

Fetches all ~603K establishments using high-concurrency parallel requests.

Usage:
    python3 scripts/download_datasus.py
"""

from __future__ import annotations

import csv
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.request import Request, urlopen

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

API_BASE = "https://apidadosabertos.saude.gov.br/cnes/estabelecimentos"
PAGE_SIZE = 20  # API maximum
MAX_WORKERS = 50
MAX_OFFSET = 604_000  # ~603K records total
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data" / "datasus"

FIELDS = [
    "codigo_cnes",
    "numero_cnpj_entidade",
    "nome_razao_social",
    "nome_fantasia",
    "codigo_tipo_unidade",
    "descricao_esfera_administrativa",
    "codigo_municipio",
    "codigo_uf",
    "numero_cnpj",
    "estabelecimento_faz_atendimento_ambulatorial_sus",
    "estabelecimento_possui_atendimento_hospitalar",
    "estabelecimento_possui_centro_cirurgico",
    "descricao_natureza_juridica_estabelecimento",
    "data_atualizacao",
]


def fetch_page(offset: int, retries: int = 3) -> list[dict]:
    """Fetch a single page from the CNES API."""
    url = f"{API_BASE}?limit={PAGE_SIZE}&offset={offset}"
    for attempt in range(retries):
        try:
            req = Request(url, headers={"Accept": "application/json"})
            with urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                return data.get("estabelecimentos", [])
        except Exception as e:
            if attempt < retries - 1:
                wait = 2 ** (attempt + 1)
                time.sleep(wait)
            else:
                logger.error("Failed offset=%d after %d retries: %s", offset, retries, e)
                return []
    return []


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    outfile = OUTPUT_DIR / "cnes_all.csv"
    if outfile.exists():
        with open(outfile) as f:
            existing = sum(1 for _ in f) - 1
        if existing > 500_000:
            logger.info("Found existing cnes_all.csv with %d records, skipping", existing)
            return
        logger.info("Found partial cnes_all.csv with %d records, re-downloading", existing)

    offsets = list(range(0, MAX_OFFSET, PAGE_SIZE))
    logger.info(
        "Downloading ~%d records via %d API requests with %d workers...",
        MAX_OFFSET, len(offsets), MAX_WORKERS,
    )

    start = time.time()
    all_records: list[tuple[int, list[dict]]] = []
    completed = 0
    empty_streak = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(fetch_page, off): off for off in offsets}
        for future in as_completed(futures):
            off = futures[future]
            try:
                page = future.result()
                if page:
                    all_records.append((off, page))
                    empty_streak = 0
                else:
                    empty_streak += 1
            except Exception as e:
                logger.error("offset=%d: %s", off, e)

            completed += 1
            if completed % 1000 == 0:
                elapsed = time.time() - start
                records_so_far = sum(len(p) for _, p in all_records)
                rate = records_so_far / elapsed if elapsed > 0 else 0
                eta = (MAX_OFFSET - records_so_far) / rate / 60 if rate > 0 else 0
                logger.info(
                    "Progress: %d/%d requests, %d records, %.0f rec/s, ETA %.0f min",
                    completed, len(offsets), records_so_far, rate, eta,
                )

    # Sort by offset to maintain order
    all_records.sort(key=lambda x: x[0])

    # Write CSV
    total = 0
    with open(outfile, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        for _, page in all_records:
            for record in page:
                writer.writerow(record)
                total += 1

    elapsed = time.time() - start
    logger.info("Downloaded %d records in %.1f minutes -> %s", total, elapsed / 60, outfile)


if __name__ == "__main__":
    main()
