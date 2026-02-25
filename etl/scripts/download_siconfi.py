#!/usr/bin/env python3
"""Download SICONFI municipal finance data from Tesouro Nacional API.

API docs: https://apidatalake.tesouro.gov.br/ords/siconfi/tt/
No authentication required.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

BASE_URL = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt"

# RREO = Relatório Resumido da Execução Orçamentária
# RGF = Relatório de Gestão Fiscal
# DCA = Declaração de Contas Anuais
ENDPOINTS = {
    "dca": "/dca",
}


def download_dca(output_dir: Path, year: int) -> None:
    """Download DCA (annual accounts) for a given year."""
    url = f"{BASE_URL}/dca"
    params = {"an_exercicio": year, "no_anexo": "DCA-Anexo I-C"}

    logger.info("Fetching DCA for %d...", year)
    try:
        response = httpx.get(
            url,
            params=params,
            timeout=120,
            headers={"User-Agent": "ICARUS-ETL/1.0"},
        )
        response.raise_for_status()
        data = response.json()
        items = data.get("items", [])
        logger.info("Got %d DCA records for %d", len(items), year)

        dest = output_dir / f"dca_{year}.json"
        with open(dest, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False)
        logger.info("Saved to %s", dest)

    except httpx.HTTPError:
        logger.warning("Failed to fetch DCA for %d", year)


def main() -> None:
    parser = argparse.ArgumentParser(description="Download SICONFI data")
    parser.add_argument("--start-year", type=int, default=2022, help="Start year")
    parser.add_argument("--end-year", type=int, default=2024, help="End year")
    parser.add_argument("--output-dir", default="./data/siconfi", help="Output directory")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for year in range(args.start_year, args.end_year + 1):
        download_dca(output_dir, year)
        time.sleep(2)  # Be polite to API


if __name__ == "__main__":
    main()
