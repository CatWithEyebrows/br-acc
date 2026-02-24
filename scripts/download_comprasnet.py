#!/usr/bin/env python3
"""Download federal procurement contracts from PNCP API.

Source: Portal Nacional de Contratações Públicas (pncp.gov.br)
Data: Federal contracts (contratos) — distinct from Transparência convênios.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

BASE_URL = "https://pncp.gov.br/api/consulta/v1/contratos"
PAGE_SIZE = 500
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data" / "comprasnet"


def fetch_page(
    date_start: str,
    date_end: str,
    page: int,
    retries: int = 3,
) -> dict:
    """Fetch a single page from the PNCP contracts API."""
    params = {
        "dataInicial": date_start,
        "dataFinal": date_end,
        "pagina": page,
        "tamanhoPagina": PAGE_SIZE,
    }
    for attempt in range(retries):
        try:
            resp = requests.get(BASE_URL, params=params, timeout=60)
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, json.JSONDecodeError) as exc:
            wait = 2 ** (attempt + 1)
            logger.warning(
                "Page %d attempt %d failed: %s — retrying in %ds",
                page, attempt + 1, exc, wait,
            )
            time.sleep(wait)
    logger.error("Page %d failed after %d retries, skipping", page, retries)
    return {"data": []}


def download_month(year: int, month: int) -> list[dict]:
    """Download all contracts for a given month."""
    # Calculate last day of month
    if month == 12:
        next_month_start = f"{year + 1}0101"
    else:
        next_month_start = f"{year}{month + 1:02d}01"
    from datetime import date, timedelta

    last_day = (
        date(
            year + 1 if month == 12 else year,
            1 if month == 12 else month + 1,
            1,
        )
        - timedelta(days=1)
    ).day

    date_start = f"{year}{month:02d}01"
    date_end = f"{year}{month:02d}{last_day:02d}"

    logger.info("Fetching %s to %s...", date_start, date_end)

    # Get first page to know total
    first = fetch_page(date_start, date_end, 1)
    total_records = first.get("totalRegistros", 0)
    total_pages = first.get("totalPaginas", 0)

    if not total_records:
        logger.info("  No records for %d-%02d", year, month)
        return []

    logger.info(
        "  %d records, %d pages for %d-%02d",
        total_records, total_pages, year, month,
    )

    all_records = list(first.get("data", []))

    for page in range(2, total_pages + 1):
        if page % 50 == 0:
            logger.info("  Page %d/%d...", page, total_pages)
        data = fetch_page(date_start, date_end, page)
        all_records.extend(data.get("data", []))
        # Respect rate limits
        time.sleep(0.2)

    logger.info(
        "  Downloaded %d records for %d-%02d",
        len(all_records), year, month,
    )
    return all_records


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    years = [2024]
    if len(sys.argv) > 1:
        years = [int(y) for y in sys.argv[1:]]

    total = 0
    for year in years:
        year_records: list[dict] = []
        for month in range(1, 13):
            out_file = OUTPUT_DIR / f"{year}_{month:02d}_contratos.json"
            if out_file.exists():
                existing = json.loads(out_file.read_text())
                logger.info(
                    "Skipping %d-%02d — already downloaded (%d records)",
                    year, month, len(existing),
                )
                year_records.extend(existing)
                continue

            records = download_month(year, month)
            out_file.write_text(json.dumps(records, ensure_ascii=False))
            year_records.extend(records)

        total += len(year_records)
        logger.info(
            "Year %d: %d total records downloaded", year, len(year_records),
        )

    logger.info("Grand total: %d contracts downloaded", total)


if __name__ == "__main__":
    main()
