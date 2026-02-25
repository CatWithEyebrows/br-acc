#!/usr/bin/env python3
"""Download PNCP procurement bid publications via REST API.

Fetches data from the PNCP public API in date-range windows and saves
as JSON files for pipeline consumption.

API: https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao
Swagger: https://pncp.gov.br/api/consulta/swagger-ui/index.html

Usage:
    python etl/scripts/download_pncp.py
    python etl/scripts/download_pncp.py --start-date 2024-01-01 --end-date 2024-12-31
    python etl/scripts/download_pncp.py --output-dir ./data/pncp --modalidades 6,8,9
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

import click
import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

API_BASE = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"

# All PNCP modalidade codes (procurement types)
ALL_MODALIDADES = [1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]

# API constraints discovered via testing
MAX_PAGE_SIZE = 50
MAX_DATE_RANGE_DAYS = 10  # Smaller windows for reliability
REQUEST_DELAY_SECONDS = 1.0


def _fetch_page(
    client: httpx.Client,
    date_start: str,
    date_end: str,
    modalidade: int,
    page: int,
) -> dict:
    """Fetch a single page from the PNCP API.

    Returns parsed JSON response dict.
    Raises httpx.HTTPStatusError on non-200 responses.
    """
    params = {
        "dataInicial": date_start,
        "dataFinal": date_end,
        "codigoModalidadeContratacao": modalidade,
        "pagina": page,
        "tamanhoPagina": MAX_PAGE_SIZE,
    }
    response = client.get(API_BASE, params=params)
    response.raise_for_status()
    # PNCP sometimes returns invalid control characters in JSON text fields
    return json.loads(response.text, strict=False)  # type: ignore[no-any-return]


def _fetch_window(
    client: httpx.Client,
    date_start: str,
    date_end: str,
    modalidade: int,
) -> list[dict]:
    """Fetch all pages for a single date window + modalidade combination."""
    all_records: list[dict] = []
    page = 1

    while True:
        try:
            data = _fetch_page(client, date_start, date_end, modalidade, page)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 400:
                # No data or invalid range — not an error
                break
            logger.warning(
                "API error for %s-%s mod=%d page=%d: %s",
                date_start, date_end, modalidade, page, e,
            )
            break
        except httpx.HTTPError as e:
            logger.warning(
                "Network error for %s-%s mod=%d page=%d: %s",
                date_start, date_end, modalidade, page, e,
            )
            break

        items = data.get("data", [])
        if not items or data.get("empty", True):
            break

        all_records.extend(items)
        total_pages = data.get("totalPaginas", 1)
        remaining = data.get("paginasRestantes", 0)

        if page >= total_pages or remaining <= 0:
            break

        page += 1
        time.sleep(REQUEST_DELAY_SECONDS)

    return all_records


def _date_windows(
    start: datetime, end: datetime, window_days: int,
) -> list[tuple[str, str]]:
    """Generate (start_yyyymmdd, end_yyyymmdd) tuples for date windows."""
    windows: list[tuple[str, str]] = []
    current = start
    while current < end:
        window_end = min(current + timedelta(days=window_days - 1), end)
        windows.append((
            current.strftime("%Y%m%d"),
            window_end.strftime("%Y%m%d"),
        ))
        current = window_end + timedelta(days=1)
    return windows


@click.command()
@click.option(
    "--start-date",
    default=lambda: (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d"),
    help="Start date (YYYY-MM-DD). Default: 6 months ago.",
)
@click.option(
    "--end-date",
    default=lambda: datetime.now().strftime("%Y-%m-%d"),
    help="End date (YYYY-MM-DD). Default: today.",
)
@click.option(
    "--modalidades",
    default=",".join(str(m) for m in ALL_MODALIDADES),
    help="Comma-separated modalidade codes. Default: all.",
)
@click.option("--output-dir", default="./data/pncp", help="Output directory")
@click.option("--window-days", type=int, default=MAX_DATE_RANGE_DAYS, help="Days per API window")
@click.option("--skip-existing/--no-skip-existing", default=True, help="Skip existing files")
@click.option("--timeout", type=int, default=60, help="HTTP request timeout in seconds")
def main(
    start_date: str,
    end_date: str,
    modalidades: str,
    output_dir: str,
    window_days: int,
    skip_existing: bool,
    timeout: int,
) -> None:
    """Download PNCP procurement bid publications."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    mod_list = [int(m.strip()) for m in modalidades.split(",")]

    logger.info("=== PNCP Download ===")
    logger.info("Date range: %s to %s", start_date, end_date)
    logger.info("Modalidades: %s", mod_list)

    windows = _date_windows(start, end, window_days)
    logger.info("Date windows: %d", len(windows))

    # Group output by month
    monthly_records: dict[str, list[dict]] = {}

    client = httpx.Client(
        timeout=timeout,
        follow_redirects=True,
        headers={"User-Agent": "ICARUS-ETL/1.0 (public data research)"},
    )

    try:
        for win_start, win_end in windows:
            for mod in mod_list:
                logger.info("Fetching %s-%s modalidade=%d...", win_start, win_end, mod)
                records = _fetch_window(client, win_start, win_end, mod)

                if records:
                    # Group by publication month
                    for rec in records:
                        pub_date = str(rec.get("dataPublicacaoPncp", win_start))
                        if "-" in pub_date:
                            month_key = pub_date[:7].replace("-", "")
                        else:
                            month_key = win_start[:6]
                        monthly_records.setdefault(month_key, []).extend([rec])

                    logger.info(
                        "  Fetched %d records for %s-%s mod=%d",
                        len(records), win_start, win_end, mod,
                    )

                time.sleep(REQUEST_DELAY_SECONDS)
    finally:
        client.close()

    # Write monthly JSON files
    total_written = 0
    for month_key, records in sorted(monthly_records.items()):
        out_file = out / f"pncp_{month_key}.json"

        if skip_existing and out_file.exists():
            # Merge with existing data
            existing = json.loads(out_file.read_text(encoding="utf-8"), strict=False)
            if isinstance(existing, dict) and "data" in existing:
                existing_data = existing["data"]
            elif isinstance(existing, list):
                existing_data = existing
            else:
                existing_data = []

            # Deduplicate by numeroControlePNCP
            seen_ids: set[str] = {
                str(r.get("numeroControlePNCP", "")) for r in existing_data
            }
            new_records = [
                r for r in records
                if str(r.get("numeroControlePNCP", "")) not in seen_ids
            ]
            records = existing_data + new_records

        out_file.write_text(
            json.dumps(records, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info("Wrote %d records to %s", len(records), out_file.name)
        total_written += len(records)

    logger.info(
        "=== Done: %d total records across %d files ===",
        total_written, len(monthly_records),
    )


if __name__ == "__main__":
    main()
