#!/usr/bin/env python3
"""Download DOU (Diario Oficial da Uniao) acts from Imprensa Nacional.

Uses the IN search API to download structured gazette act data.
Saves JSON files per date in data/dou/.

Usage:
    python etl/scripts/download_dou.py
    python etl/scripts/download_dou.py --start-date 2024-01-01 --end-date 2024-01-31
    python etl/scripts/download_dou.py --days 7
    python etl/scripts/download_dou.py --output-dir ./data/dou --delay 2.0
"""

from __future__ import annotations

import json
import logging
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import click
import httpx

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

# Imprensa Nacional search API
SEARCH_URL = "https://www.in.gov.br/consulta/-/buscar/dou"

# Default page size (max the API accepts)
PAGE_SIZE = 20

# User-Agent to reduce bot detection
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}


def _fetch_page(
    client: httpx.Client,
    date_str: str,
    page: int,
    *,
    timeout: int,
) -> list[dict] | None:
    """Fetch one page of DOU results for a given date.

    Args:
        client: HTTP client instance.
        date_str: Date in DD-MM-YYYY format.
        page: Zero-indexed page number.
        timeout: Request timeout in seconds.

    Returns:
        List of act dicts, or None on failure.
    """
    params = {
        "q": "*",
        "exactDate": date_str,
        "sortBy": "publicacao",
        "s": "desc",
        "delta": str(PAGE_SIZE),
        "currentPage": str(page),
    }

    try:
        response = client.get(SEARCH_URL, params=params, timeout=timeout)
        response.raise_for_status()

        data = response.json()

        # The API returns a wrapper with jsonArray
        if isinstance(data, dict) and "jsonArray" in data:
            return data["jsonArray"]
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            # Sometimes the response has items nested differently
            for key in ("items", "results", "content"):
                if key in data and isinstance(data[key], list):
                    return data[key]
        logger.warning("Unexpected response format for %s page %d", date_str, page)
        return []

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 403:
            logger.warning(
                "Access blocked (403) for %s page %d — API may be rate-limiting",
                date_str,
                page,
            )
        else:
            logger.warning("HTTP %d for %s page %d", e.response.status_code, date_str, page)
        return None

    except (httpx.RequestError, json.JSONDecodeError) as e:
        logger.warning("Request failed for %s page %d: %s", date_str, page, e)
        return None


def _download_date(
    client: httpx.Client,
    target_date: datetime,
    output_dir: Path,
    *,
    skip_existing: bool,
    max_pages: int,
    timeout: int,
    delay: float,
) -> int:
    """Download all acts for a single date.

    Returns:
        Number of acts downloaded.
    """
    date_str = target_date.strftime("%d-%m-%Y")
    file_date = target_date.strftime("%Y%m%d")
    output_path = output_dir / f"dou_{file_date}.json"

    if skip_existing and output_path.exists():
        logger.info("Skipping %s (exists)", output_path.name)
        return 0

    all_acts: list[dict] = []
    page = 0
    blocked = False

    while page < max_pages:
        acts = _fetch_page(client, date_str, page, timeout=timeout)

        if acts is None:
            blocked = True
            break

        if not acts:
            break

        all_acts.extend(acts)
        logger.info(
            "  %s page %d: %d acts (total: %d)",
            date_str, page, len(acts), len(all_acts),
        )

        if len(acts) < PAGE_SIZE:
            break

        page += 1
        if delay > 0:
            time.sleep(delay)

    if all_acts:
        output_path.write_text(
            json.dumps(all_acts, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info("Saved %d acts to %s", len(all_acts), output_path.name)
    elif blocked:
        logger.warning("No data saved for %s (blocked)", date_str)
    else:
        logger.info("No acts published on %s", date_str)

    return len(all_acts)


@click.command()
@click.option(
    "--start-date",
    default=None,
    help="Start date (YYYY-MM-DD). Defaults to --days ago.",
)
@click.option(
    "--end-date",
    default=None,
    help="End date (YYYY-MM-DD). Defaults to today.",
)
@click.option("--days", type=int, default=30, help="Days back from today (if no start-date)")
@click.option("--output-dir", default="./data/dou", help="Output directory")
@click.option("--skip-existing/--no-skip-existing", default=True, help="Skip existing files")
@click.option("--max-pages", type=int, default=50, help="Max pages per date")
@click.option("--timeout", type=int, default=30, help="Request timeout in seconds")
@click.option("--delay", type=float, default=1.0, help="Delay between requests in seconds")
def main(
    start_date: str | None,
    end_date: str | None,
    days: int,
    output_dir: str,
    skip_existing: bool,
    max_pages: int,
    timeout: int,
    delay: float,
) -> None:
    """Download DOU acts from Imprensa Nacional search API."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    end = datetime.strptime(end_date, "%Y-%m-%d") if end_date else datetime.now()
    start = (
        datetime.strptime(start_date, "%Y-%m-%d")
        if start_date
        else end - timedelta(days=days)
    )

    logger.info("=== DOU Download ===")
    logger.info("Date range: %s to %s", start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    logger.info("Output: %s", out.resolve())

    total_acts = 0
    total_days = 0
    blocked_days = 0

    with httpx.Client(headers=HEADERS, follow_redirects=True) as client:
        current = start
        while current <= end:
            # Skip weekends (no DOU published)
            if current.weekday() >= 5:
                current += timedelta(days=1)
                continue

            count = _download_date(
                client,
                current,
                out,
                skip_existing=skip_existing,
                max_pages=max_pages,
                timeout=timeout,
                delay=delay,
            )
            total_acts += count
            total_days += 1

            if count == 0 and not skip_existing:
                blocked_days += 1

            # Delay between dates
            if delay > 0:
                time.sleep(delay)

            current += timedelta(days=1)

    logger.info("=== Done ===")
    logger.info(
        "Downloaded %d acts across %d days (%d days with no data)",
        total_acts, total_days, blocked_days,
    )

    if blocked_days > total_days * 0.5:
        logger.warning(
            "More than half the days returned no data — API may be blocking requests. "
            "Try increasing --delay or using a browser to download manually."
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
