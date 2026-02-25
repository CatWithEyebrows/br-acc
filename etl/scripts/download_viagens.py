#!/usr/bin/env python3
"""Download Government Travel (Viagens a Servico) data from Portal da Transparencia.

Usage:
    python etl/scripts/download_viagens.py
    python etl/scripts/download_viagens.py --start-year 2020 --end-year 2025
    python etl/scripts/download_viagens.py --output-dir ./data/viagens
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path

import click
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from _download_utils import download_file, extract_zip, validate_csv

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://portaldatransparencia.gov.br/download-de-dados"

# Real government CSV columns -> pipeline expected columns
COLUMN_MAP = {
    "Código do órgão superior": "cod_orgao_superior",
    "Nome do órgão superior": "nome_orgao_superior",
    "Código órgão": "cod_orgao",
    "Nome órgão": "nome_orgao",
    "CPF servidor": "cpf",
    "Nome servidor": "nome",
    "Cargo": "cargo",
    "Função": "funcao",
    "Descrição Função": "descricao_funcao",
    "Período - Data de início": "data_inicio",
    "Período - Data de fim": "data_fim",
    "Destinos": "destinos",
    "Motivo": "motivo",
    "Valor diárias": "valor_diarias",
    "Valor passagens": "valor_passagens",
    "Valor outros gastos": "valor_outros",
}


def _find_csvs(directory: Path) -> list[Path]:
    """Find all CSV files in a directory."""
    return sorted(directory.glob("*.csv"))


def _remap_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Remap government column names to pipeline-expected names.

    Tries exact match first, then case-insensitive match.
    """
    rename_map: dict[str, str] = {}
    upper_map = {k.upper(): v for k, v in COLUMN_MAP.items()}

    for col in df.columns:
        col_stripped = col.strip()
        if col_stripped in COLUMN_MAP:
            rename_map[col] = COLUMN_MAP[col_stripped]
        elif col_stripped.upper() in upper_map:
            rename_map[col] = upper_map[col_stripped.upper()]

    if rename_map:
        df = df.rename(columns=rename_map)

    found = set(rename_map.values())
    expected = set(COLUMN_MAP.values())
    missing = expected - found
    if missing:
        logger.warning("Viagens: missing columns after mapping: %s", missing)

    return df


def _download_month(
    year: int,
    month: int,
    raw_dir: Path,
    *,
    skip_existing: bool,
    timeout: int,
) -> list[Path]:
    """Download a single month's ZIP and return extracted CSV paths."""
    date_str = f"{year}{month:02d}"
    url = f"{BASE_URL}/viagens/{date_str}"
    zip_name = f"viagens_{date_str}.zip"
    zip_path = raw_dir / zip_name

    if skip_existing and zip_path.exists():
        logger.info("Skipping (exists): %s", zip_name)
    else:
        if not download_file(url, zip_path, timeout=timeout):
            return []

    extract_dir = raw_dir / f"viagens_{date_str}_extracted"
    extract_dir.mkdir(parents=True, exist_ok=True)
    extracted = extract_zip(zip_path, extract_dir)
    return [f for f in extracted if f.suffix.lower() == ".csv"]


def _process_csvs(csvs: list[Path], output_path: Path) -> bool:
    """Read raw CSVs, remap columns, concatenate, and write output."""
    frames: list[pd.DataFrame] = []

    for csv_path in csvs:
        try:
            df = pd.read_csv(
                csv_path,
                sep=";",
                encoding="latin-1",
                dtype=str,
                keep_default_na=False,
            )
        except Exception as e:
            logger.warning("Failed to read %s: %s", csv_path.name, e)
            continue

        logger.info("Viagens: %d rows from %s, columns: %s", len(df), csv_path.name, list(df.columns)[:5])
        df = _remap_columns(df)
        frames.append(df)

    if not frames:
        logger.warning("No valid CSV data found")
        return False

    combined = pd.concat(frames, ignore_index=True)
    combined.to_csv(output_path, index=False, sep=";", encoding="latin-1")
    logger.info("Wrote %d rows to %s", len(combined), output_path)
    return True


@click.command()
@click.option("--start-year", type=int, default=None, help="Start year (default: current year)")
@click.option("--end-year", type=int, default=None, help="End year (default: current year)")
@click.option("--output-dir", default="./data/viagens", help="Output directory")
@click.option("--skip-existing/--no-skip-existing", default=True, help="Skip existing ZIPs")
@click.option("--timeout", type=int, default=600, help="Download timeout in seconds")
def main(
    start_year: int | None,
    end_year: int | None,
    output_dir: str,
    skip_existing: bool,
    timeout: int,
) -> None:
    """Download Government Travel (Viagens a Servico) data.

    By default, downloads the last 12 months. Use --start-year and --end-year
    to specify a custom range.
    """
    now = datetime.now()

    if start_year is None and end_year is None:
        # Default: last 12 months
        months_to_download: list[tuple[int, int]] = []
        for i in range(12):
            month = now.month - i
            year = now.year
            if month <= 0:
                month += 12
                year -= 1
            months_to_download.append((year, month))
        months_to_download.reverse()
    else:
        sy = start_year or now.year
        ey = end_year or now.year
        months_to_download = [
            (y, m) for y in range(sy, ey + 1) for m in range(1, 13)
        ]

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    raw_dir = out / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=== Viagens download: %d months ===", len(months_to_download))

    all_csvs: list[Path] = []
    for year, month in months_to_download:
        csvs = _download_month(year, month, raw_dir, skip_existing=skip_existing, timeout=timeout)
        all_csvs.extend(csvs)

    if not all_csvs:
        logger.warning("No CSVs downloaded")
        sys.exit(1)

    # Validate first CSV
    if all_csvs:
        validate_csv(all_csvs[0], encoding="latin-1", sep=";")

    output_path = out / "viagens.csv"
    if not _process_csvs(all_csvs, output_path):
        sys.exit(1)

    logger.info("=== Done: %d CSV files processed ===", len(all_csvs))


if __name__ == "__main__":
    main()
