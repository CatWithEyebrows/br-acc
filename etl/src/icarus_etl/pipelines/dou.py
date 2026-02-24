from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from icarus_etl.base import Pipeline
from icarus_etl.loader import Neo4jBatchLoader
from icarus_etl.transforms import format_cnpj, strip_document

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


class DouPipeline(Pipeline):
    """ETL pipeline for DOU (Diario Oficial da Uniao) gazette data.

    Supports two input formats:
    1. Querido Diário JSON (from their API or data dumps)
    2. CSV with columns: date, territory_id, territory_name, edition,
       is_extra_edition, url, excerpt (optional)

    The Querido Diário API (queridodiario.ok.org.br) is behind Cloudflare
    bot protection — data must be obtained via browser or their data dumps.
    See scripts/download_dou.sh for instructions.
    """

    name = "dou"
    source_id = "querido_diario"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size)
        self.gazettes: list[dict[str, Any]] = []
        self.gazette_entity_links: list[dict[str, Any]] = []

    def extract(self) -> None:
        dou_dir = Path(self.data_dir) / "dou"
        if not dou_dir.exists():
            msg = f"DOU data directory not found at {dou_dir}"
            raise FileNotFoundError(msg)

        # Try JSON files first (Querido Diário format)
        json_files = sorted(dou_dir.glob("*.json"))
        csv_files = sorted(dou_dir.glob("*.csv"))

        self._raw_rows: list[dict[str, str]] = []

        if json_files:
            self._extract_json(json_files)
        elif csv_files:
            self._extract_csv(csv_files)
        else:
            logger.warning("[dou] No JSON or CSV files found in %s", dou_dir)
            return

        logger.info("[dou] Extracted %d gazette records", len(self._raw_rows))

    def _extract_json(self, files: list[Path]) -> None:
        for f in files:
            with open(f, encoding="utf-8") as fh:
                data = json.load(fh)

            # Handle both list and QD API response format
            if isinstance(data, dict) and "gazettes" in data:
                items = data["gazettes"]
            elif isinstance(data, list):
                items = data
            else:
                logger.warning("[dou] Unexpected JSON format in %s", f.name)
                continue

            for item in items:
                self._raw_rows.append({
                    "date": str(item.get("date", "")),
                    "territory_id": str(item.get("territory_id", "")),
                    "territory_name": str(item.get("territory_name", "")),
                    "edition": str(item.get("edition", "")),
                    "is_extra_edition": str(item.get("is_extra_edition", False)),
                    "url": str(item.get("url", item.get("txt_url", ""))),
                    "excerpt": str(item.get("excerpts", [""])[0])
                    if isinstance(item.get("excerpts"), list)
                    else str(item.get("excerpt", "")),
                })

                if self.limit and len(self._raw_rows) >= self.limit:
                    return

    def _extract_csv(self, files: list[Path]) -> None:
        for f in files:
            with open(f, encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    self._raw_rows.append(row)
                    if self.limit and len(self._raw_rows) >= self.limit:
                        return

    def transform(self) -> None:
        gazettes: list[dict[str, Any]] = []
        links: list[dict[str, Any]] = []

        for row in self._raw_rows:
            date = row.get("date", "").strip()
            territory_id = row.get("territory_id", "").strip()
            territory_name = row.get("territory_name", "").strip()

            if not date or not territory_id:
                continue

            edition = row.get("edition", "").strip()
            is_extra = row.get("is_extra_edition", "").strip().lower() in (
                "true",
                "1",
                "sim",
            )
            url = row.get("url", "").strip()

            gazette_id = f"dou_{territory_id}_{date}_{edition or '0'}"

            gazettes.append({
                "gazette_id": gazette_id,
                "date": date,
                "territory_id": territory_id,
                "territory_name": territory_name,
                "edition": edition,
                "is_extra_edition": is_extra,
                "url": url,
                "source": "querido_diario",
            })

            # Extract CNPJ mentions from excerpt if available
            excerpt = row.get("excerpt", "").strip()
            if excerpt:
                cnpjs = self._extract_cnpjs(excerpt)
                for cnpj in cnpjs:
                    links.append({
                        "source_key": cnpj,
                        "target_key": gazette_id,
                    })

        self.gazettes = gazettes
        self.gazette_entity_links = links
        logger.info(
            "[dou] Transformed %d gazettes, %d entity mentions",
            len(self.gazettes),
            len(self.gazette_entity_links),
        )

    def _extract_cnpjs(self, text: str) -> list[str]:
        """Extract and format valid CNPJ numbers from text."""
        import re

        # Match formatted (XX.XXX.XXX/XXXX-XX) or raw 14-digit CNPJs
        pattern = r"\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2}"
        matches = re.findall(pattern, text)

        cnpjs = []
        for m in matches:
            digits = strip_document(m)
            if len(digits) == 14:
                cnpjs.append(format_cnpj(m))
        return cnpjs

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.gazettes:
            loader.load_nodes("Gazette", self.gazettes, key_field="gazette_id")
            logger.info("[dou] Loaded %d Gazette nodes", len(self.gazettes))

        if self.gazette_entity_links:
            query = (
                "UNWIND $rows AS row "
                "MATCH (g:Gazette {gazette_id: row.target_key}) "
                "MATCH (c:Company {cnpj: row.source_key}) "
                "MERGE (c)-[:MENCIONADA_EM]->(g)"
            )
            loader.run_query(query, self.gazette_entity_links)
            logger.info(
                "[dou] Created %d MENCIONADA_EM relationships",
                len(self.gazette_entity_links),
            )
