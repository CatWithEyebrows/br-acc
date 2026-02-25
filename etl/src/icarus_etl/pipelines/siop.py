from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from icarus_etl.base import Pipeline

if TYPE_CHECKING:
    from neo4j import Driver
from icarus_etl.loader import Neo4jBatchLoader
from icarus_etl.transforms import (
    deduplicate_rows,
    format_cpf,
    normalize_name,
    strip_document,
)


def _parse_brl(value: str | None) -> float:
    """Parse Brazilian monetary string to float (e.g. '1.234.567,89')."""
    if not value:
        return 0.0
    cleaned = str(value).strip()
    cleaned = re.sub(r"[R$\s]", "", cleaned)
    if not cleaned:
        return 0.0
    if "," in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _classify_amendment_type(raw_type: str) -> str:
    """Normalize amendment type to a canonical category."""
    normalized = raw_type.strip().lower()
    if "individual" in normalized:
        return "individual"
    if "bancada" in normalized:
        return "bancada"
    if "comiss" in normalized:
        return "comissao"
    if "relator" in normalized:
        return "relator"
    return raw_type.strip()


class SiopPipeline(Pipeline):
    """ETL pipeline for SIOP parliamentary amendments detail.

    Source: Portal da Transparencia emendas-parlamentares yearly CSVs.
    Enriches existing Amendment nodes (from TransfereGov) or creates new ones
    with budget execution detail (authorized, committed, paid amounts),
    amendment type classification, program/action codes, and author linkage.
    """

    name = "siop"
    source_id = "siop"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size)
        self._raw: pd.DataFrame = pd.DataFrame()
        self.amendments: list[dict[str, Any]] = []
        self.authors: list[dict[str, Any]] = []
        self.author_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        siop_dir = Path(self.data_dir) / "siop"
        csv_files = sorted(siop_dir.glob("*.csv"))
        if not csv_files:
            return

        frames: list[pd.DataFrame] = []
        for csv_path in csv_files:
            df = pd.read_csv(
                csv_path,
                dtype=str,
                encoding="latin-1",
                sep=";",
                keep_default_na=False,
            )
            frames.append(df)

        self._raw = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def transform(self) -> None:
        if self._raw.empty:
            return

        amendments: list[dict[str, Any]] = []
        authors: list[dict[str, Any]] = []
        author_rels: list[dict[str, Any]] = []

        # Group by amendment code to aggregate values across sub-functions
        col_code = "CÓDIGO EMENDA"
        if col_code not in self._raw.columns:
            return

        grouped = self._raw.groupby(col_code)

        for code, group in grouped:
            code_str = str(code).strip()
            if not code_str:
                continue

            first = group.iloc[0]

            year = str(first.get("ANO", "")).strip()
            amendment_number = str(first.get("NÚMERO EMENDA", "")).strip()
            raw_type = str(first.get("TIPO EMENDA", "")).strip()
            amendment_type = _classify_amendment_type(raw_type)
            author_name = normalize_name(str(first.get("AUTOR EMENDA", "")))
            author_doc = str(first.get("CPF/CNPJ AUTOR", "")).strip()
            locality = str(first.get("LOCALIDADE", "")).strip()

            # Program/action from first row (consistent within an amendment)
            program = normalize_name(str(first.get("NOME PROGRAMA", "")))
            program_code = str(first.get("CÓDIGO PROGRAMA", "")).strip()
            action = normalize_name(str(first.get("NOME AÇÃO", "")))
            action_code = str(first.get("CÓDIGO AÇÃO", "")).strip()
            function_name = normalize_name(str(first.get("NOME FUNÇÃO", "")))

            # Sum monetary values across all rows for this amendment
            amount_committed = sum(
                _parse_brl(str(r.get("VALOR EMPENHADO", "")))
                for _, r in group.iterrows()
            )
            amount_settled = sum(
                _parse_brl(str(r.get("VALOR LIQUIDADO", "")))
                for _, r in group.iterrows()
            )
            amount_paid = sum(
                _parse_brl(str(r.get("VALOR PAGO", "")))
                for _, r in group.iterrows()
            )

            # Build unique amendment_id from code
            amendment_id = f"siop_{code_str}"

            amendments.append({
                "amendment_id": amendment_id,
                "amendment_code": code_str,
                "amendment_number": amendment_number,
                "year": year,
                "amendment_type": amendment_type,
                "author_name": author_name,
                "locality": locality,
                "function": function_name,
                "program": program,
                "program_code": program_code,
                "action": action,
                "action_code": action_code,
                "amount_committed": amount_committed,
                "amount_settled": amount_settled,
                "amount_paid": amount_paid,
                "source": "siop",
            })

            # Author linkage — only if CPF is present (11 digits)
            author_digits = strip_document(author_doc)
            if len(author_digits) == 11 and author_name:
                cpf_formatted = format_cpf(author_doc)
                authors.append({
                    "cpf": cpf_formatted,
                    "name": author_name,
                })
                author_rels.append({
                    "source_key": cpf_formatted,
                    "target_key": amendment_id,
                })

        self.amendments = deduplicate_rows(amendments, ["amendment_id"])
        self.authors = deduplicate_rows(authors, ["cpf"])
        self.author_rels = author_rels

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        # 1. Amendment nodes
        if self.amendments:
            loader.load_nodes("Amendment", self.amendments, key_field="amendment_id")

        # 2. Person nodes for authors with CPF
        if self.authors:
            loader.load_nodes("Person", self.authors, key_field="cpf")

        # 3. Person -[:AUTOR_EMENDA]-> Amendment
        if self.author_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (p:Person {cpf: row.source_key}) "
                "MATCH (a:Amendment {amendment_id: row.target_key}) "
                "MERGE (p)-[:AUTOR_EMENDA]->(a)"
            )
            loader.run_query_with_retry(query, self.author_rels)
