"""ETL pipeline for PNCP (Portal Nacional de Contratações Públicas) bids.

Ingests procurement publications from all government levels via the PNCP REST API.
Creates Bid nodes linked to Company (agency) nodes via LICITOU relationships.
Distinct from ComprasNet pipeline — this covers the publication/bid stage
(licitações, pregões, dispensas), not the contract execution stage.

Data source: https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from icarus_etl.base import Pipeline
from icarus_etl.loader import Neo4jBatchLoader
from icarus_etl.transforms import (
    cap_contract_value,
    deduplicate_rows,
    format_cnpj,
    normalize_name,
    strip_document,
)

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

# PNCP modalidade IDs to human-readable labels
_MODALIDADE_MAP: dict[int, str] = {
    1: "leilao_eletronico",
    3: "concurso",
    4: "dialogo_competitivo",
    5: "concorrencia",
    6: "pregao_eletronico",
    7: "cotacao_eletronica",
    8: "dispensa",
    9: "inexigibilidade",
    10: "manifestacao_interesse",
    11: "pre_qualificacao",
    12: "credenciamento",
    13: "ata_pre_existente",
}

# PNCP esfera IDs to labels
_ESFERA_MAP: dict[str, str] = {
    "F": "federal",
    "E": "estadual",
    "M": "municipal",
    "D": "distrital",
}


class PncpPipeline(Pipeline):
    """ETL pipeline for PNCP procurement bid publications."""

    name = "pncp"
    source_id = "pncp_bids"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size)
        self._raw_records: list[dict[str, Any]] = []
        self.bids: list[dict[str, Any]] = []

    def extract(self) -> None:
        """Load pre-downloaded PNCP JSON files from data/pncp/."""
        src_dir = Path(self.data_dir) / "pncp"
        json_files = sorted(src_dir.glob("pncp_*.json"))
        if not json_files:
            logger.warning("No PNCP JSON files found in %s", src_dir)
            return

        all_records: list[dict[str, Any]] = []
        for f in json_files:
            raw = f.read_text(encoding="utf-8")
            payload = json.loads(raw, strict=False)

            # Handle both wrapped (API response) and flat (list) formats
            if isinstance(payload, dict) and "data" in payload:
                records = payload["data"]
            elif isinstance(payload, list):
                records = payload
            else:
                logger.warning("Unexpected format in %s, skipping", f.name)
                continue

            all_records.extend(records)
            logger.info("  Loaded %d records from %s", len(records), f.name)

        logger.info("Total raw records: %d", len(all_records))
        self._raw_records = all_records

    def transform(self) -> None:
        """Normalize fields, format CNPJs, deduplicate by bid_id."""
        if not self._raw_records:
            return

        bids: list[dict[str, Any]] = []
        skipped_no_cnpj = 0
        skipped_zero_value = 0

        for rec in self._raw_records:
            # Extract agency CNPJ
            org = rec.get("orgaoEntidade") or {}
            cnpj_raw = str(org.get("cnpj", "")).strip()
            cnpj_digits = strip_document(cnpj_raw)

            if len(cnpj_digits) != 14:
                skipped_no_cnpj += 1
                continue

            agency_cnpj = format_cnpj(cnpj_raw)

            # Extract value (prefer homologado, fallback to estimado)
            valor = rec.get("valorTotalHomologado") or rec.get("valorTotalEstimado") or 0
            if not valor or float(valor) <= 0:
                skipped_zero_value += 1
                continue

            # Build stable bid ID from PNCP control number
            numero_controle = str(rec.get("numeroControlePNCP", "")).strip()
            if not numero_controle:
                # Fallback: compose from org CNPJ + sequence + year
                seq = rec.get("sequencialCompra", "")
                ano = rec.get("anoCompra", "")
                numero_controle = f"{cnpj_digits}-1-{seq:06d}/{ano}" if seq and ano else ""

            if not numero_controle:
                continue

            # Agency info
            agency_name = normalize_name(str(org.get("razaoSocial", "")))

            # Location from unidadeOrgao
            unidade = rec.get("unidadeOrgao") or {}
            municipality = str(unidade.get("municipioNome", "")).strip()
            state = str(unidade.get("ufSigla", "")).strip()

            # Modality
            modalidade_id = rec.get("modalidadeId")
            modality = _MODALIDADE_MAP.get(modalidade_id, "") if modalidade_id else ""
            modalidade_nome = str(rec.get("modalidadeNome", "")).strip()

            # Government sphere
            esfera_id = str(org.get("esferaId", "")).strip()
            esfera = _ESFERA_MAP.get(esfera_id, esfera_id)

            # Dates
            data_pub = str(rec.get("dataPublicacaoPncp", "")).strip()
            date = data_pub[:10] if data_pub else ""

            # Status
            status = str(rec.get("situacaoCompraNome", "")).strip()

            # Description
            objeto = normalize_name(str(rec.get("objetoCompra", "")))

            bids.append({
                "bid_id": numero_controle,
                "description": objeto,
                "modality": modality or modalidade_nome,
                "amount": cap_contract_value(float(valor)),
                "date": date,
                "status": status,
                "agency_name": agency_name,
                "agency_cnpj": agency_cnpj,
                "municipality": municipality,
                "state": state,
                "esfera": esfera,
                "processo": str(rec.get("processo", "")).strip(),
                "srp": bool(rec.get("srp", False)),
                "source": "pncp",
            })

        self.bids = deduplicate_rows(bids, ["bid_id"])

        logger.info(
            "Transformed: %d bids (skipped %d no-CNPJ, %d zero-value)",
            len(self.bids),
            skipped_no_cnpj,
            skipped_zero_value,
        )

        if self.limit:
            self.bids = self.bids[: self.limit]

    def load(self) -> None:
        """Load Bid nodes and LICITOU relationships into Neo4j."""
        if not self.bids:
            logger.warning("No bids to load")
            return

        loader = Neo4jBatchLoader(self.driver)

        # Load Bid nodes (MERGE on bid_id)
        bid_nodes = [
            {
                "bid_id": b["bid_id"],
                "description": b["description"],
                "modality": b["modality"],
                "amount": b["amount"],
                "date": b["date"],
                "status": b["status"],
                "agency_name": b["agency_name"],
                "municipality": b["municipality"],
                "state": b["state"],
                "esfera": b["esfera"],
                "processo": b["processo"],
                "srp": b["srp"],
                "source": b["source"],
            }
            for b in self.bids
        ]
        count = loader.load_nodes("Bid", bid_nodes, key_field="bid_id")
        logger.info("Loaded %d Bid nodes", count)

        # Ensure Company nodes exist for agencies
        agencies = deduplicate_rows(
            [
                {"cnpj": b["agency_cnpj"], "razao_social": b["agency_name"]}
                for b in self.bids
            ],
            ["cnpj"],
        )
        count = loader.load_nodes("Company", agencies, key_field="cnpj")
        logger.info("Merged %d Company (agency) nodes", count)

        # LICITOU: Company (agency) -> Bid
        rels = [
            {"source_key": b["agency_cnpj"], "target_key": b["bid_id"]}
            for b in self.bids
        ]
        count = loader.load_relationships(
            rel_type="LICITOU",
            rows=rels,
            source_label="Company",
            source_key="cnpj",
            target_label="Bid",
            target_key="bid_id",
        )
        logger.info("Created %d LICITOU relationships", count)
