import time
from typing import Annotated, Any

from fastapi import APIRouter, Depends
from neo4j import AsyncSession

from icarus.dependencies import get_session
from icarus.services.neo4j_service import execute_query_single

router = APIRouter(prefix="/api/v1/meta", tags=["meta"])

_stats_cache: dict[str, Any] | None = None
_stats_cache_time: float = 0.0


@router.get("/health")
async def neo4j_health(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> dict[str, str]:
    record = await execute_query_single(session, "health_check", {})
    if record and record["ok"] == 1:
        return {"neo4j": "connected"}
    return {"neo4j": "error"}


@router.get("/stats")
async def database_stats(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> dict[str, Any]:
    global _stats_cache, _stats_cache_time  # noqa: PLW0603

    if _stats_cache is not None and (time.monotonic() - _stats_cache_time) < 300:
        return _stats_cache

    record = await execute_query_single(session, "meta_stats", {})
    result = {
        "total_nodes": record["total_nodes"] if record else 0,
        "total_relationships": record["total_relationships"] if record else 0,
        "person_count": record["person_count"] if record else 0,
        "company_count": record["company_count"] if record else 0,
        "data_sources": 4,
        "indexes": 23,
    }

    _stats_cache = result
    _stats_cache_time = time.monotonic()
    return result


@router.get("/sources")
async def list_sources() -> dict[str, list[dict[str, str]]]:
    return {
        "sources": [
            {"id": "cnpj", "name": "Receita Federal (CNPJ)", "frequency": "monthly"},
            {"id": "tse", "name": "Tribunal Superior Eleitoral", "frequency": "biennial"},
            {"id": "transparencia", "name": "Portal da Transparência", "frequency": "monthly"},
            {"id": "ceis", "name": "CEIS/CNEP/CEPIM/CEAF", "frequency": "monthly"},
        ]
    }
