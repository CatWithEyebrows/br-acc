from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query
from neo4j import AsyncSession
from starlette.requests import Request

from icarus.dependencies import get_session
from icarus.middleware.rate_limit import limiter
from icarus.models.entity import SourceAttribution
from icarus.models.search import SearchResponse, SearchResult
from icarus.services.neo4j_service import execute_query, sanitize_props

router = APIRouter(prefix="/api/v1", tags=["search"])


def _extract_name(node: Any, labels: list[str]) -> str:
    props = dict(node)
    entity_type = labels[0].lower() if labels else ""
    if entity_type == "company":
        return str(props.get("razao_social", props.get("name", props.get("nome_fantasia", ""))))
    return str(props.get("name", ""))


@router.get("/search", response_model=SearchResponse)
@limiter.limit("30/minute")
async def search_entities(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_session)],
    q: Annotated[str, Query(min_length=2, max_length=200)],
    entity_type: Annotated[str | None, Query(alias="type")] = None,
    page: Annotated[int, Query(ge=1)] = 1,
    size: Annotated[int, Query(ge=1, le=100)] = 20,
) -> SearchResponse:
    skip = (page - 1) * size
    type_filter = entity_type.lower() if entity_type else None

    records = await execute_query(
        session,
        "search",
        {
            "query": q,
            "entity_type": type_filter,
            "skip": skip,
            "limit": size,
        },
    )

    results: list[SearchResult] = []
    for record in records:
        node = record["node"]
        props = dict(node)
        labels = record["node_labels"]
        source_val = props.pop("source", None)
        sources: list[SourceAttribution] = []
        if isinstance(source_val, str):
            sources = [SourceAttribution(database=source_val)]
        elif isinstance(source_val, list):
            sources = [SourceAttribution(database=s) for s in source_val]

        doc_id = record["document_id"]
        # Only expose cpf/cnpj as document, not internal element IDs
        document = str(doc_id) if doc_id and not str(doc_id).startswith("4:") else None

        results.append(SearchResult(
            id=record["node_id"],
            type=labels[0].lower() if labels else "unknown",
            name=_extract_name(node, labels),
            score=record["score"],
            document=document,
            properties=sanitize_props(props),
            sources=sources,
        ))

    return SearchResponse(
        results=results,
        total=len(results),
        page=page,
        size=size,
    )
