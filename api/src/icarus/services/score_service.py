import bisect

from neo4j import AsyncSession

from icarus.models.entity import ExposureFactor, ExposureResponse, SourceAttribution
from icarus.services.neo4j_service import execute_query_single

# Weights for each factor in the exposure index formula
FACTOR_WEIGHTS = {
    "connections": 0.25,
    "sources": 0.25,
    "financial": 0.20,
    "patterns": 0.20,
    "baseline": 0.10,
}


def _percentile(values: list[float], target: float) -> float:
    """Compute the percentile rank of target within a sorted list of values."""
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    pos = bisect.bisect_right(sorted_vals, target)
    return (pos / len(sorted_vals)) * 100.0


async def compute_exposure(
    session: AsyncSession,
    entity_id: str,
) -> ExposureResponse:
    """Compute the exposure index for a given entity."""
    record = await execute_query_single(
        session, "entity_score", {"entity_id": entity_id}
    )
    if record is None:
        from fastapi import HTTPException

        raise HTTPException(status_code=404, detail="Entity not found")

    connection_count = int(record["connection_count"])
    source_count = int(record["source_count"])
    financial_volume = float(record["financial_volume"])
    entity_labels: list[str] = record["entity_labels"]
    cnae = record["cnae_principal"]

    entity_type = entity_labels[0] if entity_labels else "Unknown"
    is_company = "Company" in entity_labels

    # Determine peer group
    if is_company and cnae:
        peer_group = f"CNAE {cnae}"
        peer_label = "Company"
        peer_cnae: str | None = cnae
    elif is_company:
        peer_group = "Company (all)"
        peer_label = "Company"
        peer_cnae = None
    else:
        peer_group = f"Person ({entity_type})"
        peer_label = "Person"
        peer_cnae = None

    # Get peer distribution for percentile computation
    peer_record = await execute_query_single(
        session,
        "entity_score_peers",
        {
            "entity_id": entity_id,
            "peer_label": peer_label,
            "cnae": peer_cnae,
        },
        timeout=30,
    )

    peer_count = 0
    conn_percentile = 50.0
    fin_percentile = 50.0

    if peer_record:
        peer_count = int(peer_record["peer_count"])
        if peer_count > 0:
            conn_values = [float(v) for v in peer_record["connection_counts"]]
            fin_values = [float(v) for v in peer_record["financial_volumes"]]
            conn_percentile = _percentile(conn_values, float(connection_count))
            fin_percentile = _percentile(fin_values, financial_volume)

    # Source percentile: scale 0-4 sources to 0-100
    source_percentile = min(source_count * 25.0, 100.0)

    # Pattern and baseline factors — defaults until pattern count is available
    pattern_percentile = 0.0
    baseline_percentile = 0.0

    # Build factors
    factors: list[ExposureFactor] = [
        ExposureFactor(
            name="connections",
            value=float(connection_count),
            percentile=conn_percentile,
            weight=FACTOR_WEIGHTS["connections"],
            sources=["neo4j_graph"],
        ),
        ExposureFactor(
            name="sources",
            value=float(source_count),
            percentile=source_percentile,
            weight=FACTOR_WEIGHTS["sources"],
            sources=["neo4j_graph"],
        ),
        ExposureFactor(
            name="financial",
            value=financial_volume,
            percentile=fin_percentile,
            weight=FACTOR_WEIGHTS["financial"],
            sources=["transparencia", "tse"],
        ),
        ExposureFactor(
            name="patterns",
            value=0.0,
            percentile=pattern_percentile,
            weight=FACTOR_WEIGHTS["patterns"],
            sources=["neo4j_analysis"],
        ),
        ExposureFactor(
            name="baseline",
            value=0.0,
            percentile=baseline_percentile,
            weight=FACTOR_WEIGHTS["baseline"],
            sources=["neo4j_analysis"],
        ),
    ]

    # If peer count < 10, down-weight peer-dependent factors
    if peer_count < 10:
        for factor in factors:
            if factor.name in ("connections", "financial"):
                factor.weight *= 0.5

    # Compute weighted exposure index
    total_weight = sum(f.weight for f in factors)
    if total_weight > 0:
        exposure_index = sum(f.percentile * f.weight for f in factors) / total_weight
    else:
        exposure_index = 0.0

    # Clamp to 0-100
    exposure_index = max(0.0, min(100.0, round(exposure_index, 2)))

    return ExposureResponse(
        entity_id=entity_id,
        exposure_index=exposure_index,
        factors=factors,
        peer_group=peer_group,
        peer_count=peer_count,
        sources=[SourceAttribution(database="neo4j_analysis")],
    )
