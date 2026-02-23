from unittest.mock import AsyncMock, MagicMock

import pytest

from icarus.services.score_service import _percentile, compute_exposure


class TestPercentile:
    def test_empty_list_returns_zero(self) -> None:
        assert _percentile([], 10.0) == 0.0

    def test_single_element_below(self) -> None:
        assert _percentile([5.0], 3.0) == 0.0

    def test_single_element_above(self) -> None:
        assert _percentile([5.0], 10.0) == 100.0

    def test_single_element_equal(self) -> None:
        assert _percentile([5.0], 5.0) == 100.0

    def test_all_same_values(self) -> None:
        result = _percentile([5.0, 5.0, 5.0, 5.0], 5.0)
        assert result == 100.0

    def test_median_value(self) -> None:
        result = _percentile([1.0, 2.0, 3.0, 4.0], 2.5)
        assert result == 50.0

    def test_top_percentile(self) -> None:
        result = _percentile([1.0, 2.0, 3.0, 4.0, 5.0], 5.0)
        assert result == 100.0

    def test_bottom_percentile(self) -> None:
        result = _percentile([1.0, 2.0, 3.0, 4.0, 5.0], 0.5)
        assert result == 0.0

    def test_25th_percentile(self) -> None:
        result = _percentile([1.0, 2.0, 3.0, 4.0], 1.0)
        assert result == 25.0


@pytest.mark.anyio
async def test_compute_exposure_returns_response() -> None:
    session = AsyncMock()

    score_record = MagicMock()
    score_record.__getitem__ = lambda self, key: {
        "entity_id": "4:abc:1",
        "entity_labels": ["Company"],
        "connection_count": 10,
        "source_count": 3,
        "financial_volume": 50000.0,
        "cnae_principal": "4711-3/02",
        "role": None,
    }[key]

    peer_record = MagicMock()
    peer_record.__getitem__ = lambda self, key: {
        "peer_count": 50,
        "connection_counts": [1, 2, 3, 5, 8, 10, 15, 20, 25, 30],
        "financial_volumes": [
            1000.0, 5000.0, 10000.0, 20000.0, 50000.0,
            80000.0, 100000.0, 200000.0, 500000.0, 1000000.0,
        ],
    }[key]

    call_count = 0

    async def mock_run(cypher: str, params: dict, timeout: float = 15):  # type: ignore[no-untyped-def]
        nonlocal call_count
        call_count += 1

        result = AsyncMock()
        if call_count == 1:
            result.single = AsyncMock(return_value=score_record)
        else:
            result.single = AsyncMock(return_value=peer_record)
        return result

    session.run = mock_run

    response = await compute_exposure(session, "4:abc:1")

    assert response.entity_id == "4:abc:1"
    assert 0.0 <= response.exposure_index <= 100.0
    assert len(response.factors) == 5
    assert response.peer_group == "CNAE 4711-3/02"
    assert response.peer_count == 50
    assert len(response.sources) > 0


@pytest.mark.anyio
async def test_compute_exposure_entity_not_found() -> None:
    session = AsyncMock()

    async def mock_run(cypher: str, params: dict, timeout: float = 15):  # type: ignore[no-untyped-def]
        result = AsyncMock()
        result.single = AsyncMock(return_value=None)
        return result

    session.run = mock_run

    from fastapi import HTTPException

    with pytest.raises(HTTPException) as exc_info:
        await compute_exposure(session, "nonexistent")
    assert exc_info.value.status_code == 404


@pytest.mark.anyio
async def test_compute_exposure_small_peer_group_downweights() -> None:
    session = AsyncMock()

    score_record = MagicMock()
    score_record.__getitem__ = lambda self, key: {
        "entity_id": "4:abc:2",
        "entity_labels": ["Person"],
        "connection_count": 5,
        "source_count": 2,
        "financial_volume": 0.0,
        "cnae_principal": None,
        "role": "deputado",
    }[key]

    peer_record = MagicMock()
    peer_record.__getitem__ = lambda self, key: {
        "peer_count": 3,
        "connection_counts": [1, 2, 3],
        "financial_volumes": [0.0, 0.0, 0.0],
    }[key]

    call_count = 0

    async def mock_run(cypher: str, params: dict, timeout: float = 15):  # type: ignore[no-untyped-def]
        nonlocal call_count
        call_count += 1
        result = AsyncMock()
        if call_count == 1:
            result.single = AsyncMock(return_value=score_record)
        else:
            result.single = AsyncMock(return_value=peer_record)
        return result

    session.run = mock_run

    response = await compute_exposure(session, "4:abc:2")

    # With peer_count < 10, connections and financial weights should be halved
    conn_factor = next(f for f in response.factors if f.name == "connections")
    fin_factor = next(f for f in response.factors if f.name == "financial")
    assert conn_factor.weight == 0.125  # 0.25 * 0.5
    assert fin_factor.weight == 0.10  # 0.20 * 0.5


@pytest.mark.anyio
async def test_compute_exposure_source_attribution() -> None:
    session = AsyncMock()

    score_record = MagicMock()
    score_record.__getitem__ = lambda self, key: {
        "entity_id": "4:abc:3",
        "entity_labels": ["Company"],
        "connection_count": 0,
        "source_count": 1,
        "financial_volume": 0.0,
        "cnae_principal": None,
        "role": None,
    }[key]

    peer_record = MagicMock()
    peer_record.__getitem__ = lambda self, key: {
        "peer_count": 0,
        "connection_counts": [],
        "financial_volumes": [],
    }[key]

    call_count = 0

    async def mock_run(cypher: str, params: dict, timeout: float = 15):  # type: ignore[no-untyped-def]
        nonlocal call_count
        call_count += 1
        result = AsyncMock()
        if call_count == 1:
            result.single = AsyncMock(return_value=score_record)
        else:
            result.single = AsyncMock(return_value=peer_record)
        return result

    session.run = mock_run

    response = await compute_exposure(session, "4:abc:3")
    assert response.sources[0].database == "neo4j_analysis"

    # Each factor should have source attribution
    for factor in response.factors:
        assert len(factor.sources) > 0
