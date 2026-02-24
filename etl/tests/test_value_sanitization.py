from __future__ import annotations

from icarus_etl.transforms.value_sanitization import (
    MAX_CONTRACT_VALUE,
    cap_contract_value,
)


def test_normal_values_pass_through() -> None:
    assert cap_contract_value(150_000.0) == 150_000.0
    assert cap_contract_value(1_500_000.0) == 1_500_000.0
    assert cap_contract_value(5_000_000_000.0) == 5_000_000_000.0


def test_threshold_boundary() -> None:
    """Exact threshold passes; threshold + 1 → None."""
    assert cap_contract_value(MAX_CONTRACT_VALUE) == MAX_CONTRACT_VALUE
    assert cap_contract_value(MAX_CONTRACT_VALUE + 1) is None


def test_above_threshold_returns_none() -> None:
    assert cap_contract_value(50_000_000_000.0) is None
    assert cap_contract_value(3_300_000_000_000.0) is None


def test_zero_and_negative_unchanged() -> None:
    assert cap_contract_value(0.0) == 0.0
    assert cap_contract_value(-100.0) == -100.0


def test_none_input_returns_none() -> None:
    assert cap_contract_value(None) is None
