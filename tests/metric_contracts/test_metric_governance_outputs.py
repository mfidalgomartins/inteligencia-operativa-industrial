from pathlib import Path

import pandas as pd
import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROCESSED = PROJECT_ROOT / "data" / "processed"


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_metric_registry_and_contract_checks_exist() -> None:
    assert (PROCESSED / "semantic_metric_registry.csv").exists()
    assert (PROCESSED / "metric_contract_validation.csv").exists()
    assert (PROCESSED / "cross_output_consistency.csv").exists()


@pytest.mark.blocker
@pytest.mark.quick
@pytest.mark.full
def test_metric_contracts_without_errors() -> None:
    contracts = pd.read_csv(PROCESSED / "metric_contract_validation.csv")
    assert not contracts.empty
    assert (contracts["status"] != "ERROR").all()


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_cross_output_checks_without_errors() -> None:
    cross = pd.read_csv(PROCESSED / "cross_output_consistency.csv")
    assert not cross.empty
    assert (cross["status"] != "ERROR").all()
    assert (cross["status"] != "WARN").all()
