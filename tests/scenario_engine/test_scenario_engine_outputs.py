from pathlib import Path

import pandas as pd
import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROCESSED = PROJECT_ROOT / "data" / "processed"


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_scenario_table_structure_and_ranges() -> None:
    scenario = pd.read_csv(PROCESSED / "scenario_table.csv")
    required_cols = {
        "iniciativa_id",
        "macro_scenario",
        "risk_adjusted_annual_savings",
        "npv_risk_adjusted",
        "payback_meses",
        "success_probability",
        "value_at_risk_95",
    }
    assert required_cols.issubset(set(scenario.columns))
    assert scenario["macro_scenario"].nunique() >= 3
    assert scenario["success_probability"].between(0.2, 1.0).all()
    assert scenario["payback_meses"].gt(0).all()


@pytest.mark.high
@pytest.mark.quick
@pytest.mark.full
def test_scenario_dependency_and_conflict_outputs_exist() -> None:
    assert (PROCESSED / "scenario_dependencies.csv").exists()
    assert (PROCESSED / "scenario_conflicts.csv").exists()
