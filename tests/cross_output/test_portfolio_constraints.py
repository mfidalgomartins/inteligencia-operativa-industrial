from pathlib import Path

import pandas as pd
import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROCESSED = PROJECT_ROOT / "data" / "processed"


@pytest.mark.blocker
@pytest.mark.quick
@pytest.mark.full
def test_portfolio_budget_constraints_respected() -> None:
    summary = pd.read_csv(PROCESSED / "portfolio_summary.csv").iloc[0]
    assert float(summary["capex_selected"]) <= float(summary["capex_budget"]) + 1e-6
    assert float(summary["opex_selected"]) <= float(summary["opex_budget"]) + 1e-6


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_prioritization_includes_portfolio_fields() -> None:
    invest = pd.read_csv(PROCESSED / "investment_prioritization_final.csv")
    expected = {
        "selected_portfolio_flag",
        "portfolio_wave",
        "initiative_class",
        "npv_risk_adjusted",
        "uncertainty_index",
    }
    assert expected.issubset(set(invest.columns))
