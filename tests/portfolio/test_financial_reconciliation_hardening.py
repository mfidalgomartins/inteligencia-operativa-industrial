from __future__ import annotations

import numpy as np
import pytest

from tests._helpers import read_csv


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_portfolio_summary_reconciles_new_financial_fields() -> None:
    invest = read_csv("investment_prioritization_final.csv")
    summary = read_csv("portfolio_summary.csv").iloc[0]
    selected = invest[invest["selected_portfolio_flag"] == 1].copy()

    assert np.isclose(float(summary["gross_technical_value_portfolio"]), float(selected["gross_technical_value"].sum()))
    assert np.isclose(float(summary["net_captured_value_portfolio"]), float(selected["net_captured_value"].sum()))
    assert np.isclose(float(summary["discounted_value_portfolio"]), float(selected["discounted_value"].sum()))
    assert np.isclose(float(summary["downside_adjusted_value_portfolio"]), float(selected["downside_adjusted_value"].sum()))


@pytest.mark.high
@pytest.mark.full
def test_portfolio_financial_readiness_is_explicitly_non_committee_final() -> None:
    summary = read_csv("portfolio_summary.csv").iloc[0]
    assert str(summary["financial_readiness_label"]) == "screening/pre-feasibility"
