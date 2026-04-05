from __future__ import annotations

import numpy as np
import pytest

from tests._helpers import read_csv


@pytest.mark.blocker
@pytest.mark.quick
@pytest.mark.full
def test_scenario_financial_invariants() -> None:
    scenario = read_csv("scenario_table.csv")

    assert scenario["gross_annual_savings"].ge(0).all()
    assert scenario["net_annual_savings"].ge(0).all()
    assert scenario["risk_adjusted_annual_savings"].ge(0).all()
    assert scenario["risk_adjusted_annual_savings"].le(scenario["net_annual_savings"] + 1e-8).all()
    assert scenario["value_at_risk_95"].le(scenario["risk_adjusted_annual_savings"] + 1e-8).all()
    assert scenario["downside_annual_savings_p20"].le(scenario["risk_adjusted_annual_savings"] + 1e-8).all()


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_scenario_shock_energy_sensitivity_regression() -> None:
    scenario = read_csv("scenario_table.csv")
    pivot = scenario.pivot_table(
        index="iniciativa_id",
        columns="macro_scenario",
        values="risk_adjusted_annual_savings",
        aggfunc="mean",
    )
    required = {"Base", "ShockEnergia"}
    assert required.issubset(set(pivot.columns))

    ratio = (pivot["ShockEnergia"] / pivot["Base"]).replace([np.inf, -np.inf], np.nan).dropna()
    assert not ratio.empty
    assert float(ratio.median()) > 1.05
    assert float((ratio > 1.0).mean()) >= 0.90


@pytest.mark.high
@pytest.mark.full
def test_dependency_penalty_applied_to_subset_of_scenarios() -> None:
    scenario = read_csv("scenario_table.csv")
    with_penalty = int((scenario["dependency_penalty_pct"] > 0).sum())
    assert with_penalty > 0
    assert with_penalty < len(scenario)


@pytest.mark.critical
@pytest.mark.full
def test_scenario_payback_npv_directional_consistency() -> None:
    scenario = read_csv("scenario_table.csv")
    top_npv = scenario.sort_values("npv_risk_adjusted", ascending=False).head(20)["payback_meses"].mean()
    low_npv = scenario.sort_values("npv_risk_adjusted", ascending=True).head(20)["payback_meses"].mean()
    assert float(top_npv) <= float(low_npv)
