from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from tests._helpers import read_csv


@pytest.mark.blocker
@pytest.mark.quick
@pytest.mark.full
def test_portfolio_summary_reconciles_with_selected_initiatives() -> None:
    invest = read_csv("investment_prioritization_final.csv")
    summary = read_csv("portfolio_summary.csv").iloc[0]
    selected = invest[invest["selected_portfolio_flag"] == 1].copy()

    assert int(summary["initiatives_selected"]) == len(selected)
    assert np.isclose(float(summary["capex_selected"]), float(selected["capex_estimado"].sum()))
    assert np.isclose(float(summary["npv_risk_adjusted_portfolio"]), float(selected["npv_risk_adjusted"].sum()))
    assert np.isclose(
        float(summary["annual_saving_risk_adjusted_portfolio"]),
        float(selected["annual_saving_proxy"].sum()),
    )


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_selected_initiatives_have_wave_and_valid_decision_rule() -> None:
    invest = read_csv("investment_prioritization_final.csv")
    selected = invest[invest["selected_portfolio_flag"] == 1]
    assert not selected.empty
    assert selected["portfolio_wave"].isin(["OLA_1", "OLA_2", "OLA_3"]).all()
    assert selected["decision_rule"].isin(["ejecutar ahora", "ejecutar en siguiente ola", "piloto"]).all()


@pytest.mark.high
@pytest.mark.full
def test_dependency_consistency_for_selected_initiatives() -> None:
    invest = read_csv("investment_prioritization_final.csv")
    deps = read_csv("scenario_dependencies.csv")
    if deps.empty:
        pytest.skip("No dependency rows generated in this run")

    child = deps.merge(
        invest[["iniciativa_id", "selected_portfolio_flag"]],
        on="iniciativa_id",
        how="left",
    ).rename(columns={"selected_portfolio_flag": "child_selected"})
    dep = invest[["iniciativa_id", "selected_portfolio_flag"]].rename(
        columns={
            "iniciativa_id": "depends_on_iniciativa_id",
            "selected_portfolio_flag": "dep_selected",
        }
    )
    merged = child.merge(dep, on="depends_on_iniciativa_id", how="left")
    unsatisfied = int(((merged["child_selected"] == 1) & (merged["dep_selected"] != 1)).sum())
    assert unsatisfied == 0


@pytest.mark.critical
@pytest.mark.full
def test_portfolio_budget_and_capacity_constraints_hold() -> None:
    invest = read_csv("investment_prioritization_final.csv")
    summary = read_csv("portfolio_summary.csv").iloc[0]

    capex_selected = float(summary["capex_selected"])
    opex_selected = float(summary["opex_selected"])
    assert capex_selected <= float(summary["capex_budget"]) + 1e-6
    assert opex_selected <= float(summary["opex_budget"]) + 1e-6
    assert int(summary["initiatives_selected"]) <= 24

    selected = invest[invest["selected_portfolio_flag"] == 1]
    assert int((selected["portfolio_wave"] == "OLA_1").sum()) <= 10
    assert int((selected["portfolio_wave"] == "OLA_2").sum()) <= 10
