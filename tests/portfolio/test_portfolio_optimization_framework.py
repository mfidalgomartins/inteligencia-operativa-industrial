from __future__ import annotations

import numpy as np
import pytest

from tests._helpers import read_csv


@pytest.mark.blocker
@pytest.mark.quick
@pytest.mark.full
def test_optimization_metadata_exists_and_has_allowed_status() -> None:
    meta = read_csv("portfolio_optimization_metadata.csv").iloc[0]
    allowed_status = {
        "solved_exact",
        "time_limited_best_feasible",
        "infeasible_forced_constraints",
        "infeasible_no_feasible_solution",
    }
    assert str(meta["optimization_status"]) in allowed_status
    assert str(meta["selection_model"]) == "branch_and_bound_binary_selection"


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_optimization_status_and_optimality_degree_are_consistent() -> None:
    meta = read_csv("portfolio_optimization_metadata.csv").iloc[0]
    status = str(meta["optimization_status"])
    degree = str(meta["optimality_degree"])
    timed_out = int(meta["timed_out_flag"])
    feasible = int(meta["feasible_solution_found"])

    if status == "solved_exact":
        assert degree == "exact_for_formulation"
        assert timed_out == 0
        assert feasible == 1
    elif status == "time_limited_best_feasible":
        assert degree == "semi_formal_approximation"
        assert timed_out == 1
        assert feasible == 1
    else:
        assert degree == "no-feasible-formulation"


@pytest.mark.critical
@pytest.mark.full
def test_selected_vs_excluded_has_reason_and_same_universe_as_final_table() -> None:
    final_df = read_csv("investment_prioritization_final.csv")
    sx = read_csv("portfolio_selected_vs_excluded.csv")

    assert len(final_df) == len(sx)
    assert set(final_df["iniciativa_id"].astype(str)) == set(sx["iniciativa_id"].astype(str))
    assert sx["portfolio_constraint_reason"].astype(str).str.len().gt(0).all()


@pytest.mark.critical
@pytest.mark.full
def test_objective_best_reconciles_with_selected_portfolio_when_feasible() -> None:
    final_df = read_csv("investment_prioritization_final.csv")
    meta = read_csv("portfolio_optimization_metadata.csv").iloc[0]
    status = str(meta["optimization_status"])
    feasible = int(meta["feasible_solution_found"])
    objective_best = float(meta["objective_best"]) if not np.isnan(meta["objective_best"]) else np.nan

    selected_obj = float(final_df.loc[final_df["selected_portfolio_flag"] == 1, "objective_value_eur"].sum())
    if feasible == 1 and status in {"solved_exact", "time_limited_best_feasible"}:
        assert np.isclose(objective_best, selected_obj, atol=1e-6)


@pytest.mark.high
@pytest.mark.full
def test_constraint_pressure_and_unlocked_value_outputs_are_complete() -> None:
    pressure = read_csv("portfolio_constraint_pressure_summary.csv")
    unlocked = read_csv("portfolio_unlocked_value_analysis.csv")

    expected_pressure = {
        "capex_budget",
        "opex_budget",
        "selection_capacity",
        "reliability_min_coverage",
    }
    expected_relax = {
        "capex_plus_10pct",
        "opex_plus_10pct",
        "capacity_plus_4",
        "all_plus",
    }

    assert expected_pressure.issubset(set(pressure["constraint_name"].astype(str)))
    assert expected_relax.issubset(set(unlocked["constraint_relaxation"].astype(str)))
