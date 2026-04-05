from __future__ import annotations

import numpy as np
import pytest

from tests._helpers import read_csv


WEIGHTS = np.array([0.20, 0.18, 0.10, 0.14, 0.12, 0.10, 0.10, 0.06], dtype=float)
COMPONENTS = [
    "energy_saving_score",
    "operational_impact_score",
    "emissions_reduction_score",
    "implementation_feasibility_score",
    "payback_score",
    "strategic_priority_score",
    "financial_resilience_score",
    "uncertainty_index",
]


@pytest.mark.blocker
@pytest.mark.quick
@pytest.mark.full
def test_score_formula_reconciliation_exact() -> None:
    invest = read_csv("investment_prioritization_final.csv")
    calc = invest[COMPONENTS].to_numpy(dtype=float) @ WEIGHTS
    delta = np.abs(calc - invest["improvement_priority_index"].to_numpy(dtype=float))
    assert float(np.nanmax(delta)) <= 1e-8


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_priority_ranking_stability_across_sensitivity_scenarios() -> None:
    sensitivity = read_csv("investment_prioritization_sensitivity.csv")
    base = set(
        sensitivity.loc[sensitivity["scenario"] == "base"]
        .sort_values("ranking")
        .head(10)["iniciativa_id"]
        .tolist()
    )
    assert len(base) == 10

    for scenario_name in ["energia_alta", "operacion_critica"]:
        current = set(
            sensitivity.loc[sensitivity["scenario"] == scenario_name]
            .sort_values("ranking")
            .head(10)["iniciativa_id"]
            .tolist()
        )
        overlap = len(base & current)
        assert overlap >= 7, f"top-10 overlap too low in {scenario_name}: {overlap}/10"


@pytest.mark.high
@pytest.mark.full
def test_priority_ranking_perturbation_robustness() -> None:
    invest = read_csv("investment_prioritization_final.csv")
    x = invest[COMPONENTS].to_numpy(dtype=float)
    base_scores = x @ WEIGHTS
    top_base = set(invest.iloc[np.argsort(-base_scores)[:10]]["iniciativa_id"].tolist())

    rng = np.random.default_rng(20260331)
    noise = rng.normal(loc=0.0, scale=0.02, size=x.shape)
    perturbed = np.clip(x * (1.0 + noise), 0.0, 100.0)
    pert_scores = perturbed @ WEIGHTS
    top_pert = set(invest.iloc[np.argsort(-pert_scores)[:10]]["iniciativa_id"].tolist())

    overlap = len(top_base & top_pert)
    assert overlap >= 8, f"top-10 overlap under perturbation too low: {overlap}/10"


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_score_ranges_and_tier_alignment() -> None:
    invest = read_csv("investment_prioritization_final.csv")
    assert invest["improvement_priority_index"].between(0, 100).all()
    assert invest["energy_saving_score"].between(0, 100).all()
    assert invest["implementation_feasibility_score"].between(0, 100).all()

    tier_order = {"Tier 1": 4, "Tier 2": 3, "Tier 3": 2, "Tier 4": 1}
    ordered = invest.sort_values("improvement_priority_index", ascending=False).copy()
    ordered["tier_num"] = ordered["initiative_tier"].map(tier_order)
    assert ordered["tier_num"].isna().sum() == 0
    monotonic_fail = (ordered["tier_num"].diff().fillna(0) > 0).sum()
    assert int(monotonic_fail) == 0
