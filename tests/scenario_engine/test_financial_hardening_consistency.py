from __future__ import annotations

import numpy as np
import pytest

from tests._helpers import read_csv


@pytest.mark.blocker
@pytest.mark.quick
@pytest.mark.full
def test_avoided_loss_does_not_exceed_non_energy_exposure() -> None:
    scenario = read_csv("scenario_table.csv")
    required = {"avoided_loss", "baseline_non_energy_exposure"}
    assert required.issubset(set(scenario.columns))
    assert (scenario["avoided_loss"] <= scenario["baseline_non_energy_exposure"] + 1e-8).all()


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_financial_sign_chain_is_consistent() -> None:
    scenario = read_csv("scenario_table.csv")
    required = {
        "gross_technical_value",
        "net_operational_value",
        "net_captured_value",
        "downside_adjusted_annual",
    }
    assert required.issubset(set(scenario.columns))

    assert (scenario["gross_technical_value"] >= scenario["net_operational_value"] - 1e-8).all()
    assert (scenario["net_operational_value"] >= scenario["net_captured_value"] - 1e-8).all()
    assert (scenario["net_captured_value"] >= scenario["downside_adjusted_annual"] - 1e-8).all()
    assert scenario["downside_adjusted_annual"].ge(0).all()


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_discounted_payback_is_consistent_with_positive_npv() -> None:
    scenario = read_csv("scenario_table.csv")
    positive = scenario[scenario["npv_risk_adjusted"] > 0]
    assert not positive.empty
    assert positive["payback_meses"].le(120).all()


@pytest.mark.high
@pytest.mark.quick
@pytest.mark.full
def test_shock_energia_increases_energy_value_median() -> None:
    scenario = read_csv("scenario_table.csv")
    pivot = scenario.pivot_table(
        index="iniciativa_id",
        columns="macro_scenario",
        values="gross_technical_value_energy",
        aggfunc="mean",
    )
    required = {"Base", "ShockEnergia"}
    assert required.issubset(set(pivot.columns))

    ratio = (pivot["ShockEnergia"] / pivot["Base"]).replace([np.inf, -np.inf], np.nan).dropna()
    assert not ratio.empty
    assert float(ratio.median()) >= 1.05


@pytest.mark.high
@pytest.mark.full
def test_maturity_stage_and_committee_claim_flags_are_consistent() -> None:
    invest = read_csv("investment_prioritization_final.csv")
    required = {"financial_maturity_stage", "committee_claim_allowed_flag", "committee_claim_caveat"}
    assert required.issubset(set(invest.columns))

    non_candidate = invest[invest["financial_maturity_stage"].isin(["screening", "pre_feasibility"])]
    assert (non_candidate["committee_claim_allowed_flag"] == 0).all()
    assert non_candidate["committee_claim_caveat"].str.contains("No committee-grade", regex=False).all()
