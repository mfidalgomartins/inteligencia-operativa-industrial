from __future__ import annotations

import json

import pytest

from tests._helpers import PROJECT_ROOT, read_csv


METRIC_REGISTRY = PROJECT_ROOT / "contracts" / "metrics" / "metric_registry.json"


@pytest.mark.blocker
@pytest.mark.quick
@pytest.mark.full
def test_metric_registry_integrity() -> None:
    payload = json.loads(METRIC_REGISTRY.read_text(encoding="utf-8"))
    metrics = payload.get("metrics", [])
    metric_ids = [m["metric_id"] for m in metrics]

    assert payload.get("metric_version_set")
    assert payload.get("version")
    assert len(metric_ids) == len(set(metric_ids))
    assert all("formula" in m and m["formula"] for m in metrics)
    assert all("grain" in m and isinstance(m["grain"], list) and m["grain"] for m in metrics)


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_certified_kpis_have_contract_and_runtime_presence() -> None:
    payload = json.loads(METRIC_REGISTRY.read_text(encoding="utf-8"))
    contracted = {m["technical_name"] for m in payload.get("metrics", [])}
    snapshot = read_csv("executive_kpi_snapshot.csv").iloc[0]

    required = {
        "production_volume",
        "oee_synthetic",
        "sec",
        "loss_value_proxy",
        "annual_saving_proxy",
        "portfolio_npv_risk_adjusted",
    }
    assert required.issubset(set(snapshot.index))

    # `portfolio_npv_risk_adjusted` se deriva de `npv_risk_adjusted` de contrato.
    required_contracts = required - {"portfolio_npv_risk_adjusted"}
    assert required_contracts.issubset(contracted)


@pytest.mark.blocker
@pytest.mark.quick
@pytest.mark.full
def test_metric_contract_runtime_checks_without_errors() -> None:
    checks = read_csv("metric_contract_validation.csv")
    assert not checks.empty
    assert (checks["status"] != "ERROR").all()


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_sql_python_reconciliations_are_clean() -> None:
    cross = read_csv("cross_output_consistency.csv")
    required_checks = {
        "sec_sql_vs_python",
        "oee_sql_vs_python",
        "saving_scenario_vs_invest",
        "snapshot_production_vs_line_day",
        "snapshot_loss_vs_line_day",
        "snapshot_saving_vs_invest",
        "decision_top_line_vs_scores",
        "decision_top_equipment_vs_scores",
        "dashboard_meta_run_id_present_and_trackable",
        "dashboard_meta_metric_version_present",
        "dashboard_meta_validation_state_shape",
    }
    observed_checks = set(cross["check"].tolist())
    assert required_checks.issubset(observed_checks)

    strict = cross[cross["check"].isin(["sec_sql_vs_python", "oee_sql_vs_python", "snapshot_production_vs_line_day", "snapshot_loss_vs_line_day", "snapshot_saving_vs_invest"])]
    assert (strict["status"] == "OK").all()

    saving = cross[cross["check"] == "saving_scenario_vs_invest"]
    assert not saving.empty
    assert saving["status"].iloc[0] in {"OK", "WARN"}

    if "tier" in cross.columns:
        blocker = cross[cross["tier"] == "BLOCKER"]
        assert not blocker.empty
        assert (blocker["status"] == "OK").all()
