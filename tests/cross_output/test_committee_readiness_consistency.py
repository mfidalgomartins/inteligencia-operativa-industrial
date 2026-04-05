from __future__ import annotations

import pytest

from tests._helpers import read_csv


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_execute_now_bucket_has_minimum_financial_quality() -> None:
    invest = read_csv("investment_prioritization_final.csv")
    now = invest[invest["decision_rule"] == "ejecutar ahora"]
    assert not now.empty
    assert now["npv_risk_adjusted"].gt(0).all()
    assert now["payback_months"].le(24).all()
    assert now["improvement_priority_index"].ge(50).all()


@pytest.mark.high
@pytest.mark.full
def test_headline_kpi_consistency_snapshot_vs_sources() -> None:
    snapshot = read_csv("executive_kpi_snapshot.csv").iloc[0]
    decision = read_csv("executive_decision_snapshot.csv").iloc[0]
    line = read_csv("line_day_features_scored.csv")
    invest = read_csv("investment_prioritization_final.csv")
    line_scores = read_csv("line_criticality_scores.csv")
    equipment = read_csv("equipment_energy_anomaly_scores.csv")

    assert abs(float(snapshot["production_volume"]) - float(line["production_volume"].sum())) <= 1e-6
    assert abs(float(snapshot["loss_value_proxy"]) - float(line["loss_value_proxy"].sum())) <= 1e-6
    assert abs(float(snapshot["annual_saving_proxy"]) - float(invest["annual_saving_proxy"].sum())) <= 1e-6
    threshold = float(snapshot["line_criticality_high_threshold"])
    assert int(snapshot["line_criticality_high"]) == int((line_scores["line_criticality_score"] >= threshold).sum())
    assert str(decision["linea_prioritaria"]) == str(
        line_scores.sort_values(["line_criticality_score", "loss_value_proxy"], ascending=[False, False]).iloc[0]["linea_id"]
    )
    eq_top = (
        equipment.groupby(["equipo_id", "linea_id"], as_index=False)
        .agg(
            equipment_energy_anomaly_score=("equipment_energy_anomaly_score", "mean"),
            downtime_hours=("downtime_hours", "sum"),
        )
        .sort_values(["equipment_energy_anomaly_score", "downtime_hours"], ascending=[False, False])
        .iloc[0]
    )
    assert str(decision["equipo_prioritario"]) == str(eq_top["equipo_id"])


@pytest.mark.high
@pytest.mark.full
def test_priority_table_has_complete_executive_fields() -> None:
    invest = read_csv("investment_prioritization_final.csv")
    required = [
        "initiative_class",
        "initiative_tier",
        "selected_portfolio_flag",
        "portfolio_wave",
        "main_business_case",
        "decision_rule",
    ]
    assert set(required).issubset(set(invest.columns))
    assert invest["main_business_case"].notna().all()
