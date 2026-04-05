from pathlib import Path

import pandas as pd
import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROCESSED = PROJECT_ROOT / "data" / "processed"
OUTPUTS = PROJECT_ROOT / "outputs"
DASHBOARD = OUTPUTS / "dashboard" / "dashboard_inteligencia_operativa.html"


@pytest.mark.blocker
@pytest.mark.quick
@pytest.mark.full
def test_processed_core_outputs_exist() -> None:
    required = [
        "line_day_features_scored.csv",
        "equipment_energy_anomaly_scores.csv",
        "line_criticality_scores.csv",
        "scenario_ranking.csv",
        "investment_prioritization_final.csv",
        "executive_decision_snapshot.csv",
        "validation_checklist_final.csv",
    ]
    for file_name in required:
        assert (PROCESSED / file_name).exists(), f"Missing output: {file_name}"


@pytest.mark.blocker
@pytest.mark.quick
@pytest.mark.full
def test_validation_outputs_and_publish_decision_exist() -> None:
    checklist = pd.read_csv(PROCESSED / "validation_checklist_final.csv")
    readiness = pd.read_csv(PROCESSED / "validation_readiness_summary.csv").iloc[0]
    assert not checklist.empty
    assert "status" in checklist.columns
    assert readiness["publish_decision"] in {"publish", "publish with caveat", "no publish"}
    assert readiness["overall_readiness_level"] in {
        "publish-blocked",
        "structurally-valid",
        "analytically-acceptable",
        "screening-grade-only",
        "decision-support-only",
        "not-committee-grade",
        "committee-candidate",
        "committee-grade",
    }


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_prioritization_columns_and_ranges() -> None:
    invest = pd.read_csv(PROCESSED / "investment_prioritization_final.csv")
    expected_cols = {
        "energy_saving_score",
        "operational_impact_score",
        "emissions_reduction_score",
        "implementation_feasibility_score",
        "payback_score",
        "strategic_priority_score",
        "improvement_priority_index",
        "initiative_tier",
        "recommended_sequence",
        "main_business_case",
    }
    assert expected_cols.issubset(set(invest.columns))
    assert invest["improvement_priority_index"].between(0, 100).all()
    assert invest["payback_months"].gt(0).all()


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_dashboard_generated_and_contains_sections() -> None:
    assert DASHBOARD.exists()
    html = DASHBOARD.read_text(encoding="utf-8")
    assert "Vista de Eficiencia Operativa" in html
    assert "Vista Energética" in html
    assert "Tabla Final Interactiva de Priorización" in html
    assert "Decisión Ejecutiva" in html


@pytest.mark.high
@pytest.mark.quick
@pytest.mark.full
def test_chart_inventory_has_18_visuals() -> None:
    inventory = pd.read_csv(OUTPUTS / "reports" / "inventario_graficos.csv")
    assert len(inventory) >= 18
