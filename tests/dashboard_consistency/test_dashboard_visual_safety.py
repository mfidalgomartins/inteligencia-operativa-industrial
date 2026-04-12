from __future__ import annotations

import json

import pytest

from tests._helpers import DASHBOARD_HTML, DATA_PROCESSED


CANONICAL_DATASET = DATA_PROCESSED / "dashboard_canonical_dataset.json"


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_dashboard_html_has_layout_safety_primitives() -> None:
    html = DASHBOARD_HTML.read_text(encoding="utf-8")

    required_tokens = [
        "overflow-x:hidden",
        ".chart-canvas-wrap{position:relative;height:",
        ".chart-canvas-wrap canvas{position:absolute;inset:0;width:100% !important;height:100% !important",
        "maintainAspectRatio:false",
        "autoSkip:true",
        "maxTicksLimit",
        "denseCategoryOptions",
    ]
    for token in required_tokens:
        assert token in html, f"Missing visual safety token: {token}"


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_dashboard_dataset_chart_density_is_capped() -> None:
    payload = json.loads(CANONICAL_DATASET.read_text(encoding="utf-8"))
    charts = payload.get("charts", {})

    assert len(charts.get("sec_trend", {}).get("datasets", [])) <= 5
    assert len(charts.get("oee_trend", {}).get("datasets", [])) <= 5
    assert len(charts.get("energy_cost_line", [])) <= 10
    assert len(charts.get("equipment_anomaly", [])) <= 10
    assert len(charts.get("root_cause", [])) <= 8
    assert len(charts.get("shift_variance", [])) <= 10


@pytest.mark.high
@pytest.mark.quick
@pytest.mark.full
def test_dashboard_kpi_cards_are_compact_and_overflow_safe() -> None:
    html = DASHBOARD_HTML.read_text(encoding="utf-8")

    expected = [
        ".kpi-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr))",
        ".kpi-card{position:relative;background:var(--panel)",
        ".kpi-card.kpi-long .kpi-value",
        "overflow-wrap:anywhere",
    ]
    for token in expected:
        assert token in html
