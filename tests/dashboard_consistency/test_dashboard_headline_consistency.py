from __future__ import annotations

import re

import pytest

from tests._helpers import DASHBOARD_HTML, read_csv


@pytest.mark.blocker
@pytest.mark.quick
@pytest.mark.full
def test_dashboard_contains_governance_stamps() -> None:
    html = DASHBOARD_HTML.read_text(encoding="utf-8")
    snapshot = read_csv("executive_kpi_snapshot.csv").iloc[0]
    assert str(snapshot["run_id"]) in html
    assert str(snapshot["metric_version_set"]) in html


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_dashboard_headline_values_reconcile_with_snapshot() -> None:
    html = DASHBOARD_HTML.read_text(encoding="utf-8")
    snapshot = read_csv("executive_kpi_snapshot.csv").iloc[0]

    expected_tokens = [
        f"{float(snapshot['production_volume']):,.0f} t",
        f"{float(snapshot['oee_synthetic']):.3f}",
        f"{float(snapshot['sec']):.1f} kWh/t",
        f"{float(snapshot['loss_value_proxy']):,.0f} EUR",
        f"{float(snapshot['annual_saving_proxy']):,.0f} EUR/año",
        f"{float(snapshot['portfolio_npv_risk_adjusted']):,.0f} EUR",
    ]
    for token in expected_tokens:
        assert token in html, f"missing KPI token in dashboard: {token}"


@pytest.mark.high
@pytest.mark.full
def test_dashboard_decision_block_matches_top_critical_line() -> None:
    html = DASHBOARD_HTML.read_text(encoding="utf-8")
    decision = read_csv("executive_decision_snapshot.csv").iloc[0]
    assert str(decision["linea_prioritaria"]) in html
    assert str(decision["equipo_prioritario"]) in html


@pytest.mark.warning
@pytest.mark.full
def test_dashboard_does_not_compute_headline_snapshot_metrics_in_js() -> None:
    html = DASHBOARD_HTML.read_text(encoding="utf-8")
    script_match = re.search(r"<script>\s*const DATA.*?</script>", html, flags=re.DOTALL)
    assert script_match is not None
    script = script_match.group(0)

    forbidden_patterns = [
        r"reduce\(\(a,b\)=>a\+b,0\)\s*/\s*Math\.max\(1,vals\.length\).*production_volume",
        r"reduce\(\(a,b\)=>a\+b,0\)\s*/\s*Math\.max\(1,vals\.length\).*loss_value_proxy",
    ]
    for pattern in forbidden_patterns:
        assert re.search(pattern, script, flags=re.DOTALL) is None
