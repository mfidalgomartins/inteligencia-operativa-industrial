from __future__ import annotations

import pytest

from tests._helpers import PROJECT_ROOT, read_csv


@pytest.mark.high
@pytest.mark.quick
@pytest.mark.full
def test_readme_runtime_claims_are_synced_with_canonical_snapshots() -> None:
    readme = (PROJECT_ROOT / "README.md").read_text(encoding="utf-8")
    snapshot = read_csv("executive_kpi_snapshot.csv").iloc[0]
    decision = read_csv("executive_decision_snapshot.csv").iloc[0]

    required_static_claims = [
        "cockpit de decisión para planta industrial",
        "Dataset sintético.",
        "Métricas financieras en modo screening/pre‑feasibility.",
        "GitHub Pages:",
        "outputs/dashboard/industrial-operating-command-center.html",
    ]
    for token in required_static_claims:
        assert token in readme

    runtime_tokens = [
        f"**{float(snapshot['annual_saving_proxy']):,.2f} EUR/año**",
        f"**{float(snapshot['portfolio_npv_risk_adjusted']):,.2f} EUR**",
        f"**{str(decision['linea_prioritaria'])}**",
        f"**{str(decision['equipo_prioritario'])}**",
        f"**{float(decision['ahorro_now_top5']):,.2f} EUR/año**",
    ]
    for token in runtime_tokens:
        assert token in readme
