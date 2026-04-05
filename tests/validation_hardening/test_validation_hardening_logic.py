from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import pytest

from src.validate_project import determine_publish_decision, evaluate_waiver_status
from tests._helpers import OUTPUTS, PROJECT_ROOT, read_csv


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_readiness_level_assignment_is_from_allowed_taxonomy() -> None:
    summary = read_csv("validation_readiness_summary.csv").iloc[0]
    assert summary["overall_readiness_level"] in {
        "publish-blocked",
        "structurally-valid",
        "analytically-acceptable",
        "screening-grade-only",
        "decision-support-only",
        "not-committee-grade",
        "committee-candidate",
        "committee-grade",
    }
    assert summary["technical_state"] in {"publish-blocked", "structurally-valid"}
    assert summary["committee_readiness_state"] in {"not-committee-grade", "committee-candidate", "committee-grade"}
    assert summary["committee_readiness_domain_state"] in {"not-committee-grade", "committee-candidate", "committee-grade"}

    if summary["overall_readiness_level"] not in {"committee-candidate", "committee-grade"}:
        assert summary["committee_readiness_state"] == "not-committee-grade"


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_overclaim_detection_exists_when_committee_claims_present() -> None:
    checks = read_csv("validation_checks_hardened.csv")
    overclaim = checks[checks["check"] == "overclaim_risk"]
    assert not overclaim.empty
    assert overclaim["status"].iloc[0] in {"OK", "WARN", "ERROR"}
    alignment = checks[checks["check"] == "committee_state_alignment"]
    assert not alignment.empty
    assert alignment["status"].iloc[0] in {"OK", "WARN", "ERROR"}


@pytest.mark.blocker
@pytest.mark.quick
@pytest.mark.full
def test_publish_blocking_logic_function() -> None:
    assert determine_publish_decision("committee-grade", 0, 0) == "publish"
    assert determine_publish_decision("committee-candidate", 0, 1) == "publish with caveat"
    assert determine_publish_decision("not-committee-grade", 0, 0) == "publish with caveat"
    assert determine_publish_decision("publish-blocked", 1, 0) == "no publish"


@pytest.mark.critical
@pytest.mark.full
def test_waiver_expiry_logic_function() -> None:
    waiver = pd.Series(
        {
            "estado": "activo",
            "fecha_caducidad": (date.today() - timedelta(days=1)).isoformat(),
            "nivel_maximo_permitido_uso": "committee-candidate",
        }
    )
    applied, status = evaluate_waiver_status(waiver, as_of=date.today(), requested_usage_level="committee-candidate")
    assert not applied
    assert status == "expired"


@pytest.mark.high
@pytest.mark.full
def test_validation_state_is_reflected_in_report_narrative() -> None:
    summary = read_csv("validation_readiness_summary.csv").iloc[0]
    report = (OUTPUTS / "reports" / "validation_report.md").read_text(encoding="utf-8")
    assert str(summary["overall_readiness_level"]) in report
    assert str(summary["publish_decision"]) in report
    assert str(summary["committee_readiness_state"]) in report

    readme = (PROJECT_ROOT / "README.md").read_text(encoding="utf-8")
    if summary["overall_readiness_level"] not in {"committee-candidate", "committee-grade"}:
        assert "committee-grade" not in readme.lower()
