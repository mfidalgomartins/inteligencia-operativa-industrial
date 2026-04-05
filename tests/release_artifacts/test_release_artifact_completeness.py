from __future__ import annotations

import json

import pandas as pd
import pytest

from tests._helpers import DATA_PROCESSED, MANIFEST_LATEST, PROJECT_ROOT, REPORTS


@pytest.mark.blocker
@pytest.mark.quick
@pytest.mark.full
def test_build_manifest_exists_and_has_successful_steps() -> None:
    assert MANIFEST_LATEST.exists()
    payload = json.loads(MANIFEST_LATEST.read_text(encoding="utf-8"))

    required_steps = {
        "generate_synthetic_data",
        "run_explore_data_audit",
        "run_sql_pipeline",
        "build_feature_layer",
        "build_root_cause_ranking",
        "run_scenario_engine",
        "run_investment_prioritization",
        "run_metric_governance",
        "run_sector_parameterization",
        "run_advanced_analysis",
        "generate_charts",
        "build_dashboard",
        "run_full_validation",
        "build_dashboard_post_validation",
        "run_full_validation_final",
        "build_traceability_layer",
        "build_reports",
        "run_repro_packaging",
        "build_notebooks",
    }
    executed = {row["step"] for row in payload.get("steps", [])}
    assert required_steps.issubset(executed)
    assert payload.get("overall_status") == "SUCCESS"
    assert all(row.get("status") == "OK" for row in payload.get("steps", []))
    assert "validation_readiness" in payload
    assert "environment_fingerprint" in payload
    assert "packaging_summary" in payload
    if payload["validation_readiness"]:
        assert payload["validation_readiness"].get("publish_decision") in {"publish", "publish with caveat", "no publish"}
    if payload["environment_fingerprint"]:
        assert payload["environment_fingerprint"].get("lockfile_present") is True
    if payload["packaging_summary"]:
        assert float(payload["packaging_summary"].get("min_reduction_pct", 0.0)) >= 90.0


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_release_artifact_core_pack_exists() -> None:
    required_files = [
        PROJECT_ROOT / "requirements.lock.txt",
        PROJECT_ROOT / "manifests" / "environment_fingerprint.json",
        PROJECT_ROOT / "manifests" / "artifact_fingerprints_latest.csv",
        DATA_PROCESSED / "executive_kpi_snapshot.csv",
        DATA_PROCESSED / "executive_decision_snapshot.csv",
        DATA_PROCESSED / "sector_profile_registry.csv",
        DATA_PROCESSED / "sector_adaptation_evidence.csv",
        DATA_PROCESSED / "sector_mapping_coverage.csv",
        DATA_PROCESSED / "semantic_metric_registry.csv",
        DATA_PROCESSED / "metric_contract_validation.csv",
        DATA_PROCESSED / "cross_output_consistency.csv",
        DATA_PROCESSED / "validation_checks_hardened.csv",
        DATA_PROCESSED / "validation_domain_assessment.csv",
        DATA_PROCESSED / "validation_readiness_summary.csv",
        DATA_PROCESSED / "validation_waivers_status.csv",
        DATA_PROCESSED / "sample_data_manifest.csv",
        DATA_PROCESSED / "release_pack_manifest.csv",
        DATA_PROCESSED / "release_pack_governance_audit.csv",
        DATA_PROCESSED / "packaging_audit_before_after.csv",
        DATA_PROCESSED / "output_retention_audit.csv",
        DATA_PROCESSED / "lineage_registry.csv",
        DATA_PROCESSED / "lineage_edges.csv",
        DATA_PROCESSED / "recommendation_trace_table.csv",
        DATA_PROCESSED / "executive_output_trace_table.csv",
        DATA_PROCESSED / "version_dependency_map.csv",
        DATA_PROCESSED / "scenario_table.csv",
        DATA_PROCESSED / "investment_prioritization_final.csv",
        DATA_PROCESSED / "portfolio_summary.csv",
        REPORTS / "validation_report.md",
        REPORTS / "advanced_analysis_report.md",
        REPORTS / "sector_parameterization_evidence.md",
        REPORTS / "repro_packaging_summary.md",
        REPORTS / "phase_transformation_status.md",
    ]
    missing = [str(path.name) for path in required_files if not path.exists()]
    assert not missing, f"Missing release artifacts: {missing}"


@pytest.mark.high
@pytest.mark.full
def test_committee_readiness_minimum_consistency() -> None:
    checklist = pd.read_csv(DATA_PROCESSED / "validation_checks_hardened.csv")
    issues = pd.read_csv(DATA_PROCESSED / "validation_issues_found.csv")
    readiness = pd.read_csv(DATA_PROCESSED / "validation_readiness_summary.csv").iloc[0]
    domains = pd.read_csv(DATA_PROCESSED / "validation_domain_assessment.csv")

    blocker_subset = checklist[checklist["severity"] == "BLOCKER"]
    assert not blocker_subset.empty
    unresolved_blockers = int((issues["severity"] == "BLOCKER").sum())
    assert int(readiness["unwaived_blockers"]) == unresolved_blockers
    assert readiness["publish_decision"] in {"publish", "publish with caveat", "no publish"}
    assert len(domains) >= 7

    if readiness["publish_decision"] == "no publish":
        assert int(readiness["unwaived_blockers"]) > 0

    if readiness["overall_readiness_level"] not in {"committee-candidate", "committee-grade"}:
        assert readiness["committee_readiness_state"] == "not-committee-grade"
