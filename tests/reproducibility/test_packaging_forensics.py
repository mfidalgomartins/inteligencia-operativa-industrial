from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from tests._helpers import DATA_PROCESSED, PROJECT_ROOT


ENV_FP = PROJECT_ROOT / "manifests" / "environment_fingerprint.json"
ART_FP = PROJECT_ROOT / "manifests" / "artifact_fingerprints_latest.csv"
RELEASE_PACKS_DIR = PROJECT_ROOT / "outputs" / "release_packs"


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_environment_fingerprint_and_lockfile_consistency() -> None:
    assert ENV_FP.exists()
    payload = json.loads(ENV_FP.read_text(encoding="utf-8"))
    assert payload.get("lockfile_present") is True
    assert payload.get("lockfile_sync_with_runtime") is True
    assert str(payload.get("python_version", "")).count(".") >= 1
    assert int(payload.get("pip_freeze_packages", 0)) > 0


@pytest.mark.high
@pytest.mark.quick
@pytest.mark.full
def test_artifact_fingerprints_are_complete_for_critical_set() -> None:
    assert ART_FP.exists()
    artifacts = pd.read_csv(ART_FP)
    assert not artifacts.empty
    assert (artifacts["exists_flag"] == 1).all()
    assert artifacts["sha256"].astype(str).str.len().eq(64).all()


@pytest.mark.critical
@pytest.mark.full
def test_release_packs_generated_for_all_modes() -> None:
    expected = {"minimal_public", "github_reviewer", "committee_pack"}
    assert RELEASE_PACKS_DIR.exists()
    observed = {path.name for path in RELEASE_PACKS_DIR.iterdir() if path.is_dir()}
    assert expected.issubset(observed)

    governance = pd.read_csv(DATA_PROCESSED / "release_pack_governance_audit.csv")
    assert set(governance["profile"].astype(str)) == expected

    for mode in expected:
        manifest = RELEASE_PACKS_DIR / mode / "pack_manifest.csv"
        assert manifest.exists(), f"missing pack manifest for {mode}"
        pack = pd.read_csv(manifest)
        assert not pack.empty
        governance_status = governance.loc[governance["profile"] == mode, "governance_status"].iloc[0]
        if governance_status == "BLOCKED":
            blocked_note = RELEASE_PACKS_DIR / mode / "README_BLOCKED.md"
            assert blocked_note.exists(), f"blocked profile {mode} must include README_BLOCKED.md"
            assert int((pack["governance_status"] == "BLOCKED").sum()) >= 1
        else:
            assert int((pack["exists_flag"] == 0).sum()) == 0


@pytest.mark.high
@pytest.mark.quick
@pytest.mark.full
def test_packaging_audit_shows_material_reduction_vs_full_repo() -> None:
    audit = pd.read_csv(DATA_PROCESSED / "packaging_audit_before_after.csv")
    assert not audit.empty
    assert (audit["pack_size_bytes"] > 0).all()
    assert (audit["reduction_vs_full_repo_pct"] >= 90).all()


@pytest.mark.high
@pytest.mark.quick
@pytest.mark.full
def test_committee_pack_blocked_when_readiness_not_publishable() -> None:
    readiness = pd.read_csv(DATA_PROCESSED / "validation_readiness_summary.csv").iloc[0]
    governance = pd.read_csv(DATA_PROCESSED / "release_pack_governance_audit.csv")
    committee = governance[governance["profile"] == "committee_pack"].iloc[0]

    if readiness["publish_decision"] != "publish":
        assert committee["governance_status"] == "BLOCKED"
    else:
        assert committee["governance_status"] == "ALLOWED"


@pytest.mark.high
@pytest.mark.quick
@pytest.mark.full
def test_sample_data_manifest_generated_with_expected_files() -> None:
    manifest = pd.read_csv(DATA_PROCESSED / "sample_data_manifest.csv")
    expected = {
        "raw_consumos_energia_sample.csv",
        "raw_produccion_turno_sample.csv",
        "processed_line_day_features_sample.csv",
        "processed_investment_prioritization_sample.csv",
    }
    observed = set(manifest["sample_file"].astype(str))
    assert expected.issubset(observed)
    assert (manifest["status"] == "OK").all()
