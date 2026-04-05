from __future__ import annotations

import json

import pytest

from tests._helpers import DATA_PROCESSED, DASHBOARD_HTML, OUTPUTS, read_csv


CANONICAL_DATASET = DATA_PROCESSED / "dashboard_canonical_dataset.json"
SERVING_AUDIT = DATA_PROCESSED / "dashboard_serving_audit.csv"
LEGACY_HTMLS = [
    OUTPUTS / "dashboard" / "dashboard_executive_light.html",
    OUTPUTS / "dashboard" / "dashboard_full_offline.html",
    OUTPUTS / "dashboard" / "dashboard_board_pack.html",
]
LEGACY_DATASETS = [
    DATA_PROCESSED / "dashboard_exec_dataset.json",
    DATA_PROCESSED / "dashboard_full_dataset.json",
]


@pytest.mark.blocker
@pytest.mark.quick
@pytest.mark.full
def test_single_official_dashboard_html_exists_without_competitors() -> None:
    assert DASHBOARD_HTML.exists()
    for path in LEGACY_HTMLS:
        assert not path.exists(), f"legacy dashboard must be absent: {path.name}"


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_single_official_dashboard_dataset_exists_without_legacy_payloads() -> None:
    assert CANONICAL_DATASET.exists()
    for path in LEGACY_DATASETS:
        assert not path.exists(), f"legacy dashboard dataset must be absent: {path.name}"


@pytest.mark.critical
@pytest.mark.full
def test_dashboard_dataset_is_version_stamped_and_aligned_with_snapshot() -> None:
    snapshot = read_csv("executive_kpi_snapshot.csv").iloc[0]
    payload = json.loads(CANONICAL_DATASET.read_text(encoding="utf-8"))
    meta = payload.get("meta", {})

    assert meta.get("dashboard_mode") == "canonical_single_html"
    assert str(meta.get("run_id")) == str(snapshot["run_id"])
    assert str(meta.get("metric_version_set")) == str(snapshot["metric_version_set"])
    assert str(meta.get("snapshot_id", "")).startswith(f"{snapshot['run_id']}:")
    assert "|" in str(meta.get("validation_state", ""))


@pytest.mark.high
@pytest.mark.quick
@pytest.mark.full
def test_dashboard_html_contains_required_stamps_and_mode() -> None:
    html = DASHBOARD_HTML.read_text(encoding="utf-8")
    required = [
        "dashboard_mode: canonical_single_html",
        "run_id:",
        "snapshot_id:",
        "metric_version_set:",
        "decision_model_version:",
        "validation_state:",
    ]
    for token in required:
        assert token in html, f"missing required dashboard stamp token: {token}"


@pytest.mark.high
@pytest.mark.full
def test_dashboard_serving_audit_flags_and_thresholds() -> None:
    audit = read_csv("dashboard_serving_audit.csv")
    assert not audit.empty
    metric_map = dict(zip(audit["metric"].astype(str), audit["value"]))

    assert int(metric_map.get("canonical_under_threshold_flag", 0)) == 1
    assert int(metric_map.get("no_competing_html_flag", 0)) == 1
    assert int(metric_map.get("legacy_outputs_remaining_count", 1)) == 0
    assert int(metric_map.get("canonical_html_bytes", 999999999)) <= int(
        metric_map.get("canonical_html_threshold_bytes", 4_000_000)
    )

