from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from .config import DATA_PROCESSED_DIR, OUTPUT_REPORTS_DIR, PROJECT_ROOT


MANIFESTS_DIR = PROJECT_ROOT / "manifests"
RELEASES_DIR = MANIFESTS_DIR / "releases"


def write_build_manifest(run_id: str, steps: list[dict[str, object]], overall_status: str) -> Path:
    MANIFESTS_DIR.mkdir(parents=True, exist_ok=True)
    RELEASES_DIR.mkdir(parents=True, exist_ok=True)

    validation_summary_path = DATA_PROCESSED_DIR / "validation_readiness_summary.csv"
    validation_summary: dict[str, object] = {}
    if validation_summary_path.exists():
        try:
            row = pd.read_csv(validation_summary_path).iloc[0].to_dict()
            validation_summary = {
                "overall_readiness_level": row.get("overall_readiness_level"),
                "technical_state": row.get("technical_state"),
                "decision_state": row.get("decision_state"),
                "committee_readiness_state": row.get("committee_readiness_state"),
                "evidence_grade": row.get("evidence_grade"),
                "publish_decision": row.get("publish_decision"),
                "unwaived_blockers": int(row.get("unwaived_blockers", 0)),
                "unwaived_critical": int(row.get("unwaived_critical", 0)),
            }
        except Exception:
            validation_summary = {"warning": "validation_summary_unavailable"}

    lineage_summary_path = DATA_PROCESSED_DIR / "recommendation_trace_table.csv"
    lineage_registry_path = DATA_PROCESSED_DIR / "lineage_registry.csv"
    version_dependency_path = DATA_PROCESSED_DIR / "version_dependency_map.csv"
    env_fingerprint_path = MANIFESTS_DIR / "environment_fingerprint.json"
    packaging_audit_path = DATA_PROCESSED_DIR / "packaging_audit_before_after.csv"
    packaging_governance_path = DATA_PROCESSED_DIR / "release_pack_governance_audit.csv"

    lineage_summary: dict[str, object] = {}
    if lineage_summary_path.exists() and lineage_registry_path.exists():
        try:
            trace = pd.read_csv(lineage_summary_path)
            registry = pd.read_csv(lineage_registry_path)
            lineage_summary = {
                "recommendations_traced": int(len(trace)),
                "selected_recommendations_traced": int(trace["selected_flag"].sum()) if "selected_flag" in trace.columns else 0,
                "lineage_nodes": int(len(registry)),
                "lineage_ready": True,
            }
        except Exception:
            lineage_summary = {"lineage_ready": False, "warning": "lineage_summary_unavailable"}
    else:
        lineage_summary = {"lineage_ready": False, "warning": "lineage_artifacts_missing"}

    version_dependency: dict[str, object] = {}
    if version_dependency_path.exists():
        try:
            version_row = pd.read_csv(version_dependency_path).iloc[0].to_dict()
            version_dependency = version_row
        except Exception:
            version_dependency = {"warning": "version_dependency_unavailable"}

    environment_fingerprint: dict[str, object] = {}
    if env_fingerprint_path.exists():
        try:
            environment_fingerprint = json.loads(env_fingerprint_path.read_text(encoding="utf-8"))
        except Exception:
            environment_fingerprint = {"warning": "environment_fingerprint_unavailable"}

    packaging_summary: dict[str, object] = {}
    if packaging_audit_path.exists():
        try:
            packs = pd.read_csv(packaging_audit_path)
            packaging_summary = {
                "packs_generated": int(len(packs)),
                "min_reduction_pct": float(packs["reduction_vs_full_repo_pct"].min()) if not packs.empty else 0.0,
                "max_pack_size_mb": float(packs["pack_size_mb"].max()) if not packs.empty else 0.0,
            }
        except Exception:
            packaging_summary = {"warning": "packaging_summary_unavailable"}

    if packaging_governance_path.exists():
        try:
            gov = pd.read_csv(packaging_governance_path)
            packaging_summary["profiles_blocked"] = int((gov["governance_status"] == "BLOCKED").sum())
            packaging_summary["profiles_allowed"] = int((gov["governance_status"] == "ALLOWED").sum())
        except Exception:
            packaging_summary["profiles_blocked"] = "unknown"
            packaging_summary["profiles_allowed"] = "unknown"

    payload = {
        "run_id": run_id,
        "generated_at_utc": datetime.utcnow().isoformat(timespec="seconds"),
        "overall_status": overall_status,
        "steps": steps,
        "validation_readiness": validation_summary,
        "lineage_traceability": lineage_summary,
        "version_dependency": version_dependency,
        "environment_fingerprint": environment_fingerprint,
        "packaging_summary": packaging_summary,
    }

    latest = MANIFESTS_DIR / "build_manifest_latest.json"
    release_file = RELEASES_DIR / f"build_manifest_{run_id}.json"

    latest.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    release_file.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    return release_file


def write_phase_status_report(run_id: str, steps: list[dict[str, object]], overall_status: str) -> Path:
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    lines = [
        "# Estado de Transformación por Fases",
        "",
        f"- run_id: **{run_id}**",
        f"- estado global: **{overall_status}**",
        "",
        "| Step | Status | Duration (s) |",
        "|---|---:|---:|",
    ]

    for row in steps:
        lines.append(f"| {row['step']} | {row['status']} | {row['duration_seconds']:.2f} |")

    validation_summary_path = DATA_PROCESSED_DIR / "validation_readiness_summary.csv"
    if validation_summary_path.exists():
        try:
            row = pd.read_csv(validation_summary_path).iloc[0]
            lines.extend(
                [
                    "",
                    "## Validation Readiness",
                    f"- overall_readiness_level: **{row.get('overall_readiness_level', 'n/a')}**",
                    f"- evidence_grade: **{row.get('evidence_grade', 'n/a')}**",
                    f"- publish_decision: **{row.get('publish_decision', 'n/a')}**",
                    f"- committee_readiness_state: **{row.get('committee_readiness_state', 'n/a')}**",
                ]
            )
        except Exception:
            lines.extend(["", "## Validation Readiness", "- unavailable"])

    recommendation_trace_path = DATA_PROCESSED_DIR / "recommendation_trace_table.csv"
    packaging_audit_path = DATA_PROCESSED_DIR / "packaging_audit_before_after.csv"
    packaging_governance_path = DATA_PROCESSED_DIR / "release_pack_governance_audit.csv"
    if recommendation_trace_path.exists():
        try:
            trace = pd.read_csv(recommendation_trace_path)
            lines.extend(
                [
                    "",
                    "## Traceability",
                    f"- recommendations_traced: **{len(trace)}**",
                    f"- selected_recommendations_traced: **{int(trace['selected_flag'].sum()) if 'selected_flag' in trace.columns else 0}**",
                ]
            )
        except Exception:
            lines.extend(["", "## Traceability", "- unavailable"])

    if packaging_audit_path.exists():
        try:
            audit = pd.read_csv(packaging_audit_path)
            lines.extend(
                [
                    "",
                    "## Packaging",
                    f"- packs_generated: **{len(audit)}**",
                    f"- min_reduction_vs_repo_pct: **{audit['reduction_vs_full_repo_pct'].min():.2f}%**",
                    f"- max_pack_size_mb: **{audit['pack_size_mb'].max():.3f} MB**",
                ]
            )
        except Exception:
            lines.extend(["", "## Packaging", "- unavailable"])

    if packaging_governance_path.exists():
        try:
            gov = pd.read_csv(packaging_governance_path)
            lines.extend(
                [
                    "",
                    "## Packaging Governance",
                    f"- profiles_allowed: **{int((gov['governance_status'] == 'ALLOWED').sum())}**",
                    f"- profiles_blocked: **{int((gov['governance_status'] == 'BLOCKED').sum())}**",
                ]
            )
        except Exception:
            lines.extend(["", "## Packaging Governance", "- unavailable"])

    out = OUTPUT_REPORTS_DIR / "phase_transformation_status.md"
    out.write_text("\n".join(lines), encoding="utf-8")
    return out
