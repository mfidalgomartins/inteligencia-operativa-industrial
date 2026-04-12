from __future__ import annotations

import hashlib
import json
import platform
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .config import (
    DATA_PROCESSED_DIR,
    DATA_RAW_DIR,
    DATA_SAMPLES_DIR,
    OUTPUT_RELEASE_PACKS_DIR,
    PROJECT_ROOT,
)


RELEASE_PROFILE_PATH = PROJECT_ROOT / "configs" / "release" / "release_profiles.json"
ENV_FINGERPRINT_PATH = PROJECT_ROOT / "manifests" / "environment_fingerprint.json"
ARTIFACT_FINGERPRINT_PATH = PROJECT_ROOT / "manifests" / "artifact_fingerprints_latest.csv"
PACKAGING_AUDIT_PATH = DATA_PROCESSED_DIR / "packaging_audit_before_after.csv"
RETENTION_AUDIT_PATH = DATA_PROCESSED_DIR / "output_retention_audit.csv"
PACK_GOVERNANCE_AUDIT_PATH = DATA_PROCESSED_DIR / "release_pack_governance_audit.csv"


SAMPLE_SPECS = [
    ("raw_consumos_energia_sample.csv", DATA_RAW_DIR / "consumos_energia_horarios.csv", 4000),
    ("raw_produccion_turno_sample.csv", DATA_RAW_DIR / "produccion_turno.csv", 2000),
    ("processed_line_day_features_sample.csv", DATA_PROCESSED_DIR / "line_day_features_scored.csv", 2000),
    (
        "processed_investment_prioritization_sample.csv",
        DATA_PROCESSED_DIR / "investment_prioritization_final.csv",
        800,
    ),
]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_rel(path: Path) -> str:
    return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))


def _folder_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for file in path.rglob("*"):
        if file.is_file():
            total += file.stat().st_size
    return total


def _read_release_profiles() -> dict:
    if not RELEASE_PROFILE_PATH.exists():
        raise FileNotFoundError(f"No se encontró release profile config: {RELEASE_PROFILE_PATH}")
    return json.loads(RELEASE_PROFILE_PATH.read_text(encoding="utf-8"))


def _write_samples() -> pd.DataFrame:
    DATA_SAMPLES_DIR.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, object]] = []

    for target_name, source_path, nrows in SAMPLE_SPECS:
        if not source_path.exists():
            rows.append(
                {
                    "sample_file": target_name,
                    "source_file": _safe_rel(source_path),
                    "status": "MISSING_SOURCE",
                    "rows_written": 0,
                }
            )
            continue

        sample_path = DATA_SAMPLES_DIR / target_name
        sample_df = pd.read_csv(source_path, nrows=nrows)
        sample_df.to_csv(sample_path, index=False)

        rows.append(
            {
                "sample_file": target_name,
                "source_file": _safe_rel(source_path),
                "status": "OK",
                "rows_written": int(len(sample_df)),
            }
        )

    samples_df = pd.DataFrame(rows)
    samples_df.to_csv(DATA_PROCESSED_DIR / "sample_data_manifest.csv", index=False)
    return samples_df


def _build_environment_fingerprint() -> dict[str, object]:
    pip_freeze = subprocess.run(
        [sys.executable, "-m", "pip", "freeze"],
        check=True,
        text=True,
        capture_output=True,
    ).stdout.strip()

    freeze_lines = [line.strip() for line in pip_freeze.splitlines() if line.strip()]
    freeze_norm = "\n".join(sorted(freeze_lines))
    freeze_sha = hashlib.sha256(freeze_norm.encode("utf-8")).hexdigest()

    req_txt = PROJECT_ROOT / "requirements.txt"
    req_lock = PROJECT_ROOT / "requirements.lock.txt"
    lock_lines: list[str] = []
    if req_lock.exists():
        lock_raw = req_lock.read_text(encoding="utf-8")
        lock_lines = [line.strip() for line in lock_raw.splitlines() if line.strip()]
    lock_norm = "\n".join(sorted(lock_lines))
    lock_sha_norm = hashlib.sha256(lock_norm.encode("utf-8")).hexdigest() if lock_norm else ""

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "python_executable": sys.executable,
        "python_version": sys.version.split()[0],
        "platform": platform.platform(),
        "machine": platform.machine(),
        "requirements_txt_sha256": _sha256(req_txt) if req_txt.exists() else "",
        "requirements_lock_sha256": _sha256(req_lock) if req_lock.exists() else "",
        "requirements_lock_normalized_sha256": lock_sha_norm,
        "pip_freeze_sha256": freeze_sha,
        "pip_freeze_packages": len(freeze_lines),
        "lockfile_present": req_lock.exists(),
        "lockfile_sync_with_runtime": bool(req_lock.exists() and lock_norm == freeze_norm),
    }

    ENV_FINGERPRINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    ENV_FINGERPRINT_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return payload


def _build_artifact_fingerprints() -> pd.DataFrame:
    fingerprints: list[dict[str, object]] = []
    targets = [
        PROJECT_ROOT / "README.md",
        PROJECT_ROOT / "manifests" / "build_manifest_latest.json",
        DATA_PROCESSED_DIR / "executive_kpi_snapshot.csv",
        DATA_PROCESSED_DIR / "validation_readiness_summary.csv",
        DATA_PROCESSED_DIR / "portfolio_summary.csv",
        DATA_PROCESSED_DIR / "sector_profile_registry.csv",
        PROJECT_ROOT / "outputs" / "reports" / "validation_report.md",
        PROJECT_ROOT / "outputs" / "dashboard" / "industrial-operating-command-center.html",
    ]
    for path in targets:
        if not path.exists():
            fingerprints.append(
                {
                    "path": _safe_rel(path),
                    "exists_flag": 0,
                    "size_bytes": 0,
                    "sha256": "",
                    "modified_utc": "",
                }
            )
            continue
        fingerprints.append(
            {
                "path": _safe_rel(path),
                "exists_flag": 1,
                "size_bytes": int(path.stat().st_size),
                "sha256": _sha256(path),
                "modified_utc": datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat(timespec="seconds"),
            }
        )

    df = pd.DataFrame(fingerprints)
    ARTIFACT_FINGERPRINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(ARTIFACT_FINGERPRINT_PATH, index=False)
    return df


def _write_profile_pack(profile_name: str, file_list: list[str], description: str) -> pd.DataFrame:
    profile_dir = OUTPUT_RELEASE_PACKS_DIR / profile_name
    if profile_dir.exists():
        shutil.rmtree(profile_dir)
    profile_dir.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, object]] = []
    for rel_path in file_list:
        src = PROJECT_ROOT / rel_path
        dst = profile_dir / rel_path
        if not src.exists():
            rows.append(
                {
                    "profile": profile_name,
                    "description": description,
                    "relative_path": rel_path,
                    "exists_flag": 0,
                    "size_bytes": 0,
                    "sha256": "",
                }
            )
            continue

        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        rows.append(
            {
                "profile": profile_name,
                "description": description,
                "relative_path": rel_path,
                "exists_flag": 1,
                "size_bytes": int(src.stat().st_size),
                "sha256": _sha256(src),
            }
        )

    manifest_df = pd.DataFrame(rows)
    manifest_df.to_csv(profile_dir / "pack_manifest.csv", index=False)
    return manifest_df


def _load_readiness_summary() -> dict[str, str]:
    readiness_path = DATA_PROCESSED_DIR / "validation_readiness_summary.csv"
    if not readiness_path.exists():
        return {
            "overall_readiness_level": "unknown",
            "publish_decision": "unknown",
            "committee_readiness_state": "unknown",
        }
    row = pd.read_csv(readiness_path).iloc[0].to_dict()
    return {
        "overall_readiness_level": str(row.get("overall_readiness_level", "unknown")),
        "publish_decision": str(row.get("publish_decision", "unknown")),
        "committee_readiness_state": str(row.get("committee_readiness_state", "unknown")),
    }


def _evaluate_profile_gate(profile_payload: dict, readiness: dict[str, str]) -> tuple[bool, str]:
    governance = profile_payload.get("governance", {})
    allow_states = set(governance.get("require_readiness_states", []))
    allow_publish = set(governance.get("require_publish_decisions", []))

    readiness_state = readiness.get("overall_readiness_level", "unknown")
    publish_decision = readiness.get("publish_decision", "unknown")

    state_ok = True if not allow_states else readiness_state in allow_states
    publish_ok = True if not allow_publish else publish_decision in allow_publish
    allowed = bool(state_ok and publish_ok)

    reason = "allowed"
    if not allowed:
        reason = (
            f"blocked_by_governance: readiness={readiness_state}; publish_decision={publish_decision}; "
            f"allowed_states={sorted(allow_states) if allow_states else ['*']}; "
            f"allowed_publish={sorted(allow_publish) if allow_publish else ['*']}"
        )
    return allowed, reason


def _write_blocked_profile_pack(profile_name: str, description: str, block_reason: str, readiness: dict[str, str]) -> pd.DataFrame:
    profile_dir = OUTPUT_RELEASE_PACKS_DIR / profile_name
    if profile_dir.exists():
        shutil.rmtree(profile_dir)
    profile_dir.mkdir(parents=True, exist_ok=True)

    blocked_path = profile_dir / "README_BLOCKED.md"
    blocked_text = "\n".join(
        [
            f"# {profile_name} blocked by release governance",
            "",
            f"- Description: {description}",
            f"- Reason: {block_reason}",
            f"- overall_readiness_level: {readiness.get('overall_readiness_level', 'unknown')}",
            f"- publish_decision: {readiness.get('publish_decision', 'unknown')}",
            f"- committee_readiness_state: {readiness.get('committee_readiness_state', 'unknown')}",
            "",
            "This profile is intentionally not published to prevent overclaim in committee-facing distribution.",
        ]
    )
    blocked_path.write_text(blocked_text, encoding="utf-8")

    manifest_df = pd.DataFrame(
        [
            {
                "profile": profile_name,
                "description": description,
                "relative_path": f"{profile_name}/README_BLOCKED.md",
                "exists_flag": 1,
                "size_bytes": int(blocked_path.stat().st_size),
                "sha256": _sha256(blocked_path),
                "governance_status": "BLOCKED",
                "governance_reason": block_reason,
            }
        ]
    )
    manifest_df.to_csv(profile_dir / "pack_manifest.csv", index=False)
    return manifest_df


def _build_packaging_audit(profile_manifests: dict[str, pd.DataFrame]) -> pd.DataFrame:
    total_repo = _folder_size_bytes(PROJECT_ROOT)
    rows = []
    for profile_name, manifest in profile_manifests.items():
        existing = manifest[manifest["exists_flag"] == 1]
        pack_size = int(existing["size_bytes"].sum())
        rows.append(
            {
                "mode": profile_name,
                "repo_total_size_bytes": total_repo,
                "pack_size_bytes": pack_size,
                "pack_size_mb": round(pack_size / (1024 * 1024), 3),
                "reduction_vs_full_repo_pct": round(100.0 * (1.0 - (pack_size / max(total_repo, 1))), 3),
                "files_in_pack": int(len(existing)),
                "missing_files": int((manifest["exists_flag"] == 0).sum()),
            }
        )

    audit_df = pd.DataFrame(rows).sort_values("mode").reset_index(drop=True)
    audit_df.to_csv(PACKAGING_AUDIT_PATH, index=False)
    return audit_df


def _build_retention_audit() -> pd.DataFrame:
    rows = []
    for file in sorted(DATA_RAW_DIR.glob("*.csv")):
        rows.append(
            {
                "artifact": _safe_rel(file),
                "size_bytes": int(file.stat().st_size),
                "retention_policy": "local_only_regenerable",
            }
        )
    for file in sorted(DATA_PROCESSED_DIR.glob("*.csv")):
        policy = "publishable_small" if file.name in {
            "executive_kpi_snapshot.csv",
            "validation_readiness_summary.csv",
            "cross_output_consistency.csv",
            "portfolio_summary.csv",
            "sector_profile_registry.csv",
            "sector_mapping_coverage.csv",
        } else "local_only_regenerable"
        rows.append(
            {
                "artifact": _safe_rel(file),
                "size_bytes": int(file.stat().st_size),
                "retention_policy": policy,
            }
        )
    df = pd.DataFrame(rows).sort_values(["retention_policy", "size_bytes"], ascending=[True, False]).reset_index(drop=True)
    df.to_csv(RETENTION_AUDIT_PATH, index=False)
    return df


def run_repro_packaging() -> dict[str, pd.DataFrame]:
    OUTPUT_RELEASE_PACKS_DIR.mkdir(parents=True, exist_ok=True)
    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    release_profiles = _read_release_profiles()
    profiles = release_profiles.get("profiles", {})
    if not profiles:
        raise ValueError("release_profiles.json sin perfiles definidos")

    samples = _write_samples()
    env = _build_environment_fingerprint()
    fingerprints = _build_artifact_fingerprints()

    readiness = _load_readiness_summary()

    profile_manifests: dict[str, pd.DataFrame] = {}
    governance_rows: list[dict[str, object]] = []
    for profile_name, profile_payload in profiles.items():
        description = profile_payload.get("description", "")
        allowed, reason = _evaluate_profile_gate(profile_payload=profile_payload, readiness=readiness)

        if allowed:
            manifest_df = _write_profile_pack(
                profile_name=profile_name,
                file_list=profile_payload.get("include_files", []),
                description=description,
            )
            manifest_df["governance_status"] = "ALLOWED"
            manifest_df["governance_reason"] = "allowed"
            (OUTPUT_RELEASE_PACKS_DIR / profile_name / "pack_manifest.csv").write_text(
                manifest_df.to_csv(index=False),
                encoding="utf-8",
            )
            profile_manifests[profile_name] = manifest_df
        else:
            profile_manifests[profile_name] = _write_blocked_profile_pack(
                profile_name=profile_name,
                description=description,
                block_reason=reason,
                readiness=readiness,
            )

        governance_rows.append(
            {
                "profile": profile_name,
                "governance_status": "ALLOWED" if allowed else "BLOCKED",
                "governance_reason": reason,
                "overall_readiness_level": readiness.get("overall_readiness_level", "unknown"),
                "publish_decision": readiness.get("publish_decision", "unknown"),
                "committee_readiness_state": readiness.get("committee_readiness_state", "unknown"),
            }
        )

    packaging_audit = _build_packaging_audit(profile_manifests=profile_manifests)
    governance_df = pd.DataFrame(governance_rows).sort_values("profile").reset_index(drop=True)
    governance_df.to_csv(PACK_GOVERNANCE_AUDIT_PATH, index=False)
    retention_audit = _build_retention_audit()

    all_manifests = []
    for profile_name, manifest in profile_manifests.items():
        copy = manifest.copy()
        copy["profile"] = profile_name
        all_manifests.append(copy)
    pack_manifest = pd.concat(all_manifests, ignore_index=True)
    pack_manifest.to_csv(DATA_PROCESSED_DIR / "release_pack_manifest.csv", index=False)

    top_heavy = retention_audit.sort_values("size_bytes", ascending=False).head(8)
    summary_lines = [
        "# Reproducibility & Packaging Summary",
        "",
        "## Environment Fingerprint",
        f"- Python: `{env['python_version']}`",
        f"- Lockfile present: `{env['lockfile_present']}`",
        f"- Lock sync with runtime: `{env['lockfile_sync_with_runtime']}`",
        f"- Pip freeze packages: `{env['pip_freeze_packages']}`",
        "",
        "## Packaging Audit",
        packaging_audit.to_markdown(index=False),
        "",
        "## Release Governance Audit",
        governance_df.to_markdown(index=False),
        "",
        "## Top Heavy Artifacts (Retention Audit)",
        top_heavy.to_markdown(index=False),
        "",
        "## Sample Data Manifest",
        samples.to_markdown(index=False),
        "",
        "## Cleanup Applied",
        "- Lockfile pinneado (`requirements.lock.txt`) y fingerprint de entorno habilitado.",
        "- Packs de publicación (`minimal_public`, `github_reviewer`, `committee_pack`) generados.",
    ]
    (PROJECT_ROOT / "outputs" / "reports" / "repro_packaging_summary.md").write_text(
        "\n".join(summary_lines),
        encoding="utf-8",
    )

    return {
        "sample_data_manifest": samples,
        "artifact_fingerprints": fingerprints,
        "packaging_audit": packaging_audit,
        "packaging_governance_audit": governance_df,
        "retention_audit": retention_audit,
        "release_pack_manifest": pack_manifest,
    }


if __name__ == "__main__":
    run_repro_packaging()
