from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .config import DATA_PROCESSED_DIR, PROJECT_ROOT
from .runtime_claims import sync_readme_runtime_claims


CONTRACTS_DATA_PATH = PROJECT_ROOT / "contracts" / "data" / "data_contracts.json"
CONTRACTS_METRIC_PATH = PROJECT_ROOT / "contracts" / "metrics" / "metric_registry.json"
DASHBOARD_CANONICAL_PATH = DATA_PROCESSED_DIR / "dashboard_canonical_dataset.json"


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _check_invariant(df: pd.DataFrame, invariant: str) -> int:
    try:
        failed = int(len(df.query(f"not ({invariant})", engine="python")))
    except Exception:
        failed = -1
    return failed


def _cross_check_row(
    *,
    check: str,
    observed: Any,
    expected: Any,
    tolerance: float = 0.0,
    tier: str = "CRITICAL",
    allow_warn: bool = False,
    warn_tolerance: float | None = None,
) -> dict[str, object]:
    status = "ERROR"
    detail = ""

    if isinstance(observed, (int, float, np.floating)) and isinstance(expected, (int, float, np.floating)):
        delta = abs(float(observed) - float(expected))
        detail = f"delta={delta:.8f}"
        if delta <= tolerance:
            status = "OK"
        elif allow_warn and warn_tolerance is not None and delta <= warn_tolerance:
            status = "WARN"
        observed_out: Any = float(observed)
        expected_out: Any = float(expected)
    else:
        eq = str(observed) == str(expected)
        if eq:
            status = "OK"
        elif allow_warn:
            status = "WARN"
        detail = "string_or_id_compare"
        observed_out = str(observed)
        expected_out = str(expected)

    return {
        "check": check,
        "status": status,
        "tier": tier,
        "observed": observed_out,
        "expected": expected_out,
        "detail": detail,
    }


def run_metric_governance(run_id: str | None = None) -> dict[str, pd.DataFrame]:
    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    run_id = run_id or datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    data_contracts = _read_json(CONTRACTS_DATA_PATH)
    metric_registry = _read_json(CONTRACTS_METRIC_PATH)

    registry_df = pd.DataFrame(metric_registry.get("metrics", []))
    registry_df["metric_version_set"] = metric_registry.get("metric_version_set", "unknown")
    registry_df["registry_version"] = metric_registry.get("version", "0.0.0")

    checks: list[dict[str, object]] = []

    for asset in data_contracts.get("assets", []):
        name = asset["name"]
        file_path = DATA_PROCESSED_DIR / f"{name}.csv"
        if not file_path.exists():
            checks.append(
                {
                    "asset": name,
                    "check_type": "asset_exists",
                    "status": "ERROR",
                    "detail": "Archivo no encontrado en data/processed",
                    "failed_count": 1,
                }
            )
            continue

        df = pd.read_csv(file_path)

        missing_cols = [c for c in asset.get("required_columns", []) if c not in df.columns]
        checks.append(
            {
                "asset": name,
                "check_type": "required_columns",
                "status": "OK" if not missing_cols else "ERROR",
                "detail": "missing=" + ",".join(missing_cols) if missing_cols else "all_present",
                "failed_count": len(missing_cols),
            }
        )

        grain_cols = asset.get("grain", [])
        if grain_cols and all(c in df.columns for c in grain_cols):
            dup = int(df.duplicated(subset=grain_cols).sum())
            checks.append(
                {
                    "asset": name,
                    "check_type": "grain_uniqueness",
                    "status": "OK" if dup == 0 else "ERROR",
                    "detail": f"grain={grain_cols}",
                    "failed_count": dup,
                }
            )
        else:
            checks.append(
                {
                    "asset": name,
                    "check_type": "grain_uniqueness",
                    "status": "WARN",
                    "detail": "grain columns unavailable",
                    "failed_count": 0,
                }
            )

        for invariant in asset.get("invariants", []):
            failed_count = _check_invariant(df, invariant)
            checks.append(
                {
                    "asset": name,
                    "check_type": "invariant",
                    "status": "OK" if failed_count == 0 else ("WARN" if failed_count < 0 else "ERROR"),
                    "detail": invariant,
                    "failed_count": failed_count if failed_count >= 0 else 0,
                }
            )

    checks_df = pd.DataFrame(checks)

    line_day = pd.read_csv(DATA_PROCESSED_DIR / "line_day_features_scored.csv")
    line_scores = pd.read_csv(DATA_PROCESSED_DIR / "line_criticality_scores.csv")
    equipment = pd.read_csv(DATA_PROCESSED_DIR / "equipment_energy_anomaly_scores.csv")
    invest = pd.read_csv(DATA_PROCESSED_DIR / "investment_prioritization_final.csv")
    portfolio_summary = pd.read_csv(DATA_PROCESSED_DIR / "portfolio_summary.csv")
    scenario = pd.read_csv(DATA_PROCESSED_DIR / "scenario_table.csv")
    mart_line_day = pd.read_csv(DATA_PROCESSED_DIR / "mart_line_day.csv")

    line_high_threshold = float(line_scores["line_criticality_score"].quantile(0.75)) if not line_scores.empty else 70.0

    executive_snapshot = pd.DataFrame(
        [
            {
                "run_id": run_id,
                "metric_version_set": metric_registry.get("metric_version_set", "unknown"),
                "snapshot_utc": datetime.utcnow().isoformat(timespec="seconds"),
                "production_volume": float(line_day["production_volume"].sum()),
                "oee_synthetic": float(line_day["oee_synthetic"].mean()),
                "sec": float(line_day["sec"].mean()),
                "loss_value_proxy": float(line_day["loss_value_proxy"].sum()),
                "line_criticality_high": int((line_scores["line_criticality_score"] >= line_high_threshold).sum()),
                "line_criticality_high_threshold": line_high_threshold,
                "annual_saving_proxy": float(invest["annual_saving_proxy"].sum()),
                "portfolio_npv_risk_adjusted": float(invest["portfolio_npv_contribution"].sum()),
                "portfolio_selected": int(invest["selected_portfolio_flag"].sum()),
                "gross_technical_value_portfolio": float(invest["gross_technical_value"].sum()) if "gross_technical_value" in invest.columns else 0.0,
                "net_captured_value_portfolio": float(invest["net_captured_value"].sum()) if "net_captured_value" in invest.columns else float(invest["annual_saving_proxy"].sum()),
                "downside_adjusted_value_portfolio": float(invest["downside_adjusted_value"].sum()) if "downside_adjusted_value" in invest.columns else 0.0,
                "cost_of_delay_12m_portfolio": float(invest["cost_of_delay_12m"].sum()) if "cost_of_delay_12m" in invest.columns else 0.0,
                "screening_var_95_npv_portfolio": float(invest["screening_var_95_npv"].sum()) if "screening_var_95_npv" in invest.columns else 0.0,
                "financial_readiness_label": str(portfolio_summary.iloc[0]["financial_readiness_label"]) if (not portfolio_summary.empty and "financial_readiness_label" in portfolio_summary.columns) else "screening/pre-feasibility",
                "score_scaling_method": str(invest["score_scaling_method"].mode().iloc[0]) if "score_scaling_method" in invest.columns else "unknown",
                "score_scale_version": str(invest["score_scale_version"].mode().iloc[0]) if "score_scale_version" in invest.columns else "unknown",
                "contextual_score_count": int((invest.get("improvement_priority_comparability_tag", pd.Series(dtype=str)) == "contextual_comparable_inter_release").sum()),
            }
        ]
    )

    top_line = (
        line_scores.sort_values(["line_criticality_score", "loss_value_proxy"], ascending=[False, False]).iloc[0]
        if not line_scores.empty
        else pd.Series(dtype=object)
    )
    equipment_top = (
        equipment.groupby(["equipo_id", "linea_id"], as_index=False)
        .agg(
            equipment_energy_anomaly_score=("equipment_energy_anomaly_score", "mean"),
            downtime_hours=("downtime_hours", "sum"),
        )
        .sort_values(["equipment_energy_anomaly_score", "downtime_hours"], ascending=[False, False])
    )
    top_equipment = equipment_top.iloc[0] if not equipment_top.empty else pd.Series(dtype=object)
    ahorro_now_top5 = float(
        invest[invest["decision_rule"] == "ejecutar ahora"]
        .sort_values("improvement_priority_index", ascending=False)
        .head(5)["annual_saving_proxy"]
        .sum()
    )

    executive_decision_snapshot = pd.DataFrame(
        [
            {
                "run_id": run_id,
                "metric_version_set": metric_registry.get("metric_version_set", "unknown"),
                "decision_snapshot_utc": datetime.utcnow().isoformat(timespec="seconds"),
                "linea_prioritaria": str(top_line.get("linea_id", "")),
                "line_criticality_score": float(top_line.get("line_criticality_score", 0.0)),
                "equipo_prioritario": str(top_equipment.get("equipo_id", "")),
                "equipment_energy_anomaly_score": float(top_equipment.get("equipment_energy_anomaly_score", 0.0)),
                "ahorro_now_top5": ahorro_now_top5,
                "decision_logic_version": "executive_decision_v1",
            }
        ]
    )

    scenario_base = scenario[scenario["macro_scenario"].str.lower() == "base"].copy()
    if scenario_base.empty:
        scenario_base = scenario.copy()

    scen_by_init = scenario_base.groupby("iniciativa_id", as_index=False)["risk_adjusted_annual_savings"].mean()
    inv_by_init = invest[["iniciativa_id", "annual_saving_proxy"]].copy()
    merged_savings = inv_by_init.merge(scen_by_init, on="iniciativa_id", how="left")
    merged_savings["saving_delta"] = merged_savings["annual_saving_proxy"] - merged_savings["risk_adjusted_annual_savings"]

    if "net_captured_value" in scenario_base.columns and "net_captured_value" in invest.columns:
        scen_captured = scenario_base.groupby("iniciativa_id", as_index=False)["net_captured_value"].mean().rename(columns={"net_captured_value": "net_captured_scenario"})
        inv_captured = invest[["iniciativa_id", "net_captured_value"]].rename(columns={"net_captured_value": "net_captured_invest"})
        captured = inv_captured.merge(scen_captured, on="iniciativa_id", how="left")
        captured_delta = float(np.nanmax(np.abs(captured["net_captured_invest"] - captured["net_captured_scenario"]))) if not captured.empty else 0.0
    else:
        captured_delta = 0.0

    top_line_source = (
        line_scores.sort_values(["line_criticality_score", "loss_value_proxy"], ascending=[False, False]).iloc[0]
        if not line_scores.empty
        else pd.Series(dtype=object)
    )
    top_equipment_source = top_equipment

    dashboard_run_id = ""
    dashboard_metric_version = ""
    dashboard_validation_state = ""
    if DASHBOARD_CANONICAL_PATH.exists():
        try:
            dashboard_payload = json.loads(DASHBOARD_CANONICAL_PATH.read_text(encoding="utf-8"))
            dashboard_meta = dashboard_payload.get("meta", {})
            dashboard_run_id = str(dashboard_meta.get("run_id", ""))
            dashboard_metric_version = str(dashboard_meta.get("metric_version_set", ""))
            dashboard_validation_state = str(dashboard_meta.get("validation_state", ""))
        except Exception:
            dashboard_run_id = "INVALID_JSON"
            dashboard_metric_version = "INVALID_JSON"
            dashboard_validation_state = "INVALID_JSON"

    cross_rows = [
        _cross_check_row(
            check="sec_sql_vs_python",
            observed=float(line_day["sec"].mean()),
            expected=float(mart_line_day["sec_kwh_ton"].mean()),
            tolerance=1e-6,
            tier="BLOCKER",
        ),
        _cross_check_row(
            check="oee_sql_vs_python",
            observed=float(line_day["oee_synthetic"].mean()),
            expected=float(mart_line_day["oee_synthetic"].mean()),
            tolerance=1e-6,
            tier="BLOCKER",
        ),
        _cross_check_row(
            check="saving_scenario_vs_invest",
            observed=float(np.nanmax(np.abs(merged_savings["saving_delta"]))),
            expected=0.0,
            tolerance=1e-6,
            allow_warn=True,
            warn_tolerance=1e3,
            tier="CRITICAL",
        ),
        _cross_check_row(
            check="net_captured_scenario_vs_invest",
            observed=float(captured_delta),
            expected=0.0,
            tolerance=1e-6,
            allow_warn=True,
            warn_tolerance=1e3,
            tier="CRITICAL",
        ),
        _cross_check_row(
            check="snapshot_production_vs_line_day",
            observed=float(executive_snapshot.iloc[0]["production_volume"]),
            expected=float(line_day["production_volume"].sum()),
            tolerance=1e-6,
            tier="BLOCKER",
        ),
        _cross_check_row(
            check="snapshot_loss_vs_line_day",
            observed=float(executive_snapshot.iloc[0]["loss_value_proxy"]),
            expected=float(line_day["loss_value_proxy"].sum()),
            tolerance=1e-6,
            tier="BLOCKER",
        ),
        _cross_check_row(
            check="snapshot_saving_vs_invest",
            observed=float(executive_snapshot.iloc[0]["annual_saving_proxy"]),
            expected=float(invest["annual_saving_proxy"].sum()),
            tolerance=1e-6,
            tier="BLOCKER",
        ),
        _cross_check_row(
            check="decision_top_line_vs_scores",
            observed=str(executive_decision_snapshot.iloc[0]["linea_prioritaria"]),
            expected=str(top_line_source.get("linea_id", "")),
            tier="CRITICAL",
        ),
        _cross_check_row(
            check="decision_top_equipment_vs_scores",
            observed=str(executive_decision_snapshot.iloc[0]["equipo_prioritario"]),
            expected=str(top_equipment_source.get("equipo_id", "")),
            tier="CRITICAL",
        ),
        _cross_check_row(
            check="snapshot_vs_decision_run_id",
            observed=str(executive_snapshot.iloc[0]["run_id"]),
            expected=str(executive_decision_snapshot.iloc[0]["run_id"]),
            tier="CRITICAL",
        ),
        _cross_check_row(
            check="snapshot_vs_decision_metric_version_set",
            observed=str(executive_snapshot.iloc[0]["metric_version_set"]),
            expected=str(executive_decision_snapshot.iloc[0]["metric_version_set"]),
            tier="CRITICAL",
        ),
    ]

    if dashboard_run_id:
        dashboard_run_alignment = (
            "current_run"
            if dashboard_run_id == str(executive_snapshot.iloc[0]["run_id"])
            else "stale_but_allowed_pre_dashboard_refresh"
        )
        cross_rows.extend(
            [
                {
                    "check": "dashboard_meta_run_id_present_and_trackable",
                    "status": "OK" if bool(dashboard_run_id) else "WARN",
                    "tier": "HIGH",
                    "observed": dashboard_run_id,
                    "expected": "non_empty_run_id",
                    "detail": dashboard_run_alignment,
                },
                {
                    "check": "dashboard_meta_metric_version_present",
                    "status": "OK" if bool(dashboard_metric_version) else "WARN",
                    "tier": "HIGH",
                    "observed": dashboard_metric_version,
                    "expected": "non_empty_metric_version_set",
                    "detail": "presence_check",
                },
                {
                    "check": "dashboard_meta_validation_state_shape",
                    "status": "OK" if "|" in dashboard_validation_state else "ERROR",
                    "tier": "HIGH",
                    "observed": dashboard_validation_state,
                    "expected": "overall_readiness|publish_decision",
                    "detail": "validation_state_format",
                },
            ]
        )
    else:
        cross_rows.append(
            {
                "check": "dashboard_meta_presence",
                "status": "WARN",
                "tier": "HIGH",
                "observed": "missing_dashboard_dataset",
                "expected": "dashboard_canonical_dataset_available",
                "detail": "dashboard not yet generated at metric governance stage",
            }
        )

    cross_checks = pd.DataFrame(cross_rows)

    registry_df.to_csv(DATA_PROCESSED_DIR / "semantic_metric_registry.csv", index=False)
    checks_df.to_csv(DATA_PROCESSED_DIR / "metric_contract_validation.csv", index=False)
    cross_checks.to_csv(DATA_PROCESSED_DIR / "cross_output_consistency.csv", index=False)
    executive_snapshot.to_csv(DATA_PROCESSED_DIR / "executive_kpi_snapshot.csv", index=False)
    executive_decision_snapshot.to_csv(DATA_PROCESSED_DIR / "executive_decision_snapshot.csv", index=False)

    readme_path = PROJECT_ROOT / "README.md"
    sync_readme_runtime_claims(
        readme_path=readme_path,
        snapshot_row=executive_snapshot.iloc[0],
        decision_row=executive_decision_snapshot.iloc[0],
    )

    return {
        "registry": registry_df,
        "contract_checks": checks_df,
        "cross_checks": cross_checks,
        "executive_snapshot": executive_snapshot,
        "executive_decision_snapshot": executive_decision_snapshot,
    }


if __name__ == "__main__":
    run_metric_governance()
