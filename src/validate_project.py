from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
import json
import re

import numpy as np
import pandas as pd

from .config import DATA_PROCESSED_DIR, DATA_RAW_DIR, OUTPUT_DASHBOARD_DIR, OUTPUT_REPORTS_DIR, PROJECT_ROOT
from .score_stability import ALLOWED_COMPARABILITY_TAGS

VALIDATION_DOMAINS = [
    "structural validity",
    "semantic validity",
    "analytical validity",
    "financial validity",
    "decision validity",
    "release validity",
    "committee-readiness validity",
]

READINESS_STATES = [
    "publish-blocked",
    "structurally-valid",
    "analytically-acceptable",
    "screening-grade-only",
    "decision-support-only",
    "not-committee-grade",
    "committee-candidate",
    "committee-grade",
]

SEVERITY_LEVELS = ["BLOCKER", "CRITICAL", "HIGH", "WARNING"]
USAGE_LEVEL_ORDER = {
    "screening-grade-only": 1,
    "decision-support-only": 2,
    "not-committee-grade": 3,
    "committee-candidate": 4,
    "committee-grade": 5,
}

WAIVER_COLUMNS = [
    "waiver_id",
    "issue_id",
    "motivo",
    "owner",
    "fecha_caducidad",
    "nivel_maximo_permitido_uso",
    "plan_cierre",
    "estado",
]

WAIVER_PATH = PROJECT_ROOT / "contracts" / "waivers" / "waivers_active.csv"


# ---------- Helpers ----------
def _status(ok_condition: bool, warn_condition: bool = False) -> str:
    if ok_condition:
        return "OK"
    if warn_condition:
        return "WARN"
    return "ERROR"


def _safe_ratio(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator else 0.0


def _usage_rank(level: str) -> int:
    return USAGE_LEVEL_ORDER.get(str(level), 0)


def evaluate_waiver_status(
    waiver_row: pd.Series,
    as_of: date,
    requested_usage_level: str,
) -> tuple[bool, str]:
    estado = str(waiver_row.get("estado", "")).strip().lower()
    if estado not in {"activo", "active"}:
        return False, "inactive"

    expiry_raw = waiver_row.get("fecha_caducidad")
    expiry = pd.to_datetime(expiry_raw, errors="coerce")
    if pd.isna(expiry):
        return False, "invalid_expiry"
    if expiry.date() < as_of:
        return False, "expired"

    allowed_level = str(waiver_row.get("nivel_maximo_permitido_uso", "")).strip()
    if _usage_rank(requested_usage_level) > _usage_rank(allowed_level):
        return False, "scope_exceeded"

    return True, "applied"


def determine_publish_decision(
    overall_readiness_level: str,
    unwaived_blockers: int,
    unwaived_critical: int,
) -> str:
    if unwaived_blockers > 0:
        return "no publish"
    if overall_readiness_level in {"committee-grade", "committee-candidate"} and unwaived_critical == 0:
        return "publish"
    return "publish with caveat"


def _ensure_waiver_contract() -> pd.DataFrame:
    WAIVER_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not WAIVER_PATH.exists():
        pd.DataFrame(columns=WAIVER_COLUMNS).to_csv(WAIVER_PATH, index=False)
    waivers = pd.read_csv(WAIVER_PATH)
    for col in WAIVER_COLUMNS:
        if col not in waivers.columns:
            waivers[col] = ""
    return waivers[WAIVER_COLUMNS].copy()


def _record_check(
    rows: list[dict[str, object]],
    domain: str,
    check_id: str,
    severity: str,
    status: str,
    observed: object,
    expected: object,
    detail: str,
    risk_type: str,
    evidence_grade: str,
) -> None:
    rows.append(
        {
            "domain": domain,
            "check": check_id,
            "severity": severity,
            "status": status,
            "observed": observed,
            "expected": expected,
            "detail": detail,
            "risk_type": risk_type,
            "evidence_grade": evidence_grade,
            "issue_id": f"ISS_{check_id}",
        }
    )


def _domain_state(domain: str, issues_domain: pd.DataFrame) -> str:
    if issues_domain.empty:
        if domain == "analytical validity":
            return "analytically-acceptable"
        if domain == "financial validity":
            return "committee-candidate"
        if domain == "decision validity":
            return "committee-candidate"
        if domain == "committee-readiness validity":
            return "committee-grade"
        return "structurally-valid"

    blocker_errors = int(((issues_domain["severity"] == "BLOCKER") & (issues_domain["effective_status"] == "ERROR")).sum())
    critical_open = int(((issues_domain["severity"] == "CRITICAL") & (issues_domain["effective_status"].isin(["ERROR", "WARN"]))).sum())
    high_open = int(((issues_domain["severity"] == "HIGH") & (issues_domain["effective_status"].isin(["ERROR", "WARN"]))).sum())
    warning_open = int(((issues_domain["severity"] == "WARNING") & (issues_domain["effective_status"].isin(["ERROR", "WARN"]))).sum())

    if blocker_errors > 0:
        return "publish-blocked"

    if domain in {"structural validity", "semantic validity", "release validity"}:
        return "structurally-valid"
    if domain == "analytical validity":
        return "analytically-acceptable"
    if domain == "financial validity":
        if critical_open > 0 or high_open > 0:
            return "screening-grade-only"
        return "committee-candidate"
    if domain == "decision validity":
        if critical_open > 0 or high_open > 0:
            return "decision-support-only"
        return "committee-candidate"
    if domain == "committee-readiness validity":
        if critical_open > 0 or high_open > 0:
            return "not-committee-grade"
        if warning_open > 0:
            return "committee-candidate"
        return "committee-grade"

    return "analytically-acceptable"


def _overall_readiness(domain_state_map: dict[str, str]) -> str:
    technical_domains = ["structural validity", "semantic validity", "release validity"]
    if any(domain_state_map.get(dom) == "publish-blocked" for dom in technical_domains):
        return "publish-blocked"

    committee_state = domain_state_map.get("committee-readiness validity", "not-committee-grade")
    financial_state = domain_state_map.get("financial validity", "screening-grade-only")
    decision_state = domain_state_map.get("decision validity", "decision-support-only")
    analytical_state = domain_state_map.get("analytical validity", "analytically-acceptable")

    if committee_state == "committee-grade" and financial_state == "committee-candidate" and decision_state == "committee-candidate":
        return "committee-grade"
    if committee_state == "committee-candidate" and financial_state in {"committee-candidate", "screening-grade-only"} and decision_state in {"committee-candidate", "decision-support-only"}:
        return "committee-candidate"
    if committee_state == "not-committee-grade":
        return "not-committee-grade"
    if decision_state == "decision-support-only":
        return "decision-support-only"
    if financial_state == "screening-grade-only":
        return "screening-grade-only"
    if analytical_state == "analytically-acceptable":
        return "analytically-acceptable"
    return "structurally-valid"


def _effective_committee_state(
    overall_readiness: str,
    committee_domain_state: str,
    financial_state: str,
    decision_state: str,
) -> str:
    # Committee readiness is only valid if decision + financial maturity also supports it.
    if overall_readiness == "committee-grade":
        return "committee-grade"
    if overall_readiness == "committee-candidate":
        return "committee-candidate"

    # Any lower maturity forces not-committee-grade regardless of local domain score.
    if overall_readiness in {
        "publish-blocked",
        "structurally-valid",
        "analytically-acceptable",
        "screening-grade-only",
        "decision-support-only",
        "not-committee-grade",
    }:
        return "not-committee-grade"

    # Defensive fallback for unknown states.
    if committee_domain_state in {"committee-grade", "committee-candidate"} and (
        financial_state in {"screening-grade-only"} or decision_state in {"decision-support-only"}
    ):
        return "not-committee-grade"
    return committee_domain_state


def _evidence_grade(unwaived_blockers: int, unwaived_critical: int, unwaived_high: int) -> str:
    if unwaived_blockers > 0:
        return "D"
    if unwaived_critical > 0:
        return "C"
    if unwaived_high > 0:
        return "B"
    return "A"


def _top_open_risks(issues_df: pd.DataFrame, n: int = 8) -> pd.DataFrame:
    if issues_df.empty:
        return pd.DataFrame(columns=["issue_id", "domain", "severity", "check", "effective_status", "detail", "waived_flag"])

    severity_rank = {"BLOCKER": 4, "CRITICAL": 3, "HIGH": 2, "WARNING": 1}
    tmp = issues_df.copy()
    tmp["severity_rank"] = tmp["severity"].map(severity_rank).fillna(0)
    tmp["open_rank"] = tmp["effective_status"].map({"ERROR": 3, "WARN": 2, "WAIVED": 1}).fillna(0)
    tmp = tmp.sort_values(["open_rank", "severity_rank"], ascending=False)
    keep = ["issue_id", "domain", "severity", "check", "effective_status", "detail", "waived_flag", "waiver_id"]
    return tmp[keep].head(n).reset_index(drop=True)


# ---------- Main validation ----------
def run_full_validation() -> dict[str, pd.DataFrame]:
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    # Inputs
    energy = pd.read_csv(DATA_RAW_DIR / "consumos_energia_horarios.csv")
    production = pd.read_csv(DATA_RAW_DIR / "produccion_turno.csv")
    emissions = pd.read_csv(DATA_RAW_DIR / "emisiones_estimadas.csv")

    line_day = pd.read_csv(DATA_PROCESSED_DIR / "line_day_features_scored.csv")
    line_scores = pd.read_csv(DATA_PROCESSED_DIR / "line_criticality_scores.csv")
    equipment = pd.read_csv(DATA_PROCESSED_DIR / "equipment_energy_anomaly_scores.csv")
    scenario = pd.read_csv(DATA_PROCESSED_DIR / "scenario_table.csv")
    scenario_rank = pd.read_csv(DATA_PROCESSED_DIR / "scenario_ranking.csv")
    invest = pd.read_csv(DATA_PROCESSED_DIR / "investment_prioritization_final.csv")
    sensitivity = pd.read_csv(DATA_PROCESSED_DIR / "investment_prioritization_sensitivity.csv")
    portfolio_summary = pd.read_csv(DATA_PROCESSED_DIR / "portfolio_summary.csv")
    metric_contract = pd.read_csv(DATA_PROCESSED_DIR / "metric_contract_validation.csv")
    cross_output = pd.read_csv(DATA_PROCESSED_DIR / "cross_output_consistency.csv")
    validation_sql = pd.read_csv(DATA_PROCESSED_DIR / "validation_sql_checks.csv")
    snapshot = pd.read_csv(DATA_PROCESSED_DIR / "executive_kpi_snapshot.csv")
    decision_snapshot = pd.read_csv(DATA_PROCESSED_DIR / "executive_decision_snapshot.csv")
    score_stability = pd.read_csv(DATA_PROCESSED_DIR / "score_stability_before_after.csv")
    score_ranking = pd.read_csv(DATA_PROCESSED_DIR / "score_ranking_robustness_before_after.csv")
    score_comparability = pd.read_csv(DATA_PROCESSED_DIR / "score_comparability_registry.csv")
    dashboard_audit = pd.read_csv(DATA_PROCESSED_DIR / "dashboard_serving_audit.csv")
    optimization_meta_path = DATA_PROCESSED_DIR / "portfolio_optimization_metadata.csv"
    optimization_meta = pd.read_csv(optimization_meta_path) if optimization_meta_path.exists() else pd.DataFrame()

    dashboard_path = OUTPUT_DASHBOARD_DIR / "industrial-operating-command-center.html"
    dashboard_dataset_path = DATA_PROCESSED_DIR / "dashboard_canonical_dataset.json"
    legacy_dashboard_paths = [
        OUTPUT_DASHBOARD_DIR / "dashboard_executive_light.html",
        OUTPUT_DASHBOARD_DIR / "dashboard_full_offline.html",
        OUTPUT_DASHBOARD_DIR / "dashboard_board_pack.html",
    ]
    dashboard_text = dashboard_path.read_text(encoding="utf-8") if dashboard_path.exists() else ""
    dashboard_dataset = json.loads(dashboard_dataset_path.read_text(encoding="utf-8")) if dashboard_dataset_path.exists() else {}
    readiness_prev_path = DATA_PROCESSED_DIR / "validation_readiness_summary.csv"
    readiness_prev = pd.read_csv(readiness_prev_path) if readiness_prev_path.exists() else pd.DataFrame()

    advanced_report_path = OUTPUT_REPORTS_DIR / "advanced_analysis_report.md"
    advanced_report_text = advanced_report_path.read_text(encoding="utf-8") if advanced_report_path.exists() else ""

    readme_path = PROJECT_ROOT / "README.md"
    readme_text = readme_path.read_text(encoding="utf-8") if readme_path.exists() else ""

    checks: list[dict[str, object]] = []

    # -------- Structural validity --------
    min_rows = {
        "energy_rows_min": (len(energy), 100000),
        "production_rows_min": (len(production), 10000),
        "line_day_rows_min": (len(line_day), 1000),
        "equipment_rows_min": (len(equipment), 10000),
        "scenario_rows_min": (len(scenario), 100),
    }
    for check_id, (obs, exp) in min_rows.items():
        _record_check(
            checks,
            "structural validity",
            check_id,
            "BLOCKER",
            _status(obs >= exp),
            obs,
            f">={exp}",
            "Volumen por debajo del umbral mínimo.",
            "rowcount",
            "A",
        )

    dup_energy = int(energy.duplicated(subset=["timestamp", "planta_id", "linea_id", "equipo_id", "fuente_energia"]).sum())
    _record_check(
        checks,
        "structural validity",
        "energy_grain_duplicates",
        "BLOCKER",
        _status(dup_energy == 0),
        dup_energy,
        0,
        "Duplicados en grano equipo-hora-fuente.",
        "grain",
        "A",
    )

    null_keys = int(line_day[["fecha", "planta_id", "linea_id"]].isna().sum().sum())
    _record_check(
        checks,
        "structural validity",
        "line_day_key_nulls",
        "BLOCKER",
        _status(null_keys == 0),
        null_keys,
        0,
        "Nulos en claves críticas de línea-día.",
        "nullability",
        "A",
    )

    temporal_nan = int(pd.to_datetime(energy["timestamp"], errors="coerce").isna().sum())
    _record_check(
        checks,
        "structural validity",
        "energy_temporal_parse",
        "BLOCKER",
        _status(temporal_nan == 0),
        temporal_nan,
        0,
        "Timestamp inválido en energía.",
        "temporal",
        "A",
    )

    # -------- Semantic validity --------
    contract_errors = int((metric_contract["status"] == "ERROR").sum())
    _record_check(
        checks,
        "semantic validity",
        "metric_contract_errors",
        "BLOCKER",
        _status(contract_errors == 0),
        contract_errors,
        0,
        "Contrato de datos/métricas con errores.",
        "metric-contract",
        "A",
    )

    if "tier" not in cross_output.columns:
        cross_output["tier"] = "CRITICAL"
    co_blocker_errors = int(((cross_output["tier"] == "BLOCKER") & (cross_output["status"] == "ERROR")).sum())
    co_critical_non_ok = int(
        ((cross_output["tier"] == "CRITICAL") & (cross_output["status"].isin(["ERROR", "WARN"]))).sum()
    )
    co_errors = int((cross_output["status"] == "ERROR").sum())
    co_warns = int((cross_output["status"] == "WARN").sum())
    co_semantic_ok = co_blocker_errors == 0 and co_critical_non_ok == 0
    co_semantic_warn = (co_blocker_errors == 0) and (co_critical_non_ok > 0 or co_warns > 0)
    _record_check(
        checks,
        "semantic validity",
        "cross_output_consistency",
        "CRITICAL",
        _status(co_semantic_ok, warn_condition=co_semantic_warn),
        f"blocker_errors={co_blocker_errors};critical_non_ok={co_critical_non_ok};errors={co_errors};warns={co_warns}",
        "blocker_errors=0;critical_non_ok=0",
        "Inconsistencia SQL/Python/serving.",
        "semantic-drift",
        "A",
    )

    snapshot_line = {
        "production_volume": float(line_day["production_volume"].sum()),
        "oee_synthetic": float(line_day["oee_synthetic"].mean()),
        "sec": float(line_day["sec"].mean()),
        "loss_value_proxy": float(line_day["loss_value_proxy"].sum()),
    }
    deltas = [
        abs(float(snapshot.iloc[0][k]) - v)
        for k, v in snapshot_line.items()
    ]
    max_delta = float(max(deltas))
    _record_check(
        checks,
        "semantic validity",
        "snapshot_metric_drift",
        "CRITICAL",
        _status(max_delta <= 1e-6, warn_condition=max_delta <= 1e-3),
        max_delta,
        "<=1e-6",
        "Drift entre snapshot ejecutivo y cálculo canónico.",
        "metric-drift",
        "A",
    )

    invalid_comparability_tags = int((~score_comparability["comparability_tag"].isin(ALLOWED_COMPARABILITY_TAGS)).sum())
    _record_check(
        checks,
        "semantic validity",
        "score_comparability_tags_valid",
        "CRITICAL",
        _status(invalid_comparability_tags == 0),
        invalid_comparability_tags,
        0,
        "Tags de comparabilidad de scores fuera de taxonomía gobernada.",
        "semantic-drift",
        "A",
    )

    # -------- Analytical validity --------
    sec_out = int(((line_day["sec"] < 50) | (line_day["sec"] > 3000)).sum())
    oee_out = int(((line_day["oee_synthetic"] < 0) | (line_day["oee_synthetic"] > 1.2)).sum())
    _record_check(
        checks,
        "analytical validity",
        "sec_range",
        "CRITICAL",
        _status(sec_out == 0),
        sec_out,
        0,
        "SEC fuera de rango razonable.",
        "plausibility",
        "A",
    )
    _record_check(
        checks,
        "analytical validity",
        "oee_range",
        "CRITICAL",
        _status(oee_out == 0),
        oee_out,
        0,
        "OEE fuera de rango razonable.",
        "plausibility",
        "A",
    )

    recomposed = (
        0.20 * invest["energy_saving_score"]
        + 0.18 * invest["operational_impact_score"]
        + 0.10 * invest["emissions_reduction_score"]
        + 0.14 * invest["implementation_feasibility_score"]
        + 0.12 * invest["payback_score"]
        + 0.10 * invest["strategic_priority_score"]
        + 0.10 * invest["financial_resilience_score"]
        + 0.06 * invest["uncertainty_index"]
    )
    formula_delta = float((recomposed - invest["improvement_priority_index"]).abs().max())
    _record_check(
        checks,
        "analytical validity",
        "priority_formula_alignment",
        "CRITICAL",
        _status(formula_delta <= 1e-8),
        formula_delta,
        "<=1e-8",
        "Fórmula del score no reconciliada.",
        "score-stability",
        "A",
    )

    base_top = set(
        sensitivity[sensitivity["scenario"] == "base"]
        .sort_values("ranking")
        .head(10)["iniciativa_id"]
        .tolist()
    )
    energy_top = set(
        sensitivity[sensitivity["scenario"] == "energia_alta"]
        .sort_values("ranking")
        .head(10)["iniciativa_id"]
        .tolist()
    )
    overlap = len(base_top & energy_top) / 10.0 if base_top else 0.0
    _record_check(
        checks,
        "analytical validity",
        "score_stability_sensitivity_overlap",
        "HIGH",
        _status(overlap >= 0.70, warn_condition=overlap >= 0.50),
        overlap,
        ">=0.70",
        "Inestabilidad de ranking entre escenarios de sensibilidad.",
        "score-instability",
        "B",
    )

    anchored_worse_count = int(
        (
            score_stability["anchored_outlier_median_abs_shift"]
            > score_stability["legacy_outlier_median_abs_shift"] + 1e-9
        ).sum()
    )
    _record_check(
        checks,
        "analytical validity",
        "anchored_outlier_stability_vs_legacy",
        "HIGH",
        _status(anchored_worse_count == 0, warn_condition=anchored_worse_count > 0),
        anchored_worse_count,
        0,
        "La escala anclada no mejora estabilidad frente a outliers en todos los scores.",
        "score-instability",
        "B",
    )

    ranking_worse_count = int(
        (
            score_ranking["anchored_top10_overlap_after_outlier"]
            < score_ranking["legacy_top10_overlap_after_outlier"]
        ).sum()
    )
    _record_check(
        checks,
        "analytical validity",
        "anchored_ranking_robustness_vs_legacy",
        "HIGH",
        _status(ranking_worse_count == 0, warn_condition=ranking_worse_count > 0),
        ranking_worse_count,
        0,
        "La robustez de ranking anclada no supera baseline legacy bajo shock de outlier.",
        "score-instability",
        "B",
    )

    # -------- Financial validity --------
    emissions_ratio = emissions["emisiones_co2e_kg"] / emissions["consumo_kwh"].replace(0, np.nan)
    incons_em = int((np.abs(emissions_ratio - emissions["factor_emision_proxy"]) > 0.02).sum())
    _record_check(
        checks,
        "financial validity",
        "emissions_consistency",
        "CRITICAL",
        _status(incons_em == 0),
        incons_em,
        0,
        "Emisiones inconsistentes con consumo/factor.",
        "financial-reconciliation",
        "A",
    )

    scenario_base = scenario[scenario["macro_scenario"].str.lower() == "base"]
    if scenario_base.empty:
        scenario_base = scenario
    scen_i = scenario_base.groupby("iniciativa_id", as_index=False)["risk_adjusted_annual_savings"].mean()
    inv_i = invest[["iniciativa_id", "annual_saving_proxy"]]
    merged = scen_i.merge(inv_i, on="iniciativa_id", how="inner")
    save_delta = float(np.abs(merged["risk_adjusted_annual_savings"] - merged["annual_saving_proxy"]).max()) if not merged.empty else 0.0
    _record_check(
        checks,
        "financial validity",
        "scenario_invest_alignment",
        "BLOCKER",
        _status(save_delta <= 1e-6, warn_condition=save_delta <= 1e3),
        save_delta,
        "<=1e-6",
        "Ahorro desalineado entre scenario engine y priorización.",
        "financial-reconciliation",
        "A",
    )

    var_invalid = int((scenario["value_at_risk_95"] > scenario["risk_adjusted_annual_savings"] + 1e-6).sum())
    downside_invalid = int((scenario["downside_annual_savings_p20"] > scenario["risk_adjusted_annual_savings"] + 1e-6).sum())
    _record_check(
        checks,
        "financial validity",
        "var_downside_bounds",
        "CRITICAL",
        _status(var_invalid == 0 and downside_invalid == 0),
        f"var_invalid={var_invalid};downside_invalid={downside_invalid}",
        "var_invalid=0;downside_invalid=0",
        "VaR/Downside fuera de límites lógicos.",
        "financial-reconciliation",
        "A",
    )

    payback_bad = int(((invest["payback_months"] < 1) | (invest["payback_months"] > 180)).sum())
    _record_check(
        checks,
        "financial validity",
        "payback_range",
        "HIGH",
        _status(payback_bad == 0),
        payback_bad,
        0,
        "Payback fuera de rango operativo-financiero.",
        "financial-weakness",
        "B",
    )

    # Prevención de doble conteo: avoided_loss no puede exceder la exposición no energética base.
    if {"avoided_loss", "baseline_non_energy_exposure"}.issubset(set(scenario.columns)):
        double_count = int((scenario["avoided_loss"] > scenario["baseline_non_energy_exposure"] + 1e-6).sum())
    else:
        double_count = 0
    _record_check(
        checks,
        "financial validity",
        "double_counting_prevention",
        "CRITICAL",
        _status(double_count == 0),
        double_count,
        0,
        "Detección de doble conteo entre avoided loss y exposición base no energética.",
        "financial-reconciliation",
        "A",
    )

    # Lógica de signos financieros: gross >= net_operational >= net_captured >= downside_adjusted.
    if {"gross_technical_value", "net_operational_value", "net_captured_value", "downside_adjusted_annual"}.issubset(set(scenario.columns)):
        sign_invalid = int(
            (
                (scenario["gross_technical_value"] < scenario["net_operational_value"] - 1e-6)
                | (scenario["net_operational_value"] < scenario["net_captured_value"] - 1e-6)
                | (scenario["net_captured_value"] < scenario["downside_adjusted_annual"] - 1e-6)
                | (scenario["downside_adjusted_annual"] < -1e-6)
            ).sum()
        )
    else:
        sign_invalid = 0
    _record_check(
        checks,
        "financial validity",
        "financial_sign_logic",
        "CRITICAL",
        _status(sign_invalid == 0),
        sign_invalid,
        0,
        "Inconsistencia de lógica de signos en cadena de valor financiero.",
        "financial-weakness",
        "A",
    )

    # Consistencia de payback descontado: NPV positivo con payback no extremo.
    if {"npv_risk_adjusted", "payback_months"}.issubset(set(invest.columns)):
        payback_consistency_invalid = int(((invest["npv_risk_adjusted"] > 0) & (invest["payback_months"] > 120)).sum())
    else:
        payback_consistency_invalid = 0
    _record_check(
        checks,
        "financial validity",
        "discounted_payback_consistency",
        "HIGH",
        _status(payback_consistency_invalid == 0),
        payback_consistency_invalid,
        0,
        "NPV positivo con payback descontado inconsistente.",
        "financial-weakness",
        "B",
    )

    # Sensibilidad explícita: Shock energía debe subir valor energético medio vs Base.
    if {"macro_scenario", "gross_technical_value_energy"}.issubset(set(scenario.columns)):
        by_macro_energy = scenario.groupby("macro_scenario", as_index=False)["gross_technical_value_energy"].mean()
        map_energy = {row["macro_scenario"]: row["gross_technical_value_energy"] for _, row in by_macro_energy.iterrows()}
        base_energy = float(map_energy.get("Base", np.nan))
        shock_energy = float(map_energy.get("ShockEnergia", np.nan))
        sens_ratio = _safe_ratio(shock_energy, base_energy) if base_energy and not np.isnan(base_energy) else np.nan
        sensitivity_ok = bool(not np.isnan(sens_ratio) and sens_ratio >= 1.05)
    else:
        sensitivity_ok = True
        sens_ratio = 1.0
    _record_check(
        checks,
        "financial validity",
        "scenario_sensitivity_consistency",
        "HIGH",
        _status(sensitivity_ok, warn_condition=not sensitivity_ok),
        sens_ratio,
        ">=1.05",
        "Sensibilidad financiera a precio energético no consistente con diseño del escenario.",
        "financial-weakness",
        "B",
    )

    # Madurez financiera vs caveat de comité.
    if {"financial_maturity_stage", "committee_claim_allowed_flag"}.issubset(set(invest.columns)):
        maturity_mismatch = int(
            (
                (invest["financial_maturity_stage"].isin(["screening", "pre_feasibility"]))
                & (invest["committee_claim_allowed_flag"] == 1)
            ).sum()
        )
    else:
        maturity_mismatch = 0
    _record_check(
        checks,
        "financial validity",
        "financial_maturity_claim_consistency",
        "CRITICAL",
        _status(maturity_mismatch == 0),
        maturity_mismatch,
        0,
        "Claims de comité no alineados con madurez financiera real.",
        "overclaim-risk",
        "A",
    )

    # Riesgo explícito de proxy financiero
    _record_check(
        checks,
        "financial validity",
        "financial_proxy_risk",
        "HIGH",
        "WARN",
        "IRR/VaR y pérdidas usan proxies declarados",
        "Modelo financiero auditado corporativamente",
        "Modelo financiero aún proxy: no apto para committee-grade sin hardening financiero.",
        "proxy-risk",
        "C",
    )

    # -------- Decision validity --------
    capex_ok = float(portfolio_summary["capex_selected"].iloc[0]) <= float(portfolio_summary["capex_budget"].iloc[0]) + 1e-6
    opex_ok = float(portfolio_summary["opex_selected"].iloc[0]) <= float(portfolio_summary["opex_budget"].iloc[0]) + 1e-6
    _record_check(
        checks,
        "decision validity",
        "portfolio_budget_constraints",
        "BLOCKER",
        _status(capex_ok and opex_ok),
        f"capex_ok={capex_ok};opex_ok={opex_ok}",
        "capex_ok=True;opex_ok=True",
        "Cartera fuera de restricciones CAPEX/OPEX.",
        "decision-integrity",
        "A",
    )

    deps_path = DATA_PROCESSED_DIR / "scenario_dependencies.csv"
    deps = pd.read_csv(deps_path) if deps_path.exists() else pd.DataFrame(columns=["iniciativa_id", "depends_on_iniciativa_id"])
    dep_unsatisfied = 0
    if not deps.empty:
        child = deps.merge(invest[["iniciativa_id", "selected_portfolio_flag"]], on="iniciativa_id", how="left").rename(columns={"selected_portfolio_flag": "child_selected"})
        dep = invest[["iniciativa_id", "selected_portfolio_flag"]].rename(columns={"iniciativa_id": "depends_on_iniciativa_id", "selected_portfolio_flag": "dep_selected"})
        depm = child.merge(dep, on="depends_on_iniciativa_id", how="left")
        dep_unsatisfied = int(((depm["child_selected"] == 1) & (depm["dep_selected"] != 1)).sum())
    _record_check(
        checks,
        "decision validity",
        "portfolio_dependency_consistency",
        "CRITICAL",
        _status(dep_unsatisfied == 0),
        dep_unsatisfied,
        0,
        "Dependencias de iniciativas no satisfechas en cartera seleccionada.",
        "decision-integrity",
        "A",
    )

    selected_discarded = int(((invest["selected_portfolio_flag"] == 1) & (invest["decision_rule"] == "descartar")).sum())
    _record_check(
        checks,
        "decision validity",
        "selected_vs_decision_rule_consistency",
        "CRITICAL",
        _status(selected_discarded == 0),
        selected_discarded,
        0,
        "Iniciativas seleccionadas con regla de descarte.",
        "decision-integrity",
        "A",
    )

    optimization_solution_exists = (DATA_PROCESSED_DIR / "portfolio_optimization_solution.csv").exists()
    optimization_meta_exists = not optimization_meta.empty
    opt_status = str(optimization_meta.iloc[0]["optimization_status"]) if optimization_meta_exists else "missing"
    opt_degree = str(optimization_meta.iloc[0]["optimality_degree"]) if optimization_meta_exists else "missing"
    opt_timed_out = int(optimization_meta.iloc[0]["timed_out_flag"]) if optimization_meta_exists else -1
    opt_feasible = int(optimization_meta.iloc[0]["feasible_solution_found"]) if optimization_meta_exists else -1
    allowed_status = {
        "solved_exact",
        "time_limited_best_feasible",
        "infeasible_forced_constraints",
        "infeasible_no_feasible_solution",
    }
    status_ok = opt_status in allowed_status
    consistency_ok = (
        (opt_status == "solved_exact" and opt_degree == "exact_for_formulation" and opt_timed_out == 0 and opt_feasible == 1)
        or (opt_status == "time_limited_best_feasible" and opt_degree == "semi_formal_approximation" and opt_timed_out == 1 and opt_feasible == 1)
        or (opt_status in {"infeasible_forced_constraints", "infeasible_no_feasible_solution"} and opt_degree == "no-feasible-formulation")
    )
    if not consistency_ok:
        status_consistency = "ERROR"
    elif opt_status == "solved_exact":
        status_consistency = "OK"
    elif opt_status == "time_limited_best_feasible":
        status_consistency = "WARN"
    else:
        status_consistency = "ERROR"

    _record_check(
        checks,
        "decision validity",
        "optimization_formalism_weakness",
        "HIGH",
        _status(optimization_solution_exists and optimization_meta_exists and status_ok, warn_condition=optimization_solution_exists and not (optimization_meta_exists and status_ok)),
        f"solution={int(optimization_solution_exists)};meta={int(optimization_meta_exists)};status={opt_status}",
        "solution=1;meta=1;status in allowed_status",
        "Selección de cartera sin evidencia suficiente de formulación de optimización.",
        "optimization-weakness",
        "B",
    )

    _record_check(
        checks,
        "decision validity",
        "optimization_status_consistency",
        "CRITICAL",
        status_consistency,
        f"status={opt_status};degree={opt_degree};timed_out={opt_timed_out};feasible={opt_feasible}",
        "consistent metadata by status",
        "Inconsistencia entre estado del optimizador y claim de optimalidad.",
        "optimization-weakness",
        "A",
    )

    # -------- Release validity --------
    required_outputs = [
        DATA_PROCESSED_DIR / "executive_kpi_snapshot.csv",
        DATA_PROCESSED_DIR / "executive_decision_snapshot.csv",
        DATA_PROCESSED_DIR / "scenario_table.csv",
        DATA_PROCESSED_DIR / "investment_prioritization_final.csv",
        DATA_PROCESSED_DIR / "portfolio_summary.csv",
        DATA_PROCESSED_DIR / "portfolio_optimization_metadata.csv",
        DATA_PROCESSED_DIR / "portfolio_selected_vs_excluded.csv",
        DATA_PROCESSED_DIR / "portfolio_constraint_pressure_summary.csv",
        DATA_PROCESSED_DIR / "portfolio_unlocked_value_analysis.csv",
        DATA_PROCESSED_DIR / "portfolio_wave_logic_explained.csv",
        DATA_PROCESSED_DIR / "score_stability_before_after.csv",
        DATA_PROCESSED_DIR / "score_ranking_robustness_before_after.csv",
        DATA_PROCESSED_DIR / "score_comparability_registry.csv",
        DATA_PROCESSED_DIR / "dashboard_canonical_dataset.json",
        DATA_PROCESSED_DIR / "dashboard_serving_audit.csv",
        OUTPUT_REPORTS_DIR / "advanced_analysis_report.md",
        OUTPUT_REPORTS_DIR / "validation_report.md",
        dashboard_path,
    ]
    missing_outputs = [p.name for p in required_outputs if not p.exists()]
    _record_check(
        checks,
        "release validity",
        "release_artifact_completeness",
        "BLOCKER",
        _status(len(missing_outputs) == 0),
        "missing=" + ",".join(missing_outputs) if missing_outputs else "missing=none",
        "missing=none",
        "Artefactos incompletos para publicación/revisión.",
        "release-incompleteness",
        "A",
    )

    sections = [
        "Vista de Eficiencia Operativa",
        "Vista Energética y Coste",
        "Vista de Proceso y Causas Raíz",
        "Vista de Cartera y Riesgo",
        "Tabla Final Interactiva de Priorización",
        "Decisión Ejecutiva",
    ]
    has_sections = all(s in dashboard_text for s in sections)
    _record_check(
        checks,
        "release validity",
        "dashboard_sections_consistency",
        "CRITICAL",
        _status(dashboard_path.exists() and has_sections),
        int(dashboard_path.exists() and has_sections),
        1,
        "Dashboard no contiene secciones ejecutivas obligatorias.",
        "dashboard-report-inconsistency",
        "A",
    )

    stamp_ok = all(
        token in dashboard_text
        for token in [
            "dashboard_mode: canonical_single_html",
            "run_id:",
            "snapshot_id:",
            "metric_version_set:",
            "decision_model_version:",
            "validation_state:",
        ]
    )
    _record_check(
        checks,
        "release validity",
        "dashboard_version_stamping",
        "CRITICAL",
        _status(stamp_ok),
        int(stamp_ok),
        1,
        "Dashboard ejecutivo sin version stamping obligatorio.",
        "release-integrity",
        "A",
    )

    canonical_size = int(dashboard_path.stat().st_size) if dashboard_path.exists() else -1
    size_ok = canonical_size > 0 and canonical_size <= 4_000_000
    _record_check(
        checks,
        "release validity",
        "dashboard_payload_thresholds",
        "HIGH",
        _status(size_ok, warn_condition=canonical_size > 0),
        f"canonical={canonical_size}",
        "canonical<=4000000",
        "Tamaño del dashboard fuera de política de serving.",
        "release-incompleteness",
        "B",
    )

    audit_flags_ok = False
    if not dashboard_audit.empty:
        try:
            audit_map = dict(zip(dashboard_audit["metric"].astype(str), dashboard_audit["value"]))
            audit_flags_ok = int(audit_map.get("canonical_under_threshold_flag", 0)) == 1 and int(
                audit_map.get("no_competing_html_flag", 0)
            ) == 1
        except Exception:
            audit_flags_ok = False
    _record_check(
        checks,
        "release validity",
        "dashboard_serving_audit_consistency",
        "HIGH",
        _status(audit_flags_ok, warn_condition=not audit_flags_ok),
        int(audit_flags_ok),
        1,
        "Dashboard serving audit no consistente con thresholds.",
        "release-integrity",
        "B",
    )

    legacy_present = [p.name for p in legacy_dashboard_paths if p.exists()]
    _record_check(
        checks,
        "release validity",
        "dashboard_single_official_html_enforced",
        "CRITICAL",
        _status(len(legacy_present) == 0),
        "legacy=" + ",".join(legacy_present) if legacy_present else "legacy=none",
        "legacy=none",
        "Existen dashboards legacy compitiendo con el HTML oficial canónico.",
        "dashboard-report-inconsistency",
        "A",
    )

    expected_validation_state = ""
    if not readiness_prev.empty and {"overall_readiness_level", "publish_decision"}.issubset(readiness_prev.columns):
        prev_row = readiness_prev.iloc[0]
        expected_validation_state = f"{prev_row.get('overall_readiness_level')}|{prev_row.get('publish_decision')}"

    observed_validation_state = str(dashboard_dataset.get("meta", {}).get("validation_state", ""))
    validation_state_ok = bool(observed_validation_state and "|" in observed_validation_state)
    if expected_validation_state:
        validation_state_ok = validation_state_ok and observed_validation_state == expected_validation_state

    dataset_stamp_ok = bool(
        isinstance(dashboard_dataset, dict)
        and str(dashboard_dataset.get("meta", {}).get("run_id", "")) == str(snapshot.iloc[0]["run_id"])
        and str(dashboard_dataset.get("meta", {}).get("metric_version_set", "")) == str(snapshot.iloc[0]["metric_version_set"])
        and validation_state_ok
    )
    _record_check(
        checks,
        "release validity",
        "dashboard_dataset_stamping_consistency",
        "CRITICAL",
        _status(dataset_stamp_ok),
        int(dataset_stamp_ok),
        1,
        "Dataset canónico del dashboard no alinea run/version/readiness con outputs oficiales.",
        "dashboard-report-inconsistency",
        "A",
    )

    chart_limits = {
        "sec_trend": 5,
        "oee_trend": 5,
        "energy_cost_line": 10,
        "equipment_anomaly": 10,
        "root_cause": 8,
        "shift_variance": 10,
        "scenario_value": 6,
        "portfolio_wave": 6,
        "priority_scatter": 35,
    }
    chart_density_over = 0
    charts_payload = dashboard_dataset.get("charts", {}) if isinstance(dashboard_dataset, dict) else {}
    for chart_key, limit in chart_limits.items():
        payload = charts_payload.get(chart_key, [])
        if chart_key in {"sec_trend", "oee_trend"} and isinstance(payload, dict):
            observed_n = len(payload.get("datasets", []))
        else:
            observed_n = len(payload) if isinstance(payload, list) else 0
        if observed_n > limit:
            chart_density_over += 1

    _record_check(
        checks,
        "release validity",
        "dashboard_density_control",
        "HIGH",
        _status(chart_density_over == 0, warn_condition=chart_density_over > 0),
        chart_density_over,
        0,
        "Gráficos demasiado densos para lectura ejecutiva rápida.",
        "dashboard-readability",
        "B",
    )

    decision_row = decision_snapshot.iloc[0] if not decision_snapshot.empty else pd.Series(dtype=object)
    canonical_top_line = str(decision_row.get("linea_prioritaria", ""))
    canonical_top_equipment = str(decision_row.get("equipo_prioritario", ""))
    report_mentions_top_line = str(canonical_top_line) in advanced_report_text if canonical_top_line else False
    report_mentions_top_equipment = str(canonical_top_equipment) in advanced_report_text if canonical_top_equipment else False
    _record_check(
        checks,
        "release validity",
        "report_data_consistency",
        "HIGH",
        _status(
            report_mentions_top_line and report_mentions_top_equipment,
            warn_condition=report_mentions_top_line or report_mentions_top_equipment,
        ),
        f"line={int(report_mentions_top_line)};equipment={int(report_mentions_top_equipment)}",
        "line=1;equipment=1",
        "Informe analítico no refleja línea/equipo prioritarios canónicos.",
        "dashboard-report-inconsistency",
        "B",
    )

    dashboard_mentions_top_line = str(canonical_top_line) in dashboard_text if canonical_top_line else False
    dashboard_mentions_top_equipment = str(canonical_top_equipment) in dashboard_text if canonical_top_equipment else False
    _record_check(
        checks,
        "release validity",
        "dashboard_decision_consistency",
        "CRITICAL",
        _status(dashboard_mentions_top_line and dashboard_mentions_top_equipment),
        f"line={int(dashboard_mentions_top_line)};equipment={int(dashboard_mentions_top_equipment)}",
        "line=1;equipment=1",
        "Dashboard ejecutivo no refleja línea/equipo prioritarios del snapshot canónico.",
        "dashboard-report-inconsistency",
        "A",
    )

    readme_expected_tokens = [
        f"**{float(snapshot.iloc[0]['annual_saving_proxy']):,.2f} EUR/año**",
        f"**{float(snapshot.iloc[0]['portfolio_npv_risk_adjusted']):,.2f} EUR**",
        f"**{str(canonical_top_line)}**",
        f"**{str(canonical_top_equipment)}**",
    ]
    readme_sync_ok = all(token in readme_text for token in readme_expected_tokens)
    _record_check(
        checks,
        "release validity",
        "readme_runtime_claims_sync",
        "HIGH",
        _status(readme_sync_ok, warn_condition=not readme_sync_ok),
        int(readme_sync_ok),
        1,
        "README no está sincronizado con snapshot canónico de KPIs y decisión.",
        "dashboard-report-inconsistency",
        "B",
    )

    sql_errors = int((validation_sql["status"] != "OK").sum())
    _record_check(
        checks,
        "release validity",
        "sql_validation_consistency",
        "CRITICAL",
        _status(sql_errors == 0),
        sql_errors,
        0,
        "Errores en checks SQL de validación.",
        "release-integrity",
        "A",
    )

    # -------- Committee-readiness validity --------
    claim_patterns = [
        r"committee-grade",
        r"comit[eé] de inversi[oó]n",
        r"cockpit de comit[eé]",
        r"herramienta de comit[eé]",
        r"defendible ante comit[eé]",
    ]
    claim_hits = 0
    for pat in claim_patterns:
        claim_hits += len(re.findall(pat, readme_text, flags=re.IGNORECASE))

    weak_evidence_flags = {
        "financial_proxy_risk": True,
        "optimization_formalism_weakness": opt_status != "solved_exact",
    }
    overclaim = claim_hits > 0 and any(weak_evidence_flags.values())
    _record_check(
        checks,
        "committee-readiness validity",
        "overclaim_risk",
        "CRITICAL",
        _status(not overclaim, warn_condition=overclaim),
        f"claim_hits={claim_hits}",
        "claim_hits=0 when weaknesses open",
        "Narrativa puede sobreafirmar readiness frente a evidencia metodológica.",
        "overclaim-risk",
        "C",
    )

    # readiness overstated vs evidence (pre-check)
    critical_open_proxy = int(sum(1 for v in weak_evidence_flags.values() if v))
    readiness_overstated = overclaim and critical_open_proxy > 0
    _record_check(
        checks,
        "committee-readiness validity",
        "readiness_overstated_vs_evidence",
        "HIGH",
        _status(not readiness_overstated, warn_condition=readiness_overstated),
        int(readiness_overstated),
        0,
        "Readiness potencialmente sobredeclarado frente a evidencia disponible.",
        "readiness-overstated",
        "C",
    )

    # Build check tables
    checks_df = pd.DataFrame(checks)

    # issues (pre-waiver)
    issues_df = checks_df[checks_df["status"] != "OK"].copy()
    if issues_df.empty:
        issues_df = pd.DataFrame(
            [
                {
                    "domain": "committee-readiness validity",
                    "check": "no_material_issues",
                    "severity": "WARNING",
                    "status": "OK",
                    "observed": 0,
                    "expected": 0,
                    "detail": "Sin issues materiales",
                    "risk_type": "none",
                    "evidence_grade": "A",
                    "issue_id": "ISS_no_material_issues",
                }
            ]
        )

    # Waivers / exceptions
    waivers = _ensure_waiver_contract()
    as_of = date.today()
    requested_usage_level = "committee-candidate"

    waiver_eval_rows: list[dict[str, object]] = []
    effective_rows: list[dict[str, object]] = []

    for _, issue in issues_df.iterrows():
        candidates = waivers[waivers["issue_id"].astype(str) == str(issue["issue_id"])].copy()
        applied = False
        applied_waiver_id = ""
        waiver_status = "no_waiver"

        if not candidates.empty:
            candidates = candidates.sort_values("fecha_caducidad", ascending=False)
            c = candidates.iloc[0]
            applied, waiver_status = evaluate_waiver_status(c, as_of=as_of, requested_usage_level=requested_usage_level)
            applied_waiver_id = str(c["waiver_id"]) if pd.notna(c["waiver_id"]) else ""

        effective_status = "WAIVED" if applied else str(issue["status"])

        effective_rows.append(
            {
                **issue.to_dict(),
                "waived_flag": int(applied),
                "waiver_id": applied_waiver_id,
                "waiver_status": waiver_status,
                "effective_status": effective_status,
            }
        )

    issues_effective_df = pd.DataFrame(effective_rows)
    if issues_effective_df.empty:
        issues_effective_df = pd.DataFrame(columns=list(issues_df.columns) + ["waived_flag", "waiver_id", "waiver_status", "effective_status"])

    # Evaluate all waivers to report expiry/inactive status visibility
    for _, w in waivers.iterrows():
        applied, waiver_status = evaluate_waiver_status(w, as_of=as_of, requested_usage_level=requested_usage_level)
        waiver_eval_rows.append(
            {
                **w.to_dict(),
                "evaluated_for_usage_level": requested_usage_level,
                "waiver_status": waiver_status,
                "applied_flag": int(applied),
            }
        )
    waivers_eval_df = pd.DataFrame(waiver_eval_rows)
    if waivers_eval_df.empty:
        waivers_eval_df = pd.DataFrame(columns=WAIVER_COLUMNS + ["evaluated_for_usage_level", "waiver_status", "applied_flag"])

    # Add explicit check for waiver expiry risk
    expired_waivers = int((waivers_eval_df.get("waiver_status", pd.Series(dtype=str)) == "expired").sum())
    _record_check(
        checks,
        "release validity",
        "waiver_expiry_control",
        "CRITICAL",
        _status(expired_waivers == 0, warn_condition=expired_waivers > 0),
        expired_waivers,
        0,
        "Waivers caducados detectados; no pueden sostener excepciones activas.",
        "waiver-expiry",
        "B",
    )
    checks_df = pd.DataFrame(checks)

    # Rebuild issues with added check
    issues_df = checks_df[checks_df["status"] != "OK"].copy()
    effective_rows = []
    for _, issue in issues_df.iterrows():
        candidates = waivers[waivers["issue_id"].astype(str) == str(issue["issue_id"])].copy()
        applied = False
        applied_waiver_id = ""
        waiver_status = "no_waiver"
        if not candidates.empty:
            candidates = candidates.sort_values("fecha_caducidad", ascending=False)
            c = candidates.iloc[0]
            applied, waiver_status = evaluate_waiver_status(c, as_of=as_of, requested_usage_level=requested_usage_level)
            applied_waiver_id = str(c["waiver_id"]) if pd.notna(c["waiver_id"]) else ""

        effective_rows.append(
            {
                **issue.to_dict(),
                "waived_flag": int(applied),
                "waiver_id": applied_waiver_id,
                "waiver_status": waiver_status,
                "effective_status": "WAIVED" if applied else str(issue["status"]),
            }
        )
    issues_effective_df = pd.DataFrame(effective_rows)

    # Domain assessment
    domain_rows: list[dict[str, object]] = []
    domain_state_map: dict[str, str] = {}

    for domain in VALIDATION_DOMAINS:
        dom_issues = issues_effective_df[issues_effective_df["domain"] == domain].copy()
        state = _domain_state(domain, dom_issues)
        domain_state_map[domain] = state

        blocker_errors = int(((dom_issues["severity"] == "BLOCKER") & (dom_issues["effective_status"] == "ERROR")).sum())
        critical_open = int(((dom_issues["severity"] == "CRITICAL") & (dom_issues["effective_status"].isin(["ERROR", "WARN"]))).sum())
        high_open = int(((dom_issues["severity"] == "HIGH") & (dom_issues["effective_status"].isin(["ERROR", "WARN"]))).sum())
        warning_open = int(((dom_issues["severity"] == "WARNING") & (dom_issues["effective_status"].isin(["ERROR", "WARN"]))).sum())
        waived_count = int((dom_issues["waived_flag"] == 1).sum()) if not dom_issues.empty else 0

        if blocker_errors > 0:
            evidence = "D"
        elif critical_open > 0:
            evidence = "C"
        elif high_open > 0:
            evidence = "B"
        else:
            evidence = "A"

        domain_rows.append(
            {
                "domain": domain,
                "state": state,
                "open_blocker": blocker_errors,
                "open_critical": critical_open,
                "open_high": high_open,
                "open_warning": warning_open,
                "waived_issues": waived_count,
                "evidence_grade": evidence,
            }
        )

    domain_df = pd.DataFrame(domain_rows)

    # Overall readiness
    unwaived_blockers = int(((issues_effective_df["severity"] == "BLOCKER") & (issues_effective_df["effective_status"] == "ERROR")).sum())
    unwaived_critical = int(
        ((issues_effective_df["severity"] == "CRITICAL") & (issues_effective_df["effective_status"].isin(["ERROR", "WARN"]))).sum()
    )
    unwaived_high = int(
        ((issues_effective_df["severity"] == "HIGH") & (issues_effective_df["effective_status"].isin(["ERROR", "WARN"]))).sum()
    )

    overall = _overall_readiness(domain_state_map)
    evidence_grade = _evidence_grade(unwaived_blockers=unwaived_blockers, unwaived_critical=unwaived_critical, unwaived_high=unwaived_high)

    technical_state = "publish-blocked" if any(domain_state_map.get(d) == "publish-blocked" for d in ["structural validity", "semantic validity", "release validity"]) else "structurally-valid"
    decision_state = domain_state_map.get("decision validity", "decision-support-only")
    committee_state_domain = domain_state_map.get("committee-readiness validity", "not-committee-grade")
    committee_state = _effective_committee_state(
        overall_readiness=overall,
        committee_domain_state=committee_state_domain,
        financial_state=domain_state_map.get("financial validity", "screening-grade-only"),
        decision_state=decision_state,
    )

    committee_state_alignment_ok = committee_state == committee_state_domain or overall in {"committee-candidate", "committee-grade"}
    _record_check(
        checks,
        "committee-readiness validity",
        "committee_state_alignment",
        "CRITICAL",
        _status(committee_state_alignment_ok, warn_condition=not committee_state_alignment_ok),
        f"domain={committee_state_domain};effective={committee_state};overall={overall}",
        "domain/effective aligned or explicitly downgraded by overall readiness",
        "Evita falsa confianza cuando committee-readiness local contradice readiness global.",
        "readiness-overstated",
        "A",
    )
    checks_df = pd.DataFrame(checks)

    # Ensure domain table reflects effective committee readiness used in final release posture.
    if not domain_df.empty:
        mask = domain_df["domain"] == "committee-readiness validity"
        if mask.any():
            domain_df.loc[mask, "state"] = committee_state
            domain_df.loc[mask, "state_effective_from"] = "effective_committee_state"

    publish_decision = determine_publish_decision(
        overall_readiness_level=overall,
        unwaived_blockers=unwaived_blockers,
        unwaived_critical=unwaived_critical,
    )

    readiness_overstated_flag = int(
        ("committee" in overall and overall not in {"committee-candidate", "committee-grade"})
    )

    readiness_summary = pd.DataFrame(
        [
            {
                "validation_timestamp_utc": datetime.utcnow().isoformat(timespec="seconds"),
                "technical_state": technical_state,
                "decision_state": decision_state,
                "committee_readiness_state": committee_state,
                "committee_readiness_domain_state": committee_state_domain,
                "overall_readiness_level": overall,
                "evidence_grade": evidence_grade,
                "publish_decision": publish_decision,
                "unwaived_blockers": unwaived_blockers,
                "unwaived_critical": unwaived_critical,
                "unwaived_high": unwaived_high,
                "open_risks_count": int(len(issues_effective_df[issues_effective_df["effective_status"].isin(["ERROR", "WARN"])])),
                "readiness_overstated_flag": readiness_overstated_flag,
                "committee_presentation_allowed": int(
                    committee_state in {"committee-candidate", "committee-grade"}
                    and publish_decision == "publish"
                    and unwaived_blockers == 0
                ),
                "requested_usage_level_for_waivers": requested_usage_level,
            }
        ]
    )

    exceptions_active = issues_effective_df[
        (issues_effective_df["waived_flag"] == 1) & (issues_effective_df["effective_status"] == "WAIVED")
    ].copy()
    if exceptions_active.empty:
        exceptions_active = pd.DataFrame(columns=issues_effective_df.columns)

    top_risks_df = _top_open_risks(issues_effective_df)

    # Compatibility checklist output (legacy + hardened fields)
    checklist_df = checks_df[["check", "status", "observed", "expected", "domain", "severity", "risk_type", "evidence_grade", "issue_id"]].copy()

    # Fixes and caveats
    fixes_df = pd.DataFrame(
        [
            {"fix": "Taxonomía de dominios de validez implementada", "status": "aplicado"},
            {"fix": "Estados de readiness y decisión publish/no publish", "status": "aplicado"},
            {"fix": "Sistema de waivers con caducidad y scope", "status": "aplicado"},
            {"fix": "Controles anti-overclaim y committee-readiness", "status": "aplicado"},
        ]
    )

    caveats = [
        "El marco financiero sigue siendo proxy para screening/priorización: no sustituye un investment case corporativo auditado.",
        "La selección usa optimización branch-and-bound formal sobre la formulación definida; la claim de optimalidad depende de status exact vs time-limited.",
        "Los scores con tag contextual requieren comparar ventanas con contexto operativo equivalente; no todos los scores son benchmark absoluto cross-contexto.",
        "La estabilidad de scores está reforzada, pero la comparabilidad inter-release de largo horizonte requiere histórico adicional.",
    ]

    # Report markdown
    report_lines = [
        "# Validation Report (Hardened)",
        "",
        "## Objetivo",
        "Separar calidad técnica, metodológica, decisional y de comité para evitar falsa sensación de seguridad.",
        "",
        "## Estado de validación",
        f"- Estado técnico: **{technical_state}**",
        f"- Estado decisional: **{decision_state}**",
        f"- Committee readiness: **{committee_state}**",
        f"- Readiness global: **{overall}**",
        f"- Evidence grade: **{evidence_grade}**",
        f"- Decisión de publicación: **{publish_decision}**",
        "",
        "## Dominio por dominio",
        domain_df.to_markdown(index=False),
        "",
        "## Top riesgos abiertos",
        top_risks_df.to_markdown(index=False),
        "",
        "## Exceptions / waivers activos",
        (exceptions_active[["issue_id", "domain", "severity", "check", "waiver_id", "waiver_status", "detail"]].to_markdown(index=False) if not exceptions_active.empty else "Sin excepciones activas."),
        "",
        "## Waivers evaluados",
        (waivers_eval_df.to_markdown(index=False) if not waivers_eval_df.empty else "No hay waivers configurados."),
        "",
        "## Checklist endurecido",
        checklist_df.to_markdown(index=False),
        "",
        "## Fixes aplicadas",
        fixes_df.to_markdown(index=False),
        "",
        "## Caveats obligatorios",
    ]
    report_lines.extend([f"- {c}" for c in caveats])

    (OUTPUT_REPORTS_DIR / "validation_report.md").write_text("\n".join(report_lines), encoding="utf-8")

    # Persist outputs
    checks_df.to_csv(DATA_PROCESSED_DIR / "validation_checks_hardened.csv", index=False)
    checklist_df.to_csv(DATA_PROCESSED_DIR / "validation_checklist_final.csv", index=False)
    issues_effective_df.to_csv(DATA_PROCESSED_DIR / "validation_issues_found.csv", index=False)
    fixes_df.to_csv(DATA_PROCESSED_DIR / "validation_fixes_applied.csv", index=False)
    domain_df.to_csv(DATA_PROCESSED_DIR / "validation_domain_assessment.csv", index=False)
    readiness_summary.to_csv(DATA_PROCESSED_DIR / "validation_readiness_summary.csv", index=False)
    waivers_eval_df.to_csv(DATA_PROCESSED_DIR / "validation_waivers_status.csv", index=False)
    exceptions_active.to_csv(DATA_PROCESSED_DIR / "validation_exceptions_active.csv", index=False)
    top_risks_df.to_csv(DATA_PROCESSED_DIR / "validation_top_open_risks.csv", index=False)

    return {
        "checks": checks_df,
        "checklist": checklist_df,
        "issues": issues_effective_df,
        "domain_assessment": domain_df,
        "readiness_summary": readiness_summary,
        "waivers": waivers_eval_df,
        "exceptions": exceptions_active,
        "top_risks": top_risks_df,
        "fixes": fixes_df,
    }


if __name__ == "__main__":
    run_full_validation()
