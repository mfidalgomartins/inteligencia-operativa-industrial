from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from .config import DATA_PROCESSED_DIR, OUTPUT_DASHBOARD_DIR, OUTPUT_REPORTS_DIR


MAX_CANONICAL_SIZE_BYTES = 4_000_000
CANONICAL_HTML = OUTPUT_DASHBOARD_DIR / "industrial-operating-command-center.html"
DOCS_CANONICAL_HTML = Path(__file__).resolve().parents[1] / "docs" / "index.html"
CANONICAL_DATASET_JSON = DATA_PROCESSED_DIR / "dashboard_canonical_dataset.json"
SERVING_AUDIT_CSV = DATA_PROCESSED_DIR / "dashboard_serving_audit.csv"
CHARTJS_BUNDLE = Path(__file__).resolve().parents[1] / "assets" / "vendor" / "chart.umd.min.js"

LEGACY_DASHBOARD_OUTPUTS = [
    OUTPUT_DASHBOARD_DIR / "dashboard_executive_light.html",
    OUTPUT_DASHBOARD_DIR / "dashboard_full_offline.html",
    OUTPUT_DASHBOARD_DIR / "dashboard_board_pack.html",
    DATA_PROCESSED_DIR / "dashboard_exec_dataset.json",
    DATA_PROCESSED_DIR / "dashboard_full_dataset.json",
]


def _compact_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _load_chartjs_bundle() -> str:
    if CHARTJS_BUNDLE.exists():
        return CHARTJS_BUNDLE.read_text(encoding="utf-8")
    return ""


def _round_df(df: pd.DataFrame, digits: int = 3) -> pd.DataFrame:
    out = df.copy()
    for col in out.select_dtypes(include=["float", "float32", "float64"]).columns:
        out[col] = out[col].round(digits)
    return out


def _fmt(value: float, fmt: str) -> str:
    return format(float(value), fmt)


def _prepare_canonical_dataset() -> tuple[dict[str, Any], dict[str, Any]]:
    line_day = pd.read_csv(DATA_PROCESSED_DIR / "line_day_features_scored.csv", parse_dates=["fecha"])
    line_scores = pd.read_csv(DATA_PROCESSED_DIR / "line_criticality_scores.csv")
    equipment = pd.read_csv(DATA_PROCESSED_DIR / "equipment_energy_anomaly_scores.csv", parse_dates=["fecha"])
    shift = pd.read_csv(DATA_PROCESSED_DIR / "shift_comparison_scores.csv")
    root_cause = pd.read_csv(DATA_PROCESSED_DIR / "loss_root_cause_score.csv")
    invest = pd.read_csv(DATA_PROCESSED_DIR / "investment_prioritization_final.csv")
    scenario = pd.read_csv(DATA_PROCESSED_DIR / "scenario_table.csv")
    portfolio = pd.read_csv(DATA_PROCESSED_DIR / "portfolio_summary.csv")
    snapshot = pd.read_csv(DATA_PROCESSED_DIR / "executive_kpi_snapshot.csv").iloc[0]
    decision_snapshot = pd.read_csv(DATA_PROCESSED_DIR / "executive_decision_snapshot.csv").iloc[0]
    readiness_path = DATA_PROCESSED_DIR / "validation_readiness_summary.csv"
    readiness = pd.read_csv(readiness_path).iloc[0] if readiness_path.exists() else pd.Series(dtype=object)
    score_comp = pd.read_csv(DATA_PROCESSED_DIR / "score_comparability_registry.csv")
    optimization_meta_path = DATA_PROCESSED_DIR / "portfolio_optimization_metadata.csv"
    optimization_meta = pd.read_csv(optimization_meta_path).iloc[0] if optimization_meta_path.exists() else pd.Series(dtype=object)

    line_day["periodo"] = line_day["fecha"].dt.to_period("M").astype(str)
    top_lines = line_scores.sort_values("line_criticality_score", ascending=False).head(5)["linea_id"].astype(str).tolist()
    if str(decision_snapshot["linea_prioritaria"]) not in top_lines:
        top_lines = [str(decision_snapshot["linea_prioritaria"]), *top_lines[:5]]

    line_month = (
        line_day[line_day["linea_id"].astype(str).isin(top_lines)]
        .groupby(["periodo", "linea_id"], as_index=False)
        .agg(sec=("sec", "mean"), oee=("oee_synthetic", "mean"))
    )
    month_labels = sorted(line_month["periodo"].unique().tolist())

    sec_datasets: list[dict[str, Any]] = []
    oee_datasets: list[dict[str, Any]] = []
    for line_id in top_lines:
        tmp = line_month[line_month["linea_id"].astype(str) == line_id].set_index("periodo")
        sec_datasets.append(
            {
                "label": line_id,
                "data": [float(tmp["sec"].get(period, 0.0)) for period in month_labels],
            }
        )
        oee_datasets.append(
            {
                "label": line_id,
                "data": [float(tmp["oee"].get(period, 0.0)) for period in month_labels],
            }
        )

    energy_line = (
        line_day.groupby("linea_id", as_index=False)
        .agg(
            coste_energia_total=(
                "cost_energy_per_unit",
                lambda s: float((s * line_day.loc[s.index, "production_volume"]).sum()),
            ),
            sec_medio=("sec", "mean"),
        )
        .sort_values("coste_energia_total", ascending=False)
        .head(10)
    )

    equipment_top = (
        equipment.groupby(["equipo_id", "linea_id"], as_index=False)
        .agg(
            anomaly_score=("equipment_energy_anomaly_score", "mean"),
            downtime_hours=("downtime_hours", "sum"),
        )
        .sort_values(["anomaly_score", "downtime_hours"], ascending=[False, False])
        .head(10)
    )

    root_cause_top = (
        root_cause.groupby("causa_parada", as_index=False)["loss_root_cause_score"]
        .sum()
        .sort_values("loss_root_cause_score", ascending=False)
        .head(8)
    )

    shift_top = shift.copy()
    shift_top["label"] = shift_top["linea_id"].astype(str) + " - " + shift_top["turno"].astype(str)
    shift_top = shift_top.sort_values("shift_variance_score", ascending=False).head(10)

    scenario_summary = (
        scenario.groupby("macro_scenario", as_index=False)
        .agg(
            discounted_value=("discounted_value", "sum"),
            downside_adjusted_value=("downside_adjusted_value", "sum"),
        )
        .sort_values("discounted_value", ascending=False)
    )

    invest_table_cols = [
        "iniciativa_id",
        "linea_id",
        "initiative_class",
        "annual_saving_proxy",
        "npv_risk_adjusted",
        "payback_months",
        "improvement_priority_index",
        "initiative_tier",
        "portfolio_wave",
        "decision_rule",
        "selected_portfolio_flag",
    ]
    invest_table = _round_df(
        invest.sort_values(["selected_portfolio_flag", "improvement_priority_index"], ascending=[False, False])[invest_table_cols]
    )

    scatter_df = _round_df(
        invest[
            [
                "iniciativa_id",
                "implementation_feasibility_score",
                "improvement_priority_index",
                "annual_saving_proxy",
                "initiative_class",
            ]
        ]
    )
    scatter_df["bubble_r"] = scatter_df["annual_saving_proxy"].clip(lower=100_000) / 120_000
    scatter_df["bubble_r"] = scatter_df["bubble_r"].clip(4, 16).round(2)

    wave_summary = (
        invest[invest["selected_portfolio_flag"] == 1]
        .groupby("portfolio_wave", as_index=False)["iniciativa_id"]
        .count()
        .rename(columns={"iniciativa_id": "n_iniciativas"})
    )

    priority_comp_tag = score_comp.loc[
        score_comp["score_name"] == "improvement_priority_index", "comparability_tag"
    ]
    priority_comp = str(priority_comp_tag.iloc[0]) if not priority_comp_tag.empty else "contextual_comparable_inter_release"

    decision_model_version = "unknown"
    if not optimization_meta.empty:
        decision_model_version = (
            f"{optimization_meta.get('selection_model', 'unknown')}"
            f"|{optimization_meta.get('optimality_degree', 'unknown')}"
            f"|{optimization_meta.get('optimization_status', 'unknown')}"
        )
    elif "selection_model" in invest.columns:
        decision_model_version = f"{str(invest['selection_model'].mode().iloc[0])}|unknown|unknown"

    snapshot_id = f"{snapshot.get('run_id', 'unknown')}:{snapshot.get('snapshot_utc', 'unknown')}"
    validation_state = f"{readiness.get('overall_readiness_level', 'unknown')}|{readiness.get('publish_decision', 'unknown')}"

    portfolio_row = portfolio.iloc[0] if not portfolio.empty else pd.Series(dtype=float)
    downside_value = float(snapshot.get("downside_adjusted_value_portfolio", 0.0))
    npv_value = float(snapshot.get("portfolio_npv_risk_adjusted", 0.0))
    uncertainty_gap = max(0.0, npv_value - downside_value)
    uncertainty_ratio = (uncertainty_gap / npv_value) if npv_value > 0 else 0.0

    selected = invest[invest["selected_portfolio_flag"] == 1].copy()
    not_selected = invest[invest["selected_portfolio_flag"] != 1].copy()
    quick_wins_selected = int(((selected["quick_win_flag"] == 1).sum()) if not selected.empty else 0)
    capex_budget = float(portfolio_row.get("capex_budget", 0.0))
    capex_selected = float(portfolio_row.get("capex_selected", 0.0))
    capex_utilization = (capex_selected / capex_budget) if capex_budget > 0 else 0.0

    top_root = root_cause_top.iloc[0] if not root_cause_top.empty else pd.Series(dtype=object)
    worst_shift = shift_top.iloc[0] if not shift_top.empty else pd.Series(dtype=object)
    top_backlog = (
        not_selected.sort_values("annual_saving_proxy", ascending=False).iloc[0]
        if not not_selected.empty
        else pd.Series(dtype=object)
    )
    selected_count = int(portfolio_row.get("initiatives_selected", len(selected)))
    quick_win_ratio = (quick_wins_selected / selected_count) if selected_count > 0 else 0.0
    annual_saving_total = float(snapshot.get("annual_saving_proxy", 0.0))
    cost_of_delay_12m = float(snapshot.get("cost_of_delay_12m_portfolio", 0.0))
    delay_pressure_ratio = (cost_of_delay_12m / annual_saving_total) if annual_saving_total > 0 else 0.0
    validation_state_l = str(validation_state).lower()

    meta = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": str(snapshot["run_id"]),
        "snapshot_id": snapshot_id,
        "metric_version_set": str(snapshot["metric_version_set"]),
        "decision_model_version": decision_model_version,
        "validation_state": validation_state,
        "score_scale_version": str(snapshot.get("score_scale_version", "unknown")),
        "dashboard_mode": "canonical_single_html",
        "coverage_start": line_day["fecha"].min().strftime("%Y-%m-%d"),
        "coverage_end": line_day["fecha"].max().strftime("%Y-%m-%d"),
        "n_plants": int(line_day["planta_id"].nunique()),
        "n_lines": int(line_day["linea_id"].nunique()),
        "n_equipment": int(equipment["equipo_id"].nunique()),
    }

    fixed_insights = [
        {
            "title": "Foco operacional inmediato",
            "detail": (
                f"La línea {decision_snapshot['linea_prioritaria']} y el equipo {decision_snapshot['equipo_prioritario']} "
                f"concentran la señal más alta de criticidad/anomalía ({float(decision_snapshot['line_criticality_score']):.2f} / "
                f"{float(decision_snapshot['equipment_energy_anomaly_score']):.2f})."
            ),
            "action": "Priorizar intervención táctica en ventana operativa inmediata."
        },
        {
            "title": "Causa raíz dominante",
            "detail": (
                f"La causa {str(top_root.get('causa_parada', 'N/D'))} lidera pérdidas con score "
                f"{float(top_root.get('loss_root_cause_score', 0.0)):,.1f}."
            ),
            "action": "Abrir plan de contención de causa raíz y seguimiento semanal."
        },
        {
            "title": "Riesgo por variabilidad de turno",
            "detail": (
                f"El mayor desvío aparece en {str(worst_shift.get('label', 'N/D'))} "
                f"(score {float(worst_shift.get('shift_variance_score', 0.0)):.2f}, SEC {float(worst_shift.get('sec_shift', 0.0)):.1f} kWh/t)."
            ),
            "action": "Revisar estándar operativo y disciplina de turno en ese bloque."
        },
        {
            "title": "Valor capturable y robustez",
            "detail": (
                f"NPV ajustado por riesgo {npv_value:,.0f} EUR, downside {downside_value:,.0f} EUR, "
                f"gap de incertidumbre {uncertainty_ratio:.1%}."
            ),
            "action": "Diferenciar quick wins ejecutables ahora de iniciativas con mayor incertidumbre."
        },
        {
            "title": "Presión de cartera",
            "detail": (
                f"Se seleccionan {selected_count} iniciativas, {quick_wins_selected} quick wins, "
                f"con uso de CAPEX del {capex_utilization:.1%}. "
                f"Top no seleccionado por restricción: {str(top_backlog.get('iniciativa_id', 'N/D'))} "
                f"({float(top_backlog.get('annual_saving_proxy', 0.0)):,.0f} EUR/año)."
            ),
            "action": "Usar sensibilidad de budget/capacidad para evaluar desbloqueo de valor en siguiente ola."
        },
    ]

    conditional_candidates: list[dict[str, str]] = []
    if "publish-blocked" in validation_state_l or "no publish" in validation_state_l:
        conditional_candidates.append(
            {
                "title": "Riesgo de governance de release",
                "detail": (
                    f"El estado de validación actual ({validation_state}) limita la defensa de comité sin caveats."
                ),
                "action": "Resolver blockers/criticals antes de presentar como committee-grade."
            }
        )
    if delay_pressure_ratio >= 0.70:
        conditional_candidates.append(
            {
                "title": "Riesgo elevado por inacción",
                "detail": (
                    f"El cost of delay de 12 meses ({cost_of_delay_12m:,.0f} EUR) representa {delay_pressure_ratio:.1%} "
                    f"del ahorro anual estimado ({annual_saving_total:,.0f} EUR/año)."
                ),
                "action": "Acelerar quick wins y adelantar iniciativas de mayor contribución neta."
            }
        )
    if capex_utilization >= 0.90:
        conditional_candidates.append(
            {
                "title": "Tensión de presupuesto CAPEX",
                "detail": (
                    f"El CAPEX comprometido ({capex_selected:,.0f} EUR) consume {capex_utilization:.1%} del budget."
                ),
                "action": "Optimizar secuenciación por olas y preparar opciones de descoping."
            }
        )
    elif capex_utilization <= 0.70 and not top_backlog.empty:
        conditional_candidates.append(
            {
                "title": "Capacidad de capturar valor adicional",
                "detail": (
                    f"Con uso CAPEX de {capex_utilization:.1%}, existe margen para evaluar incorporación de backlog "
                    f"de alto valor ({str(top_backlog.get('iniciativa_id', 'N/D'))})."
                ),
                "action": "Ejecutar escenario de relajación de budget/capacidad para maximizar valor capturable."
            }
        )
    if quick_win_ratio < 0.25 and selected_count > 0:
        conditional_candidates.append(
            {
                "title": "Baja proporción de quick wins",
                "detail": (
                    f"Solo {quick_wins_selected} de {selected_count} iniciativas seleccionadas son quick wins "
                    f"({quick_win_ratio:.1%})."
                ),
                "action": "Rebalancear cartera para asegurar captura temprana de valor y tracción operativa."
            }
        )

    insights_payload = [*fixed_insights, *conditional_candidates[:2]]

    dataset = {
        "meta": meta,
        "governance": {
            "kpi_source": "data/processed/executive_kpi_snapshot.csv",
            "decision_source": "data/processed/executive_decision_snapshot.csv",
            "validation_source": "data/processed/validation_readiness_summary.csv",
            "priority_score_comparability": priority_comp,
        },
        "kpis": [
            {"name": "Volumen de producción", "value": f"{_fmt(snapshot['production_volume'], ',.0f')} t"},
            {"name": "OEE sintético", "value": f"{_fmt(snapshot['oee_synthetic'], '.3f')}"},
            {"name": "SEC medio", "value": f"{_fmt(snapshot['sec'], '.1f')} kWh/t"},
            {"name": "Pérdidas económicas proxy", "value": f"{_fmt(snapshot['loss_value_proxy'], ',.0f')} EUR"},
            {"name": "Ahorro anual proxy", "value": f"{_fmt(snapshot['annual_saving_proxy'], ',.0f')} EUR/año"},
            {"name": "NPV ajustado por riesgo", "value": f"{_fmt(snapshot['portfolio_npv_risk_adjusted'], ',.0f')} EUR"},
            {"name": "Valor downside-adjusted", "value": f"{_fmt(downside_value, ',.0f')} EUR"},
            {"name": "Coste de demora 12m", "value": f"{_fmt(snapshot.get('cost_of_delay_12m_portfolio', 0.0), ',.0f')} EUR"},
            {"name": "Portfolio seleccionado", "value": f"{int(snapshot['portfolio_selected'])} iniciativas"},
        ],
        "callouts": [
            f"Pérdida dominante: {decision_snapshot['linea_prioritaria']} lidera criticidad.",
            f"Equipo crítico: {decision_snapshot['equipo_prioritario']} con score {float(decision_snapshot['equipment_energy_anomaly_score']):.2f}.",
            f"Valor robusto vs incierto: downside {downside_value:,.0f} EUR; gap de incertidumbre {uncertainty_gap:,.0f} EUR.",
            f"Cartera seleccionada: CAPEX {float(portfolio_row.get('capex_selected', 0.0)):,.0f} / {float(portfolio_row.get('capex_budget', 0.0)):,.0f} EUR.",
        ],
        "insights": insights_payload,
        "decision_summary": {
            "linea_prioritaria": str(decision_snapshot["linea_prioritaria"]),
            "line_criticality_score": float(decision_snapshot["line_criticality_score"]),
            "equipo_prioritario": str(decision_snapshot["equipo_prioritario"]),
            "equipment_energy_anomaly_score": float(decision_snapshot["equipment_energy_anomaly_score"]),
            "ahorro_now_top5": float(decision_snapshot["ahorro_now_top5"]),
            "risk_of_inaction_12m": float(snapshot.get("cost_of_delay_12m_portfolio", 0.0)),
            "committee_note": "Soporte de decisión ejecutiva para screening/priorización. No sustituye aprobación final de inversión.",
        },
        "charts": {
            "sec_trend": {"labels": month_labels, "datasets": sec_datasets},
            "oee_trend": {"labels": month_labels, "datasets": oee_datasets},
            "energy_cost_line": _round_df(energy_line).to_dict("records"),
            "equipment_anomaly": _round_df(equipment_top).to_dict("records"),
            "root_cause": _round_df(root_cause_top).to_dict("records"),
            "shift_variance": _round_df(shift_top[["label", "shift_variance_score", "sec_shift", "scrap_pct"]]).to_dict("records"),
            "scenario_value": _round_df(scenario_summary).to_dict("records"),
            "portfolio_wave": _round_df(wave_summary).to_dict("records"),
            "priority_scatter": _round_df(scatter_df).to_dict("records"),
        },
        "table": invest_table.to_dict("records"),
    }

    metrics = {
        "line_day_rows_raw": int(len(line_day)),
        "equipment_rows_raw": int(len(equipment)),
        "invest_rows_raw": int(len(invest)),
        "payload_records": int(
            len(dataset["table"])
            + len(dataset["charts"]["energy_cost_line"])
            + len(dataset["charts"]["equipment_anomaly"])
            + len(dataset["charts"]["root_cause"])
            + len(dataset["charts"]["priority_scatter"])
        ),
    }
    return dataset, metrics


def _render_canonical_html(dataset: dict[str, Any]) -> str:
    meta = dataset["meta"]
    governance = dataset["governance"]
    decision = dataset["decision_summary"]
    kpi_lookup = {row["name"]: row["value"] for row in dataset["kpis"]}
    priority_kpis = {"OEE sintético", "SEC medio", "Pérdidas económicas proxy", "Ahorro anual proxy", "NPV ajustado por riesgo"}
    prominent_kpis = {"Pérdidas económicas proxy", "Ahorro anual proxy", "NPV ajustado por riesgo"}
    kpi_meta = {
        "Volumen de producción": ("Escala", "tone-neutral"),
        "OEE sintético": ("Rendimiento", "tone-operational"),
        "SEC medio": ("Energía", "tone-energy"),
        "Pérdidas económicas proxy": ("Pérdida", "tone-risk"),
        "Ahorro anual proxy": ("Valor", "tone-value"),
        "NPV ajustado por riesgo": ("Cartera", "tone-value"),
        "Valor downside-adjusted": ("Robustez", "tone-confidence"),
        "Coste de demora 12m": ("Urgencia", "tone-risk"),
        "Portfolio seleccionado": ("Ejecución", "tone-execution"),
    }
    readiness_level, publish_decision = (str(meta["validation_state"]).split("|", 1) + [""])[:2]
    readiness_label = readiness_level.replace("-", " ").strip().capitalize()
    publish_label = publish_decision.strip().capitalize()
    if "blocked" in readiness_level:
        readiness_tone = "critical"
    elif "screening" in readiness_level or "caveat" in publish_decision:
        readiness_tone = "warn"
    else:
        readiness_tone = "ok"
    chartjs_inline = _load_chartjs_bundle()
    chartjs_tag = (
        f"<script>{chartjs_inline}</script>"
        if chartjs_inline
        else "<script src=\"https://cdn.jsdelivr.net/npm/chart.js\"></script>"
    )
    kpi_html = "".join(
        (
            f"<div class='kpi-card {kpi_meta.get(row['name'], ('General', 'tone-neutral'))[1]} "
            f"{'kpi-priority' if row['name'] in priority_kpis else ''} "
            f"{'kpi-prominent' if row['name'] in prominent_kpis else ''} "
            f"{'kpi-long' if len(str(row['value'])) > 22 else ''}'>"
            f"<div class='kpi-topline'><span class='kpi-eyebrow'>{kpi_meta.get(row['name'], ('General', 'tone-neutral'))[0]}</span></div>"
            f"<div class='kpi-name'>{row['name']}</div>"
            f"<div class='kpi-value'>{row['value']}</div>"
            "</div>"
        )
        for row in dataset["kpis"]
    )
    hero_summary_html = "".join(
        (
            "<div class='hero-summary-card'>"
            f"<div class='hero-summary-label'>{card['label']}</div>"
            f"<div class='hero-summary-value'>{card['value']}</div>"
            f"<div class='hero-summary-detail'>{card['detail']}</div>"
            "</div>"
        )
        for card in [
            {
                "label": "Intervención inmediata",
                "value": f"{decision['linea_prioritaria']} / {decision['equipo_prioritario']}",
                "detail": (
                    f"Criticidad {decision['line_criticality_score']:.2f} · "
                    f"Anomalía {decision['equipment_energy_anomaly_score']:.2f}"
                ),
            },
            {
                "label": "Valor capturable",
                "value": kpi_lookup.get("Ahorro anual proxy", "N/D"),
                "detail": f"NPV ajustado {kpi_lookup.get('NPV ajustado por riesgo', 'N/D')}",
            },
            {
                "label": "Estado de release",
                "value": readiness_label,
                "detail": publish_label or "Sin publish_decision",
            },
        ]
    )
    callout_html = "".join(f"<div class='callout'>{row}</div>" for row in dataset["callouts"])
    insights_html = "".join(
        (
            "<div class='insight-card'>"
            f"<div class='insight-title'>{row['title']}</div>"
            f"<div class='insight-detail'>{row['detail']}</div>"
            f"<div class='insight-action'><b>Acción sugerida:</b> {row['action']}</div>"
            "</div>"
        )
        for row in dataset["insights"]
    )
    methodology_html = (
        "<ul class='method-list'>"
        f"<li><b>Fuente KPI oficial:</b> {governance['kpi_source']}</li>"
        f"<li><b>Fuente decisión oficial:</b> {governance['decision_source']}</li>"
        f"<li><b>Fuente validación oficial:</b> {governance['validation_source']}</li>"
        f"<li><b>Comparabilidad score de prioridad:</b> {governance['priority_score_comparability']}</li>"
        "<li><b>Regla:</b> KPIs headline no se recalculan en frontend; se consumen de datasets gobernados.</li>"
        "<li><b>Caveat:</b> Uso para screening/priorización ejecutiva; no sustituye business case corporativo final.</li>"
        "</ul>"
        "<details class='method-tech'>"
        "<summary>Metadata técnica (oculta en la vista ejecutiva)</summary>"
        "<ul class='method-list method-list-tech'>"
        f"<li>dashboard_mode: {meta['dashboard_mode']}</li>"
        f"<li>run_id: {meta['run_id']}</li>"
        f"<li>snapshot_id: {meta['snapshot_id']}</li>"
        f"<li>metric_version_set: {meta['metric_version_set']}</li>"
        f"<li>decision_model_version: {meta['decision_model_version']}</li>"
        f"<li>validation_state: {meta['validation_state']}</li>"
        "</ul>"
        "</details>"
    )
    wave_options = "".join(
        f"<option value='{wave}'>{wave}</option>"
        for wave in sorted({str(row['portfolio_wave']) for row in dataset['table'] if str(row['portfolio_wave'])})
    )
    data_json = _compact_json(dataset)
    return f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Cockpit Ejecutivo Industrial (Canónico)</title>
{chartjs_tag}
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&family=Space+Grotesk:wght@500;700&display=swap');
:root{{
  --bg:#edf3f8;
  --bg-2:#dce8f2;
  --panel:#ffffff;
  --panel-soft:#f7fafd;
  --surface-elev:#f4f8fc;
  --ink:#102a43;
  --ink-soft:#36516c;
  --ink-muted:#6b7c93;
  --line:#d4e0eb;
  --line-strong:#bfd2e5;
  --accent:#0f766e;
  --accent-2:#2456cf;
  --accent-3:#0ea5e9;
  --warn:#a95c16;
  --critical:#b91c1c;
  --success:#0f766e;
  --chart-grid:rgba(39,73,109,.12);
  --chart-text:#36516c;
  --chart-tooltip-bg:#ffffff;
  --chart-tooltip-text:#102a43;
  --radius-2xl:26px;
  --radius-xl:20px;
  --radius-lg:16px;
  --radius-md:12px;
  --shadow-sm:0 8px 20px rgba(16,42,67,.06);
  --shadow-md:0 18px 42px rgba(16,42,67,.12);
  --shadow-lg:0 24px 56px rgba(16,42,67,.16);
}}
[data-theme='dark']{{
  --bg:#0b1420;
  --bg-2:#0f1b2d;
  --panel:#111e31;
  --panel-soft:#13233a;
  --surface-elev:#152740;
  --ink:#e8f0fa;
  --ink-soft:#9eb7d1;
  --ink-muted:#7f97b2;
  --line:#25415f;
  --line-strong:#315778;
  --accent:#19a49a;
  --accent-2:#4a7cff;
  --accent-3:#26b4f5;
  --warn:#d08a32;
  --critical:#ef4444;
  --success:#22c55e;
  --chart-grid:rgba(173,204,237,.12);
  --chart-text:#bdd2e9;
  --chart-tooltip-bg:#1b3049;
  --chart-tooltip-text:#e8f0fa;
  --shadow-sm:0 4px 12px rgba(0,0,0,.28);
  --shadow-md:0 12px 28px rgba(0,0,0,.36);
  --shadow-lg:0 22px 54px rgba(0,0,0,.44);
}}
*{{box-sizing:border-box}}
html{{scroll-behavior:smooth;scroll-padding-top:18px}}
body{{font-family:'IBM Plex Sans','Avenir Next','Segoe UI',sans-serif;margin:0;color:var(--ink);background:
radial-gradient(1200px 520px at -10% -20%,rgba(28,148,134,.22) 0%,transparent 58%),
radial-gradient(920px 500px at 110% -12%,rgba(62,111,232,.18) 0%,transparent 52%),
linear-gradient(180deg,var(--bg) 0%,var(--bg-2) 100%);overflow-x:hidden;transition:background .22s ease,color .22s ease;-webkit-font-smoothing:antialiased;-moz-osx-font-smoothing:grayscale}}
.container{{position:relative;max-width:1280px;margin:0 auto;padding:18px 20px 34px}}
.topbar-shell{{display:flex;flex-wrap:wrap;align-items:center;justify-content:space-between;gap:12px;margin-bottom:14px}}
.ux-toolbar{{position:static;z-index:auto;display:flex;flex-wrap:wrap;gap:10px;justify-content:flex-end;margin:0;padding:8px 10px;border-radius:18px;background:color-mix(in srgb, var(--panel) 84%, transparent);border:1px solid color-mix(in srgb, var(--line-strong) 68%, transparent);box-shadow:var(--shadow-sm);backdrop-filter:blur(14px)}}
.ux-btn{{border:1px solid color-mix(in srgb, var(--line-strong) 82%, transparent);background:color-mix(in srgb, var(--panel) 88%, transparent);color:var(--ink);border-radius:999px;padding:9px 14px;font-size:12px;font-weight:700;cursor:pointer;transition:all .18s ease;box-shadow:none;backdrop-filter:blur(14px)}}
.ux-btn:hover{{transform:translateY(-1px);border-color:color-mix(in srgb, var(--accent-2) 52%, var(--line-strong));box-shadow:var(--shadow-md)}}
.ux-btn-theme{{display:inline-flex;align-items:center;gap:8px}}
.theme-dot{{width:10px;height:10px;border-radius:50%;background:linear-gradient(180deg,var(--accent),var(--accent-2));display:inline-block;box-shadow:0 0 0 4px color-mix(in srgb, var(--accent) 14%, transparent)}}
.quick-nav{{display:flex;flex:1 1 680px;flex-wrap:wrap;gap:8px;margin:0}}
.quick-nav a{{text-decoration:none;font-size:11.5px;font-weight:700;color:var(--ink-soft);background:color-mix(in srgb, var(--surface-elev) 92%, transparent);border:1px solid var(--line-strong);border-radius:999px;padding:7px 12px;transition:all .16s ease;box-shadow:0 2px 6px rgba(16,42,67,.03)}}
.quick-nav a:hover{{transform:translateY(-1px);border-color:color-mix(in srgb, var(--accent-2) 46%, var(--line-strong));color:var(--ink);background:color-mix(in srgb, var(--panel) 92%, transparent)}}
.hero-shell{{position:relative;display:grid;grid-template-columns:minmax(0,1.6fr) minmax(300px,.95fr);gap:14px;align-items:stretch;margin-bottom:8px}}
.header{{position:relative;overflow:hidden;background:
linear-gradient(137deg,#0d6c65 0%,#145e9b 42%,#264fca 100%);
color:#fff;padding:20px 22px 18px;border-radius:24px;box-shadow:var(--shadow-lg);min-height:100%}}
.header::before{{content:'';position:absolute;inset:0;background:
linear-gradient(180deg,rgba(255,255,255,.08),rgba(255,255,255,0) 38%),
radial-gradient(circle at 18% 18%,rgba(255,255,255,.14),transparent 32%)}}
.header::after{{content:'';position:absolute;right:-120px;top:-110px;width:360px;height:360px;background:radial-gradient(circle,rgba(255,255,255,.24) 0%,rgba(255,255,255,0) 65%)}}
.hero-main,.hero-side{{position:relative;z-index:1}}
.hero-kicker{{display:inline-flex;align-items:center;gap:8px;padding:7px 12px;border-radius:999px;background:rgba(255,255,255,.14);border:1px solid rgba(255,255,255,.22);font-size:11px;font-weight:700;letter-spacing:.04em;text-transform:uppercase}}
.hero-kicker::before{{content:'';width:8px;height:8px;border-radius:50%;background:#9ff3df;box-shadow:0 0 0 4px rgba(159,243,223,.18)}}
.header h1{{position:relative;font-family:'Space Grotesk','Avenir Next','Segoe UI',sans-serif;font-size:clamp(28px,2.8vw,40px);line-height:1;letter-spacing:-.032em;margin:12px 0 8px;max-width:760px}}
.header-subtitle{{position:relative;font-size:13.5px;line-height:1.48;opacity:.96;max-width:720px}}
.header-lead{{position:relative;margin-top:12px;font-size:14px;font-weight:600;line-height:1.42;max-width:720px}}
.header-stats{{position:relative;margin-top:10px;font-size:11.5px;opacity:.92}}
.header-micro{{position:relative;margin-top:10px;display:flex;flex-wrap:wrap;gap:8px}}
.header-pill{{background:rgba(255,255,255,.14);border:1px solid rgba(255,255,255,.22);padding:6px 10px;border-radius:999px;font-size:11px;font-weight:600}}
.hero-side{{display:grid;gap:8px}}
.hero-readiness{{padding:13px 15px;border-radius:20px;background:linear-gradient(180deg,rgba(255,255,255,.14),rgba(255,255,255,.08));border:1px solid rgba(255,255,255,.18);box-shadow:inset 0 1px 0 rgba(255,255,255,.12)}}
.hero-readiness-label{{font-size:11px;font-weight:700;letter-spacing:.05em;text-transform:uppercase;opacity:.82}}
.hero-readiness-value{{margin-top:6px;font-family:'Space Grotesk','Avenir Next','Segoe UI',sans-serif;font-size:22px;line-height:1.1;letter-spacing:-.02em}}
.hero-readiness-detail{{margin-top:6px;font-size:12px;opacity:.88;line-height:1.4}}
.hero-summary-grid{{display:grid;gap:8px}}
.hero-summary-card{{padding:12px 14px;border-radius:18px;background:linear-gradient(180deg,rgba(255,255,255,.17),rgba(255,255,255,.08));border:1px solid rgba(255,255,255,.18);backdrop-filter:blur(8px);box-shadow:0 10px 20px rgba(0,0,0,.08)}}
.hero-summary-label{{font-size:11px;font-weight:700;letter-spacing:.05em;text-transform:uppercase;opacity:.82}}
.hero-summary-value{{margin-top:5px;font-family:'Space Grotesk','Avenir Next','Segoe UI',sans-serif;font-size:17px;line-height:1.18;letter-spacing:-.02em}}
.hero-summary-detail{{margin-top:5px;font-size:12px;opacity:.88;line-height:1.42}}
.callouts{{position:relative;display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:8px;margin-top:14px}}
.callout{{background:linear-gradient(180deg,rgba(255,255,255,.16),rgba(255,255,255,.10));border:1px solid rgba(255,255,255,.18);padding:11px 12px;border-radius:15px;font-size:11.5px;line-height:1.42;backdrop-filter:blur(8px);box-shadow:0 10px 20px rgba(0,0,0,.06)}}
.kpi-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:8px;margin:8px 0 10px}}
.kpi-card{{position:relative;background:linear-gradient(180deg,color-mix(in srgb, var(--panel) 98%, transparent),color-mix(in srgb, var(--surface-elev) 36%, var(--panel)));border-radius:18px;padding:13px 13px 12px;border:1px solid var(--line);box-shadow:var(--shadow-sm);overflow:hidden;min-height:92px;display:flex;flex-direction:column;justify-content:flex-start;transition:transform .16s ease, box-shadow .16s ease,border-color .16s ease}}
.kpi-card:hover{{transform:translateY(-2px);box-shadow:var(--shadow-md)}}
.kpi-card::before{{content:'';position:absolute;left:0;top:0;width:100%;height:4px;background:linear-gradient(90deg,#11867d,#2f72ca)}}
.kpi-card.kpi-priority{{border-color:#a9d7d2;box-shadow:0 12px 28px rgba(15,118,110,.12)}}
.kpi-card.kpi-prominent{{min-height:132px;padding-top:16px}}
.kpi-card.kpi-priority::before{{background:linear-gradient(90deg,#0f766e,#2d66d1)}}
.kpi-card.tone-risk::before{{background:linear-gradient(90deg,#b91c1c,#d97706)}}
.kpi-card.tone-value::before{{background:linear-gradient(90deg,#0f766e,#0ea5e9)}}
.kpi-card.tone-energy::before{{background:linear-gradient(90deg,#2456cf,#0ea5e9)}}
.kpi-card.tone-confidence::before{{background:linear-gradient(90deg,#1e40af,#0f766e)}}
.kpi-card.tone-execution::before{{background:linear-gradient(90deg,#7c3aed,#2456cf)}}
.kpi-topline{{display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:7px}}
.kpi-eyebrow{{font-size:10px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:var(--ink-soft)}}
.kpi-name{{font-size:12px;font-weight:600;color:var(--ink-muted);line-height:1.25}}
.kpi-value{{font-family:'Space Grotesk','Avenir Next','Segoe UI',sans-serif;font-size:clamp(18px,1.4vw,28px);font-weight:700;letter-spacing:-.03em;color:var(--ink);line-height:1.02;margin-top:8px;overflow-wrap:anywhere;word-break:break-word;font-variant-numeric:tabular-nums}}
.kpi-card.kpi-prominent .kpi-value{{font-size:clamp(24px,2vw,34px)}}
.kpi-card.kpi-long .kpi-value{{font-size:clamp(13px,.9vw,17px);line-height:1.2}}
.section{{position:relative;background:linear-gradient(180deg,color-mix(in srgb, var(--panel) 98%, transparent),color-mix(in srgb, var(--panel-soft) 70%, var(--panel)));border:1px solid var(--line);border-radius:22px;padding:16px 16px 15px;margin:14px 0;box-shadow:var(--shadow-sm);overflow:hidden}}
.section::before{{content:'';position:absolute;left:18px;right:18px;top:0;height:1px;background:linear-gradient(90deg,color-mix(in srgb, var(--accent) 16%, transparent),transparent 70%)}}
.section h2{{font-family:'Space Grotesk','Avenir Next','Segoe UI',sans-serif;font-size:clamp(22px,1.8vw,28px);letter-spacing:-.025em;color:var(--ink);margin:1px 0 8px;display:flex;align-items:center;gap:10px}}
.section h2::before{{content:'';display:inline-block;width:10px;height:28px;border-radius:99px;background:linear-gradient(180deg,#0f766e,#1d4ed8)}}
.section-subtitle{{font-size:13px;color:var(--ink-muted);margin:-2px 0 14px;max-width:760px;line-height:1.45}}
.chart-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:12px;align-items:stretch}}
.chart-card{{position:relative;display:flex;flex-direction:column;background:linear-gradient(180deg,var(--panel-soft),var(--surface-elev));border:1px solid var(--line);border-radius:18px;padding:13px;min-width:0;overflow:hidden}}
.chart-card::before{{content:'';position:absolute;left:14px;right:14px;top:0;height:3px;border-radius:999px;background:linear-gradient(90deg,color-mix(in srgb, var(--accent) 70%, transparent),color-mix(in srgb, var(--accent-2) 70%, transparent))}}
.chart-kicker{{font-size:10px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:var(--ink-soft);margin-bottom:6px}}
.chart-title{{font-size:14px;font-weight:700;color:var(--ink);margin-bottom:4px;line-height:1.3}}
.chart-note{{font-size:11.5px;color:var(--ink-muted);margin-bottom:8px;line-height:1.45;max-width:520px}}
.chart-canvas-wrap{{position:relative;height:244px;min-width:0}}
.chart-canvas-wrap canvas{{position:absolute;inset:0;width:100% !important;height:100% !important;display:block}}
.insight-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:12px}}
.insight-card{{background:linear-gradient(180deg,var(--surface-elev) 0%,var(--panel-soft) 100%);border:1px solid var(--line);border-radius:18px;padding:13px;min-width:0;box-shadow:0 2px 6px rgba(16,42,67,.05)}}
.insight-title{{font-size:13px;font-weight:700;color:var(--ink);margin-bottom:8px}}
.insight-detail{{font-size:12px;color:var(--ink-soft);line-height:1.5}}
.insight-action{{margin-top:10px;font-size:11.5px;color:var(--ink);background:color-mix(in srgb, var(--accent-2) 10%, var(--panel));border:1px solid color-mix(in srgb, var(--accent-2) 26%, var(--line));padding:8px 9px;border-radius:12px;line-height:1.42}}

.decision-card{{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;background:linear-gradient(180deg,var(--surface-elev),var(--panel));border:1px solid var(--line-strong);border-radius:20px;padding:13px}}
.decision-item{{background:linear-gradient(180deg,var(--panel-soft),var(--panel));border:1px solid var(--line);border-radius:16px;padding:11px 12px}}
.decision-item b{{display:block;font-size:11px;color:var(--ink-muted);margin-bottom:8px;letter-spacing:.05em;text-transform:uppercase}}
.decision-item span{{font-size:15px;font-weight:700;color:var(--ink);line-height:1.35}}
.badge{{display:inline-flex;align-items:center;gap:6px;padding:4px 8px;border-radius:999px;font-size:11px;font-weight:700;border:1px solid transparent}}
.badge.critical{{background:color-mix(in srgb,var(--critical) 12%, var(--panel));border-color:color-mix(in srgb,var(--critical) 40%, var(--line));color:var(--critical)}}
.badge.warn{{background:color-mix(in srgb,var(--warn) 12%, var(--panel));border-color:color-mix(in srgb,var(--warn) 40%, var(--line));color:var(--warn)}}
.badge.ok{{background:color-mix(in srgb,var(--success) 12%, var(--panel));border-color:color-mix(in srgb,var(--success) 40%, var(--line));color:var(--success)}}
.table-toolbar{{display:flex;gap:12px;align-items:center;justify-content:space-between;margin-bottom:10px;flex-wrap:wrap}}
.table-toolbar-main{{display:flex;gap:10px;align-items:center;flex-wrap:wrap;flex:1 1 560px}}
.table-toolbar input,.table-toolbar select{{padding:10px 12px;border:1px solid var(--line-strong);border-radius:12px;min-width:220px;font-size:12px;background:var(--surface-elev);color:var(--ink);box-shadow:inset 0 1px 0 rgba(255,255,255,.24);appearance:none}}
.table-toolbar select{{min-width:170px}}
.table-status{{display:flex;gap:8px;align-items:center;flex-wrap:wrap}}
.table-count{{padding:8px 10px;border-radius:999px;background:color-mix(in srgb, var(--accent-2) 10%, var(--panel));border:1px solid color-mix(in srgb, var(--accent-2) 18%, var(--line));font-size:11px;font-weight:700;color:var(--ink)}}
.table-filters{{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px}}
.filter-chip{{border:1px solid var(--line-strong);background:var(--surface-elev);color:var(--ink-soft);border-radius:999px;padding:8px 12px;font-size:11px;font-weight:700;cursor:pointer;transition:all .16s ease}}
.filter-chip:hover{{transform:translateY(-1px);border-color:color-mix(in srgb, var(--accent-2) 40%, var(--line-strong));color:var(--ink)}}
.filter-chip.active{{background:linear-gradient(90deg,color-mix(in srgb, var(--accent) 14%, var(--panel)),color-mix(in srgb, var(--accent-2) 14%, var(--panel)));border-color:color-mix(in srgb, var(--accent-2) 30%, var(--line-strong));color:var(--ink)}}
.table-wrap{{overflow:auto;max-height:400px;border:1px solid var(--line);border-radius:16px;background:var(--panel);box-shadow:inset 0 1px 0 rgba(255,255,255,.34)}}
table{{width:100%;border-collapse:separate;border-spacing:0;font-size:12px}}
th,td{{padding:10px 10px;border-bottom:1px solid var(--line);text-align:left;color:var(--ink);vertical-align:top}}
tbody tr:nth-child(even) td{{background:color-mix(in srgb, var(--surface-elev) 40%, var(--panel))}}
tbody tr:hover td{{background:color-mix(in srgb, var(--accent-2) 6%, var(--panel))}}
th{{position:sticky;top:0;background:var(--surface-elev);color:var(--ink-soft);font-size:11px;letter-spacing:.04em;text-transform:uppercase;z-index:2}}
th:first-child,td:first-child{{position:sticky;left:0;z-index:1;background:inherit}}
th:first-child{{z-index:3}}
.quick-nav a:focus-visible,.ux-btn:focus-visible,.filter-chip:focus-visible,.table-toolbar input:focus-visible,.table-toolbar select:focus-visible,.modal-close:focus-visible{{outline:2px solid color-mix(in srgb, var(--accent-2) 70%, white);outline-offset:2px}}
.cell-num{{text-align:right;font-variant-numeric:tabular-nums}}
.cell-key{{font-weight:700;color:var(--ink)}}
.table-badge{{display:inline-flex;align-items:center;justify-content:center;padding:4px 8px;border-radius:999px;border:1px solid transparent;font-size:10.5px;font-weight:700;white-space:nowrap}}
.table-badge.ok{{background:color-mix(in srgb,var(--success) 12%, var(--panel));border-color:color-mix(in srgb,var(--success) 34%, var(--line));color:var(--success)}}
.table-badge.warn{{background:color-mix(in srgb,var(--warn) 12%, var(--panel));border-color:color-mix(in srgb,var(--warn) 34%, var(--line));color:var(--warn)}}
.table-badge.critical{{background:color-mix(in srgb,var(--critical) 12%, var(--panel));border-color:color-mix(in srgb,var(--critical) 34%, var(--line));color:var(--critical)}}
.table-badge.muted{{background:color-mix(in srgb,var(--ink-soft) 10%, var(--panel));border-color:color-mix(in srgb,var(--ink-soft) 18%, var(--line));color:var(--ink-soft)}}
.modal-backdrop{{position:fixed;inset:0;background:rgba(15,23,42,.48);display:none;align-items:center;justify-content:center;z-index:120;padding:16px}}
.modal-backdrop.show{{display:flex}}
.modal-card{{background:var(--panel);border-radius:16px;max-width:800px;width:100%;box-shadow:0 18px 44px rgba(15,23,42,.28);border:1px solid var(--line);overflow:hidden}}
.modal-header{{display:flex;justify-content:space-between;align-items:center;padding:14px 16px;border-bottom:1px solid var(--line);background:var(--surface-elev)}}
.modal-title{{font-family:'Space Grotesk','Avenir Next','Segoe UI',sans-serif;font-size:28px;font-weight:700;color:var(--ink)}}
.modal-close{{border:0;background:transparent;font-size:24px;line-height:1;cursor:pointer;color:var(--ink-soft)}}
.modal-body{{padding:14px 16px}}
.method-list{{margin:0;padding-left:18px;display:grid;gap:8px}}
.method-list li{{font-size:13px;color:var(--ink);line-height:1.42}}
.method-tech{{margin-top:12px;border:1px solid var(--line);border-radius:10px;background:var(--surface-elev);padding:8px 10px}}
.method-tech summary{{font-weight:700;font-size:13px;cursor:pointer;color:var(--ink-soft)}}
.method-list.method-list-tech{{margin-top:8px}}
.sr-only{{position:absolute !important;width:1px;height:1px;padding:0;margin:-1px;overflow:hidden;clip:rect(0,0,0,0);white-space:nowrap;border:0}}
@media (min-width:1080px){{
  .kpi-card.kpi-prominent{{grid-column:span 2}}
}}
@media (max-width:900px){{
  .topbar-shell{{align-items:flex-start}}
  .ux-toolbar{{justify-content:flex-start;width:100%}}
  .quick-nav{{flex-basis:100%}}
  .hero-shell{{grid-template-columns:1fr}}
  .header{{padding:18px 18px 16px}}
  .ux-btn{{font-size:12px;padding:7px 11px}}
  .header h1{{font-size:24px}}
  .header-subtitle{{font-size:12px}}
  .header-lead{{font-size:13px}}
  .kpi-grid{{grid-template-columns:repeat(auto-fit,minmax(170px,1fr))}}
  .kpi-card.kpi-prominent{{min-height:108px}}
  .kpi-value{{font-size:18px}}
  .section h2{{font-size:17px}}
  .chart-grid{{grid-template-columns:1fr}}
  .chart-canvas-wrap{{height:230px}}
  .table-toolbar{{display:block}}
  .table-toolbar-main{{display:grid;gap:10px}}
  .table-toolbar input,.table-toolbar select{{min-width:0;width:100%;box-sizing:border-box}}
  th:first-child,td:first-child{{position:static}}
}}
@media print{{
  body{{background:#fff}}
  .ux-toolbar,.quick-nav{{display:none !important}}
  .header{{box-shadow:none}}
  .hero-shell{{grid-template-columns:1fr}}
  .section{{break-inside:avoid;box-shadow:none}}
  .kpi-card,.insight-card,.chart-card{{box-shadow:none}}
  .chart-canvas-wrap{{height:220px}}
  .table-wrap{{max-height:none}}
  th{{position:static}}
  th:first-child,td:first-child{{position:static}}
  a{{color:inherit;text-decoration:none}}
  *{{-webkit-print-color-adjust:exact;print-color-adjust:exact}}
}}
</style>
</head>
<body>
<div class="container">
  <div class="topbar-shell">
    <div class="quick-nav" aria-label="Navegación rápida">
      <a href="#sec-insights">Insights</a>
      <a href="#sec-eff">Eficiencia</a>
      <a href="#sec-energy">Energía</a>
      <a href="#sec-process">Proceso</a>
      <a href="#sec-portfolio">Cartera</a>
      <a href="#sec-table">Tabla</a>
      <a href="#sec-decision">Decisión</a>
    </div>
    <div class="ux-toolbar">
      <button id="btnTheme" class="ux-btn ux-btn-theme" type="button" aria-pressed="false">
        <span class="theme-dot" aria-hidden="true"></span>
        <span id="btnThemeLabel">Modo oscuro</span>
      </button>
      <button id="btnMethodology" class="ux-btn" type="button" aria-haspopup="dialog">Metodología</button>
      <button id="btnPrint" class="ux-btn" type="button">Imprimir</button>
    </div>
  </div>

  <div class="hero-shell">
    <div class="header">
      <div class="hero-main">
        <div class="hero-kicker">Centro de mando de decisión</div>
        <h1 style="margin:0">Cockpit Ejecutivo de Inteligencia Operativa Industrial</h1>
        <div class="header-subtitle">Vista ejecutiva para priorizar pérdidas, riesgos operativos y captura de valor en planta industrial.</div>
        <div class="header-lead">La prioridad inmediata está en <b>{decision['linea_prioritaria']}</b> y <b>{decision['equipo_prioritario']}</b>. El sistema cuantifica {kpi_lookup.get('Ahorro anual proxy', 'N/D')} de ahorro anual proxy y {kpi_lookup.get('Coste de demora 12m', 'N/D')} de coste de demora si no se actúa.</div>
        <div class="header-stats">Cobertura: {meta['coverage_start']} a {meta['coverage_end']} · Plantas: {meta['n_plants']} · Líneas: {meta['n_lines']} · Equipos: {meta['n_equipment']}</div>
        <div class="header-micro">
          <span class="header-pill">Modo: ejecutivo</span>
          <span class="header-pill">KPIs gobernados</span>
          <span class="header-pill">Decisión: screening</span>
        </div>
        <div class="callouts">{callout_html}</div>
        <div class="sr-only" aria-hidden="true">dashboard_mode: {meta['dashboard_mode']} | run_id: {meta['run_id']} | snapshot_id: {meta['snapshot_id']} | metric_version_set: {meta['metric_version_set']} | decision_model_version: {meta['decision_model_version']} | validation_state: {meta['validation_state']}</div>
      </div>
    </div>
    <div class="hero-side">
      <div class="hero-readiness">
      <div class="hero-readiness-label">Estado de decisión</div>
      <div class="hero-readiness-value">{readiness_label}</div>
      <div class="hero-readiness-detail">{publish_label or 'Sin publish decision'}</div>
      <div style="margin-top:10px"><span class="badge {readiness_tone}">{readiness_label} · {publish_label or 'Sin publish decision'}</span></div>
    </div>
      <div class="hero-summary-grid">{hero_summary_html}</div>
    </div>
  </div>

  <div class="kpi-grid">{kpi_html}</div>

  <div class="section" id="sec-insights">
    <h2>Insights Ejecutivos</h2>
    <div class="section-subtitle">Señales clave que explican pérdidas, riesgos y oportunidades de captura de valor.</div>
    <div class="insight-grid">{insights_html}</div>
  </div>

  <div class="section" id="sec-eff">
    <h2>Vista de Eficiencia Operativa</h2>
    <div class="section-subtitle">Tendencias críticas de SEC y OEE para líneas con mayor impacto.</div>
    <div class="chart-grid">
      <div class="chart-card">
        <div class="chart-kicker">Tendencia operativa</div>
        <div class="chart-title">Tendencia SEC por línea</div>
        <div class="chart-note">Permite detectar deterioro sostenido de consumo específico en las líneas con mayor peso de decisión.</div>
        <div class="chart-canvas-wrap" data-chart-id="secTrend"><canvas id="secTrend"></canvas></div>
      </div>
      <div class="chart-card">
        <div class="chart-kicker">Tendencia operativa</div>
        <div class="chart-title">Tendencia OEE sintético por línea</div>
        <div class="chart-note">Hace visible qué líneas combinan menor desempeño y peor resiliencia operativa.</div>
        <div class="chart-canvas-wrap" data-chart-id="oeeTrend"><canvas id="oeeTrend"></canvas></div>
      </div>
    </div>
  </div>

  <div class="section" id="sec-energy">
    <h2>Vista Energética y Coste</h2>
    <div class="section-subtitle">Coste energético por línea y anomalías por equipo prioritario.</div>
    <div class="chart-grid">
      <div class="chart-card">
        <div class="chart-kicker">Concentración económica</div>
        <div class="chart-title">Coste energético total por línea</div>
        <div class="chart-note">Ordena las líneas por impacto económico para separar ruido operativo de pérdidas materiales.</div>
        <div class="chart-canvas-wrap" data-chart-id="energyCostLine"><canvas id="energyCostLine"></canvas></div>
      </div>
      <div class="chart-card">
        <div class="chart-kicker">Anomalía por activo</div>
        <div class="chart-title">Top equipos por anomalía de consumo</div>
        <div class="chart-note">Señala dónde intervenir primero a nivel de equipo antes de escalar CAPEX estructural.</div>
        <div class="chart-canvas-wrap" data-chart-id="equipmentAnomaly"><canvas id="equipmentAnomaly"></canvas></div>
      </div>
    </div>
  </div>

  <div class="section" id="sec-process">
    <h2>Vista de Proceso y Causas Raíz</h2>
    <div class="section-subtitle">Drivers dominantes de pérdida y variabilidad operativa.</div>
    <div class="chart-grid">
      <div class="chart-card">
        <div class="chart-kicker">Causa raíz</div>
        <div class="chart-title">Pérdida económica por causa raíz</div>
        <div class="chart-note">Ayuda a diferenciar paradas estructurales de eventos más tácticos o de bajo retorno.</div>
        <div class="chart-canvas-wrap" data-chart-id="rootCause"><canvas id="rootCause"></canvas></div>
      </div>
      <div class="chart-card">
        <div class="chart-kicker">Variabilidad</div>
        <div class="chart-title">Variabilidad operativa por línea-turno</div>
        <div class="chart-note">Muestra dónde la disciplina de turno está amplificando scrap, SEC y volatilidad operativa.</div>
        <div class="chart-canvas-wrap" data-chart-id="shiftVariance"><canvas id="shiftVariance"></canvas></div>
      </div>
    </div>
  </div>

  <div class="section" id="sec-portfolio">
    <h2>Vista de Cartera y Riesgo</h2>
    <div class="section-subtitle">Valor capturable, downside y distribución de iniciativas.</div>
    <div class="chart-grid">
      <div class="chart-card">
        <div class="chart-kicker">Escenarios</div>
        <div class="chart-title">Valor descontado vs valor downside-adjusted</div>
        <div class="chart-note">Compara valor bruto frente a valor robusto para evitar decisiones optimistas sin protección de downside.</div>
        <div class="chart-canvas-wrap" data-chart-id="scenarioValue"><canvas id="scenarioValue"></canvas></div>
      </div>
      <div class="chart-card">
        <div class="chart-kicker">Secuenciación</div>
        <div class="chart-title">Distribución de iniciativas por ola</div>
        <div class="chart-note">Resume la carga de ejecución por ola para comprobar si la cartera es absorbible por la organización.</div>
        <div class="chart-canvas-wrap" data-chart-id="portfolioWave"><canvas id="portfolioWave"></canvas></div>
      </div>
      <div class="chart-card">
        <div class="chart-kicker">Trade-off</div>
        <div class="chart-title">Prioridad compuesta vs viabilidad</div>
        <div class="chart-note">Sitúa cada iniciativa entre impacto y facilidad de implementación para apoyar el comité de secuenciación.</div>
        <div class="chart-canvas-wrap" data-chart-id="priorityScatter"><canvas id="priorityScatter"></canvas></div>
      </div>
    </div>
  </div>

  <div class="section" id="sec-table">
    <h2>Tabla Final de Priorización</h2>
    <div class="section-subtitle">Filtra por iniciativa, línea o decisión para preparar el comité.</div>
    <div class="table-toolbar">
      <div class="table-toolbar-main">
        <input id="tableSearch" type="text" placeholder="Buscar iniciativa, línea, clase o decisión..." />
        <select id="waveFilter" aria-label="Filtrar por ola">
          <option value="all">Todas las olas</option>
          {wave_options}
        </select>
      </div>
      <div class="table-status">
        <div id="tableCount" class="table-count">0 iniciativas</div>
      </div>
    </div>
    <div class="table-filters" aria-label="Filtros rápidos de cartera">
      <button class="filter-chip active" type="button" data-scope-filter="all">Todas</button>
      <button class="filter-chip" type="button" data-scope-filter="selected">En cartera</button>
      <button class="filter-chip" type="button" data-scope-filter="backlog">Backlog</button>
    </div>
    <div class="table-wrap"><table id="tbl"></table></div>
  </div>

  <div class="section" id="sec-decision">
    <h2>Decisión Ejecutiva</h2>
    <div class="section-subtitle">Síntesis accionable para comité: foco, riesgo y valor.</div>
    <div class="decision-card">
      <div class="decision-item">
        <b>Línea prioritaria</b>
        <span>{decision['linea_prioritaria']}</span>
        <div class="badge critical">Criticidad {decision['line_criticality_score']:.2f}</div>
      </div>
      <div class="decision-item">
        <b>Equipo prioritario</b>
        <span>{decision['equipo_prioritario']}</span>
        <div class="badge warn">Anomalía {decision['equipment_energy_anomaly_score']:.2f}</div>
      </div>
      <div class="decision-item">
        <b>Ahorro top-5 ahora</b>
        <span>{decision['ahorro_now_top5']:,.0f} EUR/año</span>
        <div class="badge ok">Capturable inmediato</div>
      </div>
      <div class="decision-item">
        <b>Riesgo de inacción 12m</b>
        <span>{decision['risk_of_inaction_12m']:,.0f} EUR</span>
        <div class="badge critical">Coste por demora</div>
      </div>
      <div class="decision-item">
        <b>Nota de comité</b>
        <span>{decision['committee_note']}</span>
      </div>
    </div>
  </div>
</div>

<div id="methodologyModal" class="modal-backdrop" role="dialog" aria-modal="true" aria-labelledby="methodologyTitle">
  <div class="modal-card">
    <div class="modal-header">
      <div id="methodologyTitle" class="modal-title">Metodología y gobernanza</div>
      <button id="btnCloseMethodology" class="modal-close" type="button" aria-label="Cerrar">×</button>
    </div>
    <div class="modal-body">
      {methodology_html}
    </div>
  </div>
</div>

<script>const DATA={data_json};</script>
<script>
const CHART_IDS=['secTrend','oeeTrend','energyCostLine','equipmentAnomaly','rootCause','shiftVariance','scenarioValue','portfolioWave','priorityScatter'];
const THEME_STORAGE_KEY='industrial_dashboard_theme';
let chartInstances=[];

function cssVar(name){{
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}}
function chartPalette(){{
  return {{
    text:cssVar('--chart-text') || '#334e68',
    grid:cssVar('--chart-grid') || 'rgba(51,78,104,.16)',
    tooltipBg:cssVar('--chart-tooltip-bg') || '#ffffff',
    tooltipText:cssVar('--chart-tooltip-text') || '#102a43'
  }};
}}
function ensureCanvas(id){{
  const current=document.getElementById(id);
  if(current) return current;
  const wrap=document.querySelector(`.chart-canvas-wrap[data-chart-id="${{id}}"]`);
  if(!wrap) return null;
  wrap.innerHTML=`<canvas id="${{id}}"></canvas>`;
  return wrap.querySelector('canvas');
}}
function destroyCharts(){{
  chartInstances.forEach((chart)=>{{try{{chart.destroy();}}catch(_e){{}}}});
  chartInstances=[];
}}
function setChartFallback(message){{
  CHART_IDS.forEach((id)=>{{
    const wrap=document.querySelector(`.chart-canvas-wrap[data-chart-id="${{id}}"]`);
    if(!wrap) return;
    wrap.innerHTML=`<div style="display:flex;align-items:center;justify-content:center;height:100%;text-align:center;color:${{cssVar('--ink-soft') || '#486581'}};font-size:12px;padding:10px;border:1px dashed ${{cssVar('--line-strong') || '#bfd2e6'}};border-radius:12px;background:${{cssVar('--surface-elev') || '#f8fbfe'}}">${{message}}</div>`;
  }});
}}
function setChartFallbackSingle(id,message){{
  const wrap=document.querySelector(`.chart-canvas-wrap[data-chart-id="${{id}}"]`);
  if(!wrap) return;
  wrap.innerHTML=`<div style="display:flex;align-items:center;justify-content:center;height:100%;text-align:center;color:${{cssVar('--ink-soft') || '#486581'}};font-size:12px;padding:10px;border:1px dashed ${{cssVar('--line-strong') || '#bfd2e6'}};border-radius:12px;background:${{cssVar('--surface-elev') || '#f8fbfe'}}">${{message}}</div>`;
}}
function getBaseOptions(){{
  const palette=chartPalette();
  return {{
    responsive:true,
    maintainAspectRatio:false,
    animation:false,
    normalized:true,
    layout:{{padding:{{left:2,right:6,top:2,bottom:0}}}},
    interaction:{{mode:'index',intersect:false}},
    plugins:{{
      legend:{{
        display:true,
        position:'bottom',
        align:'start',
        labels:{{boxWidth:10,padding:8,usePointStyle:true,font:{{size:11}},color:palette.text}}
      }},
      tooltip:{{
        enabled:true,
        backgroundColor:palette.tooltipBg,
        titleColor:palette.tooltipText,
        bodyColor:palette.tooltipText,
        borderColor:palette.grid,
        borderWidth:1
      }}
    }},
    scales:{{
      x:{{grid:{{color:palette.grid}},ticks:{{color:palette.text,autoSkip:true,maxTicksLimit:8,maxRotation:45,minRotation:0,font:{{size:11}}}}}},
      y:{{grid:{{color:palette.grid}},ticks:{{color:palette.text,maxTicksLimit:8,font:{{size:11}}}}}}
    }}
  }};
}}
function truncateLabel(value,maxLen=24){{
  const txt=String(value ?? '');
  return txt.length>maxLen ? txt.slice(0,maxLen-1)+'…' : txt;
}}
function toFiniteNumber(value){{
  if(value === null || value === undefined) return null;
  const cleaned=String(value).replace(/,/g,'');
  const num=Number(cleaned);
  return Number.isFinite(num) ? num : null;
}}
function sanitizeSeries(values){{
  return (values ?? []).map((v)=>toFiniteNumber(v));
}}
function seriesBounds(datasets, fallbackMin=0, fallbackMax=1){{
  const vals=(datasets ?? [])
    .flatMap((d)=>sanitizeSeries(d?.data ?? []))
    .filter((v)=>v !== null);
  if(!vals.length) return {{min:fallbackMin,max:fallbackMax,hasData:false}};
  return {{min:Math.min(...vals),max:Math.max(...vals),hasData:true}};
}}
function denseCategoryOptions(labels, horizontal=false){{
  const n=labels.length;
  const maxTicks=n>14?7:(n>10?8:10);
  if(horizontal){{
    return {{
      indexAxis:'y',
      scales:{{
        x:{{ticks:{{maxTicksLimit:6,font:{{size:11}}}}}},
        y:{{ticks:{{autoSkip:false,callback:(v,i)=>truncateLabel(labels[i]??'',22),font:{{size:11}}}}}}
      }}
    }};
  }}
  return {{
    scales:{{
      x:{{
        ticks:{{
          autoSkip:true,
          maxTicksLimit:maxTicks,
          maxRotation:n>9?40:0,
          minRotation:n>9?25:0,
          callback:(v,i)=>truncateLabel(labels[i]??'',18),
          font:{{size:11}}
        }}
      }},
      y:{{ticks:{{maxTicksLimit:7,font:{{size:11}}}}}}
    }}
  }};
}}
function mergeOptions(custom={{}}){{
  const out=JSON.parse(JSON.stringify(getBaseOptions()));
  if(custom.plugins){{
    out.plugins={{...out.plugins,...custom.plugins}};
    if(custom.plugins.legend){{
      out.plugins.legend={{...out.plugins.legend,...custom.plugins.legend}};
      if(custom.plugins.legend.labels){{
        out.plugins.legend.labels={{...out.plugins.legend.labels,...custom.plugins.legend.labels}};
      }}
    }}
    if(custom.plugins.tooltip){{
      out.plugins.tooltip={{...out.plugins.tooltip,...custom.plugins.tooltip}};
    }}
  }}
  if(custom.scales){{
    out.scales={{...out.scales,...custom.scales}};
    if(custom.scales.x){{
      out.scales.x={{...out.scales.x,...custom.scales.x}};
      if(custom.scales.x.ticks){{
        out.scales.x.ticks={{...out.scales.x.ticks,...custom.scales.x.ticks}};
      }}
    }}
    if(custom.scales.y){{
      out.scales.y={{...out.scales.y,...custom.scales.y}};
      if(custom.scales.y.ticks){{
        out.scales.y.ticks={{...out.scales.y.ticks,...custom.scales.y.ticks}};
      }}
    }}
  }}
  return {{...out,...custom,plugins:out.plugins,scales:out.scales}};
}}
function renderChart(id,type,labels,datasets,opts={{}}){{
  const canvas=ensureCanvas(id);
  if(!canvas) return null;
  const chart=new Chart(canvas,{{type,data:{{labels,datasets}},options:mergeOptions(opts)}});
  chartInstances.push(chart);
  return chart;
}}
function renderAllCharts(){{
  if (typeof window.Chart === 'undefined') {{
    setChartFallback('No se pudo cargar Chart.js. Abre este HTML con conexión a internet o ejecuta el dashboard desde un entorno con acceso al CDN.');
    return;
  }}
  destroyCharts();
  const palette=chartPalette();
  Chart.defaults.font.family = "'IBM Plex Sans','Avenir Next','Segoe UI',sans-serif";
  Chart.defaults.color = palette.text;
  Chart.defaults.borderColor = palette.grid;

  const colors=['#0f766e','#1d4ed8','#b45309','#b91c1c','#7c3aed','#0891b2','#16a34a','#334155'];
  try {{
    const secDatasets=DATA.charts.sec_trend.datasets.map((d,i)=>({{
      label:d.label,
      data:sanitizeSeries(d.data),
      borderColor:colors[i%colors.length],
      fill:false,
      tension:.28,
      cubicInterpolationMode:'monotone',
      pointRadius:0,
      pointHoverRadius:4,
      pointHitRadius:10,
      borderWidth:2.5
    }}));
    const secBounds=seriesBounds(secDatasets,0,100);
    const secPad=(secBounds.max-secBounds.min)*0.08;
    if(!secBounds.hasData){{
      setChartFallbackSingle('secTrend','Sin datos disponibles para SEC.');
    }} else {{
      renderChart(
        'secTrend',
        'line',
        DATA.charts.sec_trend.labels,
        secDatasets,
        {{
          ...denseCategoryOptions(DATA.charts.sec_trend.labels,false),
          scales:{{
            y:{{
              min:secBounds.min-secPad,
              max:secBounds.max+secPad,
              ticks:{{maxTicksLimit:7,font:{{size:11}}}}
            }}
          }}
        }}
      );
    }}
    const oeeDatasets=DATA.charts.oee_trend.datasets.map((d,i)=>({{
      label:d.label,
      data:sanitizeSeries(d.data),
      borderColor:colors[i%colors.length],
      fill:false,
      tension:.28,
      cubicInterpolationMode:'monotone',
      pointRadius:0,
      pointHoverRadius:4,
      pointHitRadius:10,
      borderWidth:2.5
    }}));
    const oeeBounds=seriesBounds(oeeDatasets,0.8,1.0);
    const oeeMin=Math.max(0, (oeeBounds.hasData ? oeeBounds.min : 0.8)-0.01);
    const oeeMax=Math.min(1, (oeeBounds.hasData ? oeeBounds.max : 1.0)+0.01);
    if(!oeeBounds.hasData){{
      setChartFallbackSingle('oeeTrend','Sin datos disponibles para OEE.');
    }} else {{
      renderChart(
        'oeeTrend',
        'line',
        DATA.charts.oee_trend.labels,
        oeeDatasets,
        {{
          ...denseCategoryOptions(DATA.charts.oee_trend.labels,false),
          scales:{{
            y:{{
              min:oeeMin,
              max:oeeMax,
              ticks:{{maxTicksLimit:7,font:{{size:11}}}}
            }}
          }}
        }}
      );
    }}

    const energyLabels=DATA.charts.energy_cost_line.map(r=>r.linea_id);
    const energyValues=DATA.charts.energy_cost_line.map(r=>toFiniteNumber(r.coste_energia_total));
    if(!energyValues.some(v=>v!==null)){{
      setChartFallbackSingle('energyCostLine','Sin datos disponibles de coste energético.');
    }} else {{
      renderChart(
        'energyCostLine',
        'bar',
        energyLabels,
        [{{label:'Coste energía (EUR)',data:energyValues,backgroundColor:'#1d4ed8',borderRadius:8,borderSkipped:false,maxBarThickness:24}}],
        {{...denseCategoryOptions(energyLabels,true),plugins:{{legend:{{display:false}}}},scales:{{x:{{beginAtZero:true,ticks:{{maxTicksLimit:6,font:{{size:11}}}}}}}}}}
      );
    }}

    const eqLabels=DATA.charts.equipment_anomaly.map(r=>r.equipo_id);
    const eqValues=DATA.charts.equipment_anomaly.map(r=>toFiniteNumber(r.anomaly_score));
    if(!eqValues.some(v=>v!==null)){{
      setChartFallbackSingle('equipmentAnomaly','Sin datos disponibles de anomalía.');
    }} else {{
      renderChart(
        'equipmentAnomaly',
        'bar',
        eqLabels,
        [{{label:'Score de anomalía',data:eqValues,backgroundColor:'#b91c1c',borderRadius:8,borderSkipped:false,maxBarThickness:24}}],
        {{...denseCategoryOptions(eqLabels,true),plugins:{{legend:{{display:false}}}},scales:{{x:{{beginAtZero:true,ticks:{{maxTicksLimit:6,font:{{size:11}}}}}}}}}}
      );
    }}

    const causeLabels=DATA.charts.root_cause.map(r=>r.causa_parada);
    const causeValues=DATA.charts.root_cause.map(r=>toFiniteNumber(r.loss_root_cause_score));
    if(!causeValues.some(v=>v!==null)){{
      setChartFallbackSingle('rootCause','Sin datos disponibles de causas raíz.');
    }} else {{
      renderChart(
        'rootCause',
        'bar',
        causeLabels,
        [{{label:'Score de pérdida por causa raíz',data:causeValues,backgroundColor:'#0f766e',borderRadius:8,borderSkipped:false,maxBarThickness:24}}],
        {{...denseCategoryOptions(causeLabels,true),plugins:{{legend:{{display:false}}}},scales:{{x:{{beginAtZero:true,ticks:{{maxTicksLimit:6,font:{{size:11}}}}}}}}}}
      );
    }}

    const shiftLabels=DATA.charts.shift_variance.map(r=>r.label);
    const shiftValues=DATA.charts.shift_variance.map(r=>toFiniteNumber(r.shift_variance_score));
    if(!shiftValues.some(v=>v!==null)){{
      setChartFallbackSingle('shiftVariance','Sin datos disponibles de variabilidad.');
    }} else {{
      renderChart(
        'shiftVariance',
        'bar',
        shiftLabels,
        [{{label:'Score de variabilidad por turno',data:shiftValues,backgroundColor:'#334155',borderRadius:8,borderSkipped:false,maxBarThickness:24}}],
        {{...denseCategoryOptions(shiftLabels,true),plugins:{{legend:{{display:false}}}},scales:{{x:{{beginAtZero:true,ticks:{{maxTicksLimit:6,font:{{size:11}}}}}}}}}}
      );
    }}

    const scenLabels=DATA.charts.scenario_value.map(r=>r.macro_scenario);
    const scenDisc=DATA.charts.scenario_value.map(r=>toFiniteNumber(r.discounted_value));
    const scenDown=DATA.charts.scenario_value.map(r=>toFiniteNumber(r.downside_adjusted_value));
    if(!scenDisc.some(v=>v!==null) && !scenDown.some(v=>v!==null)){{
      setChartFallbackSingle('scenarioValue','Sin datos disponibles de escenarios.');
    }} else {{
      renderChart(
        'scenarioValue',
        'bar',
        scenLabels,
        [
          {{label:'Valor descontado',data:scenDisc,backgroundColor:'#0ea5e9',borderRadius:8,borderSkipped:false,maxBarThickness:32}},
          {{label:'Valor ajustado por downside',data:scenDown,backgroundColor:'#0f766e',borderRadius:8,borderSkipped:false,maxBarThickness:32}}
        ],
        {{...denseCategoryOptions(scenLabels,false)}}
      );
    }}

    const waveLabels=DATA.charts.portfolio_wave.map(r=>r.portfolio_wave);
    const waveValues=DATA.charts.portfolio_wave.map(r=>toFiniteNumber(r.n_iniciativas));
    if(!waveValues.some(v=>v!==null)){{
      setChartFallbackSingle('portfolioWave','Sin datos disponibles de olas.');
    }} else {{
      renderChart(
        'portfolioWave',
        'doughnut',
        waveLabels,
        [{{data:waveValues,backgroundColor:['#047857','#1d4ed8','#7c3aed','#64748b'],borderWidth:0}}],
        {{cutout:'68%',plugins:{{legend:{{display:true,position:'bottom'}}}}}}
      );
    }}

    const scatterPoints=DATA.charts.priority_scatter
      .map(r=>({{x:toFiniteNumber(r.implementation_feasibility_score),y:toFiniteNumber(r.improvement_priority_index),r:toFiniteNumber(r.bubble_r)}}))
      .filter(p=>p.x!==null && p.y!==null && p.r!==null);
    if(!scatterPoints.length){{
      setChartFallbackSingle('priorityScatter','Sin datos disponibles de priorización.');
    }} else {{
      renderChart(
        'priorityScatter',
        'bubble',
        [],
        [{{
          label:'Prioridad vs viabilidad',
          data:scatterPoints,
          backgroundColor:'rgba(180,83,9,.72)',
          borderColor:'rgba(140,65,4,.58)',
          borderWidth:1
        }}],
        {{
          plugins:{{legend:{{display:false}}}},
          scales:{{
            x:{{title:{{display:true,text:'Viabilidad implementación'}},ticks:{{maxTicksLimit:7,font:{{size:11}}}}}},
            y:{{title:{{display:true,text:'Prioridad compuesta'}},ticks:{{maxTicksLimit:7,font:{{size:11}}}}}}
          }}
        }}
      );
    }}
  }} catch (error) {{
    console.error('Dashboard chart rendering failed:', error);
    setChartFallback('Se produjo un error al renderizar los gráficos. Regenera el dashboard o revisa la consola del navegador.');
  }}
}}
function getPreferredTheme(){{
  const stored=localStorage.getItem(THEME_STORAGE_KEY);
  if(stored==='light' || stored==='dark') return stored;
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}}
function applyTheme(theme, persist=false){{
  document.documentElement.setAttribute('data-theme',theme);
  const btnTheme=document.getElementById('btnTheme');
  const btnThemeLabel=document.getElementById('btnThemeLabel');
  const isDark=theme==='dark';
  if(btnTheme) btnTheme.setAttribute('aria-pressed', String(isDark));
  if(btnThemeLabel) btnThemeLabel.textContent=isDark ? 'Modo claro' : 'Modo oscuro';
  if(persist) localStorage.setItem(THEME_STORAGE_KEY, theme);
  renderAllCharts();
}}
const btnTheme=document.getElementById('btnTheme');
if(btnTheme){{
  btnTheme.addEventListener('click',()=>{{
    const current=document.documentElement.getAttribute('data-theme')==='dark' ? 'dark' : 'light';
    applyTheme(current==='dark' ? 'light' : 'dark', true);
  }});
}}
applyTheme(getPreferredTheme(), false);
window.matchMedia('(prefers-color-scheme: dark)').addEventListener?.('change',()=>{{
  if(!localStorage.getItem(THEME_STORAGE_KEY)) applyTheme(getPreferredTheme(), false);
}});

const columns=['iniciativa_id','linea_id','initiative_class','annual_saving_proxy','npv_risk_adjusted','payback_months','improvement_priority_index','initiative_tier','portfolio_wave','decision_rule','selected_portfolio_flag'];
const columnLabels={{
  iniciativa_id:'Iniciativa',
  linea_id:'Línea',
  initiative_class:'Clase',
  annual_saving_proxy:'Ahorro anual',
  npv_risk_adjusted:'NPV ajustado',
  payback_months:'Payback',
  improvement_priority_index:'Índice prioridad',
  initiative_tier:'Tier',
  portfolio_wave:'Ola',
  decision_rule:'Decisión',
  selected_portfolio_flag:'En cartera'
}};
const table=document.getElementById('tbl');
const search=document.getElementById('tableSearch');
const waveFilter=document.getElementById('waveFilter');
const tableCount=document.getElementById('tableCount');
const scopeButtons=[...document.querySelectorAll('[data-scope-filter]')];
const rows=DATA.table;
const btnMethodology=document.getElementById('btnMethodology');
const btnPrint=document.getElementById('btnPrint');
const methodologyModal=document.getElementById('methodologyModal');
const btnCloseMethodology=document.getElementById('btnCloseMethodology');
let activeScope='all';

function formatNumber(value, digits=0){{
  const num=toFiniteNumber(value);
  if(num===null) return '—';
  return new Intl.NumberFormat('es-ES', {{
    minimumFractionDigits: digits,
    maximumFractionDigits: digits
  }}).format(num);
}}
function decisionBadgeClass(value){{
  const text=String(value ?? '').toLowerCase();
  if(text.includes('ahora')) return 'critical';
  if(text.includes('siguiente')) return 'warn';
  return 'muted';
}}
function renderBadge(value, tone){{
  return `<span class="table-badge ${{tone}}">${{value}}</span>`;
}}
function formatCell(column, value){{
  switch(column){{
    case 'annual_saving_proxy':
    case 'npv_risk_adjusted':
      return `${{formatNumber(value,0)}} EUR`;
    case 'payback_months':
      return `${{formatNumber(value,0)}} meses`;
    case 'improvement_priority_index':
      return formatNumber(value,1);
    case 'selected_portfolio_flag':
      return Number(value) === 1 ? renderBadge('Sí','ok') : renderBadge('No','muted');
    case 'decision_rule':
      return renderBadge(value, decisionBadgeClass(value));
    case 'initiative_class':
      return renderBadge(value, 'muted');
    default:
      return value ?? '—';
  }}
}}
function renderTable(filteredRows){{
  if(tableCount) tableCount.textContent=`${{filteredRows.length}} iniciativas`;
  table.innerHTML='<thead><tr>'+columns.map(c=>`<th class="${{['annual_saving_proxy','npv_risk_adjusted','payback_months','improvement_priority_index'].includes(c) ? 'cell-num' : ''}}">${{columnLabels[c] ?? c}}</th>`).join('')+'</tr></thead><tbody>'+filteredRows.map(r=>'<tr>'+columns.map(c=>`<td class="${{['annual_saving_proxy','npv_risk_adjusted','payback_months','improvement_priority_index'].includes(c) ? 'cell-num' : ''}} ${{['iniciativa_id','linea_id'].includes(c) ? 'cell-key' : ''}}">${{formatCell(c,r[c])}}</td>`).join('')+'</tr>').join('')+'</tbody>';
}}
function applyTableFilters(){{
  const q=search.value.trim().toLowerCase();
  const wave=waveFilter?.value ?? 'all';
  const filtered=rows.filter((row)=>{{
    const matchesScope =
      activeScope === 'all'
      || (activeScope === 'selected' && Number(row.selected_portfolio_flag) === 1)
      || (activeScope === 'backlog' && Number(row.selected_portfolio_flag) !== 1);
    const matchesWave = wave === 'all' || String(row.portfolio_wave) === wave;
    const matchesText = !q || columns.some((column)=>String(row[column]).toLowerCase().includes(q));
    return matchesScope && matchesWave && matchesText;
  }});
  renderTable(filtered);
}}
renderTable(rows);
search.addEventListener('input',applyTableFilters);
waveFilter?.addEventListener('change',applyTableFilters);
scopeButtons.forEach((button)=>{{
  button.addEventListener('click',()=>{{
    activeScope=button.dataset.scopeFilter || 'all';
    scopeButtons.forEach((chip)=>chip.classList.toggle('active', chip===button));
    applyTableFilters();
  }});
}});

function openMethodology(){{
  methodologyModal.classList.add('show');
  document.body.style.overflow='hidden';
}}
function closeMethodology(){{
  methodologyModal.classList.remove('show');
  document.body.style.overflow='';
}}
btnMethodology.addEventListener('click',openMethodology);
btnCloseMethodology.addEventListener('click',closeMethodology);
methodologyModal.addEventListener('click',(ev)=>{{if(ev.target===methodologyModal)closeMethodology();}});
document.addEventListener('keydown',(ev)=>{{if(ev.key==='Escape'&&methodologyModal.classList.contains('show'))closeMethodology();}});
btnPrint.addEventListener('click',()=>window.print());
</script>
</body>
</html>"""


def _cleanup_legacy_outputs() -> tuple[int, int]:
    removed = 0
    remaining = 0
    for path in LEGACY_DASHBOARD_OUTPUTS:
        if path.exists():
            if path.is_file():
                path.unlink()
                removed += 1
            else:
                remaining += 1
    for path in LEGACY_DASHBOARD_OUTPUTS:
        if path.exists():
            remaining += 1
    return removed, remaining


def build_dashboard() -> Path:
    OUTPUT_DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    dataset, metrics = _prepare_canonical_dataset()
    CANONICAL_DATASET_JSON.write_text(_compact_json(dataset), encoding="utf-8")

    html = _render_canonical_html(dataset)
    CANONICAL_HTML.write_text(html, encoding="utf-8")
    DOCS_CANONICAL_HTML.parent.mkdir(parents=True, exist_ok=True)
    DOCS_CANONICAL_HTML.write_text(html, encoding="utf-8")

    removed_legacy, remaining_legacy = _cleanup_legacy_outputs()
    canonical_size = int(CANONICAL_HTML.stat().st_size)
    dataset_size = int(CANONICAL_DATASET_JSON.stat().st_size)

    competing_html = [
        p.name
        for p in OUTPUT_DASHBOARD_DIR.glob("dashboard_*.html")
        if p.name != CANONICAL_HTML.name
    ]

    audit = pd.DataFrame(
        [
            {"metric": "canonical_html_bytes", "value": canonical_size},
            {"metric": "canonical_html_threshold_bytes", "value": MAX_CANONICAL_SIZE_BYTES},
            {"metric": "canonical_under_threshold_flag", "value": int(canonical_size <= MAX_CANONICAL_SIZE_BYTES)},
            {"metric": "canonical_dataset_bytes", "value": dataset_size},
            {"metric": "legacy_outputs_removed_count", "value": removed_legacy},
            {"metric": "legacy_outputs_remaining_count", "value": remaining_legacy},
            {"metric": "no_competing_html_flag", "value": int(len(competing_html) == 0)},
            {"metric": "line_day_rows_raw", "value": metrics["line_day_rows_raw"]},
            {"metric": "equipment_rows_raw", "value": metrics["equipment_rows_raw"]},
            {"metric": "invest_rows_raw", "value": metrics["invest_rows_raw"]},
            {"metric": "payload_records", "value": metrics["payload_records"]},
        ]
    )
    audit.to_csv(SERVING_AUDIT_CSV, index=False)
    return CANONICAL_HTML


if __name__ == "__main__":
    build_dashboard()
