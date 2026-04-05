from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from .config import DATA_PROCESSED_DIR, OUTPUT_DASHBOARD_DIR, OUTPUT_REPORTS_DIR


MAX_CANONICAL_SIZE_BYTES = 4_000_000
CANONICAL_HTML = OUTPUT_DASHBOARD_DIR / "dashboard_inteligencia_operativa.html"
CANONICAL_DATASET_JSON = DATA_PROCESSED_DIR / "dashboard_canonical_dataset.json"
SERVING_AUDIT_CSV = DATA_PROCESSED_DIR / "dashboard_serving_audit.csv"
SERVING_REPORT_MD = OUTPUT_REPORTS_DIR / "dashboard_serving_refactor.md"

LEGACY_DASHBOARD_OUTPUTS = [
    OUTPUT_DASHBOARD_DIR / "dashboard_executive_light.html",
    OUTPUT_DASHBOARD_DIR / "dashboard_full_offline.html",
    OUTPUT_DASHBOARD_DIR / "dashboard_board_pack.html",
    DATA_PROCESSED_DIR / "dashboard_exec_dataset.json",
    DATA_PROCESSED_DIR / "dashboard_full_dataset.json",
]


def _compact_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


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
            {"name": "Production Volume", "value": f"{_fmt(snapshot['production_volume'], ',.0f')} t"},
            {"name": "OEE sintético", "value": f"{_fmt(snapshot['oee_synthetic'], '.3f')}"},
            {"name": "SEC medio", "value": f"{_fmt(snapshot['sec'], '.1f')} kWh/t"},
            {"name": "Pérdidas económicas proxy", "value": f"{_fmt(snapshot['loss_value_proxy'], ',.0f')} EUR"},
            {"name": "Ahorro anual proxy", "value": f"{_fmt(snapshot['annual_saving_proxy'], ',.0f')} EUR/año"},
            {"name": "NPV ajustado por riesgo", "value": f"{_fmt(snapshot['portfolio_npv_risk_adjusted'], ',.0f')} EUR"},
            {"name": "Valor downside-adjusted", "value": f"{_fmt(downside_value, ',.0f')} EUR"},
            {"name": "Cost of delay 12m", "value": f"{_fmt(snapshot.get('cost_of_delay_12m_portfolio', 0.0), ',.0f')} EUR"},
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
    priority_kpis = {"OEE sintético", "SEC medio", "Pérdidas económicas proxy", "Ahorro anual proxy", "NPV ajustado por riesgo"}
    text_heavy_kpis = {"Validation state"}
    kpi_html = "".join(
        (
            f"<div class='kpi-card {'kpi-priority' if row['name'] in priority_kpis else ''} "
            f"{'kpi-textual' if row['name'] in text_heavy_kpis else ''} "
            f"{'kpi-long' if len(str(row['value'])) > 22 else ''}'>"
            f"<div class='kpi-name'>{row['name']}</div>"
            f"<div class='kpi-value'>{row['value']}</div>"
            "</div>"
        )
        for row in dataset["kpis"]
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
    data_json = _compact_json(dataset)
    return f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Cockpit Ejecutivo Industrial (Canónico)</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&family=Space+Grotesk:wght@500;700&display=swap');
:root{{
  --bg:#ecf2f6;
  --bg-2:#dfe9f1;
  --panel:#ffffff;
  --panel-soft:#f6fafc;
  --surface-elev:#f8fbff;
  --ink:#102a43;
  --ink-soft:#486581;
  --line:#d4dee8;
  --line-strong:#c2d3e4;
  --accent:#0f766e;
  --accent-2:#1d4ed8;
  --warn:#b45309;
  --chart-grid:rgba(39,73,109,.16);
  --chart-text:#2f4f6d;
  --chart-tooltip-bg:#ffffff;
  --chart-tooltip-text:#102a43;
  --radius-xl:18px;
  --radius-lg:14px;
  --shadow-sm:0 4px 12px rgba(16,42,67,.08);
  --shadow-md:0 10px 24px rgba(16,42,67,.12);
}}
[data-theme='dark']{{
  --bg:#0b1420;
  --bg-2:#0f1b2d;
  --panel:#111e31;
  --panel-soft:#13233a;
  --surface-elev:#182b46;
  --ink:#e8f0fa;
  --ink-soft:#9eb7d1;
  --line:#25415f;
  --line-strong:#315778;
  --accent:#19a49a;
  --accent-2:#4a7cff;
  --warn:#d08a32;
  --chart-grid:rgba(173,204,237,.18);
  --chart-text:#bdd2e9;
  --chart-tooltip-bg:#1b3049;
  --chart-tooltip-text:#e8f0fa;
  --shadow-sm:0 4px 12px rgba(0,0,0,.28);
  --shadow-md:0 12px 28px rgba(0,0,0,.36);
}}
*{{box-sizing:border-box}}
html{{scroll-behavior:smooth}}
body{{font-family:'IBM Plex Sans','Avenir Next','Segoe UI',sans-serif;margin:0;color:var(--ink);background:
radial-gradient(1200px 500px at -10% -20%,rgba(49,177,160,.22) 0%,transparent 55%),
radial-gradient(900px 450px at 120% -20%,rgba(62,111,232,.18) 0%,transparent 50%),
linear-gradient(180deg,var(--bg) 0%,var(--bg-2) 100%);overflow-x:hidden;transition:background .22s ease,color .22s ease}}
.container{{position:relative;max-width:1320px;margin:0 auto;padding:14px 16px 20px}}
.ux-toolbar{{position:sticky;top:8px;z-index:80;display:flex;gap:10px;justify-content:flex-end;margin-bottom:10px}}
.ux-btn{{border:1px solid var(--line-strong);background:color-mix(in srgb, var(--panel) 88%, transparent);color:var(--ink);border-radius:14px;padding:8px 14px;font-size:13px;font-weight:700;cursor:pointer;transition:all .18s ease;box-shadow:var(--shadow-sm);backdrop-filter:blur(6px)}}
.ux-btn:hover{{transform:translateY(-1px);border-color:color-mix(in srgb, var(--accent-2) 45%, var(--line-strong));box-shadow:var(--shadow-md)}}
.ux-btn-theme{{display:inline-flex;align-items:center;gap:8px}}
.theme-dot{{width:10px;height:10px;border-radius:50%;background:linear-gradient(180deg,var(--accent),var(--accent-2));display:inline-block}}
.quick-nav{{display:flex;flex-wrap:wrap;gap:8px;margin:8px 0 12px}}
.quick-nav a{{text-decoration:none;font-size:12px;font-weight:700;color:var(--ink-soft);background:var(--surface-elev);border:1px solid var(--line-strong);border-radius:999px;padding:7px 12px;transition:all .16s ease}}
.quick-nav a:hover{{transform:translateY(-1px);border-color:color-mix(in srgb, var(--accent-2) 46%, var(--line-strong));color:var(--ink)}}
.header{{position:relative;overflow:hidden;background:linear-gradient(128deg,#0d766e 0%,#176fb2 45%,#264fca 100%);color:#fff;padding:12px 14px 13px;border-radius:var(--radius-xl);box-shadow:var(--shadow-md)}}
.header::after{{content:'';position:absolute;right:-120px;top:-110px;width:360px;height:360px;background:radial-gradient(circle,rgba(255,255,255,.26) 0%,rgba(255,255,255,0) 65%)}}
.header h1{{position:relative;font-family:'Space Grotesk','Avenir Next','Segoe UI',sans-serif;font-size:clamp(22px,2.3vw,30px);line-height:1.12;letter-spacing:-.015em;margin:0 0 5px;max-width:1180px}}
.header-subtitle{{position:relative;font-size:13px;opacity:.96;max-width:980px}}
.header-stats{{position:relative;margin-top:6px;font-size:12px;opacity:.95}}
.callouts{{position:relative;display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:8px;margin-top:10px}}
.callout{{background:rgba(255,255,255,.18);border:1px solid rgba(255,255,255,.28);padding:8px 9px;border-radius:10px;font-size:12px;line-height:1.28;backdrop-filter:blur(2px)}}
.kpi-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:8px;margin:10px 0}}
.kpi-card{{position:relative;background:var(--panel);border-radius:12px;padding:8px 8px 9px;border:1px solid var(--line);box-shadow:var(--shadow-sm);overflow:hidden;min-height:84px;display:flex;flex-direction:column;justify-content:flex-start}}
.kpi-card::before{{content:'';position:absolute;left:0;top:0;width:100%;height:4px;background:linear-gradient(90deg,#11867d,#2f72ca)}}
.kpi-card.kpi-priority{{border-color:#a9d7d2;box-shadow:0 8px 22px rgba(15,118,110,.16)}}
.kpi-card.kpi-priority::before{{background:linear-gradient(90deg,#0f766e,#2d66d1)}}
.kpi-name{{font-size:11px;font-weight:600;color:var(--ink-soft)}}
.kpi-value{{font-family:'Space Grotesk','Avenir Next','Segoe UI',sans-serif;font-size:clamp(15px,1vw,19px);font-weight:700;letter-spacing:-.01em;color:var(--ink);line-height:1.16;margin-top:3px;overflow-wrap:anywhere;word-break:break-word}}
.kpi-card.kpi-long .kpi-value{{font-size:clamp(13px,.9vw,17px);line-height:1.2}}
.kpi-card.kpi-textual .kpi-value{{font-family:'IBM Plex Sans','Avenir Next','Segoe UI',sans-serif;font-size:clamp(12px,.95vw,16px);line-height:1.18;overflow-wrap:anywhere}}
.section{{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:10px;margin:9px 0;box-shadow:var(--shadow-sm);overflow:hidden}}
.section h2{{font-family:'Space Grotesk','Avenir Next','Segoe UI',sans-serif;font-size:clamp(20px,1.5vw,24px);letter-spacing:-.01em;color:var(--ink);margin:1px 0 8px;display:flex;align-items:center;gap:8px}}
.section h2::before{{content:'';display:inline-block;width:10px;height:28px;border-radius:99px;background:linear-gradient(180deg,#0f766e,#1d4ed8)}}
.chart-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(360px,1fr));gap:10px;align-items:stretch}}
.chart-card{{background:var(--panel-soft);border:1px solid var(--line);border-radius:10px;padding:10px;min-width:0;overflow:hidden}}
.chart-title{{font-size:12px;font-weight:700;color:var(--ink-soft);margin-bottom:8px;line-height:1.3}}
.chart-canvas-wrap{{position:relative;height:300px;min-width:0}}
.chart-canvas-wrap canvas{{position:absolute;inset:0;width:100% !important;height:100% !important;display:block}}
.insight-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:10px}}
.insight-card{{background:linear-gradient(180deg,var(--surface-elev) 0%,var(--panel-soft) 100%);border:1px solid var(--line);border-radius:12px;padding:11px;min-width:0;box-shadow:0 2px 6px rgba(16,42,67,.05)}}
.insight-title{{font-size:13px;font-weight:700;color:var(--ink);margin-bottom:5px}}
.insight-detail{{font-size:11px;color:var(--ink-soft);line-height:1.4}}
.insight-action{{margin-top:7px;font-size:11px;color:var(--ink);background:color-mix(in srgb, var(--accent-2) 12%, var(--panel));border:1px solid color-mix(in srgb, var(--accent-2) 30%, var(--line));padding:5px 7px;border-radius:8px}}
.table-toolbar{{display:flex;gap:8px;align-items:center;margin-bottom:8px}}
.table-toolbar input{{padding:8px 10px;border:1px solid var(--line-strong);border-radius:10px;min-width:250px;font-size:12px;background:var(--surface-elev);color:var(--ink)}}
.table-wrap{{overflow:auto;max-height:430px;border:1px solid var(--line);border-radius:10px;background:var(--panel)}}
table{{width:100%;border-collapse:collapse;font-size:12px}}
th,td{{padding:7px;border-bottom:1px solid var(--line);text-align:left;color:var(--ink)}}
th{{position:sticky;top:0;background:var(--surface-elev);color:var(--ink-soft)}}
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
@media (max-width:900px){{
  .ux-toolbar{{position:static;justify-content:flex-start}}
  .ux-btn{{font-size:13px;padding:8px 12px}}
  .header h1{{font-size:24px}}
  .header-subtitle{{font-size:12px}}
  .kpi-grid{{grid-template-columns:repeat(auto-fit,minmax(180px,1fr))}}
  .kpi-value{{font-size:17px}}
  .kpi-card.kpi-textual .kpi-value{{font-size:15px}}
  .section h2{{font-size:18px}}
  .chart-grid{{grid-template-columns:1fr}}
  .chart-canvas-wrap{{height:255px}}
  .table-toolbar{{display:block}}
  .table-toolbar input{{min-width:0;width:100%;box-sizing:border-box}}
}}
@media print{{
  .ux-toolbar,.quick-nav{{display:none !important}}
  .section{{break-inside:avoid}}
}}
</style>
</head>
<body>
<div class="container">
  <div class="ux-toolbar">
    <button id="btnTheme" class="ux-btn ux-btn-theme" type="button" aria-pressed="false">
      <span class="theme-dot" aria-hidden="true"></span>
      <span id="btnThemeLabel">Dark mode</span>
    </button>
    <button id="btnMethodology" class="ux-btn" type="button" aria-haspopup="dialog">Methodology</button>
    <button id="btnPrint" class="ux-btn" type="button">Print</button>
  </div>

  <div class="quick-nav" aria-label="Navegación rápida">
    <a href="#sec-insights">Insights</a>
    <a href="#sec-eff">Eficiencia</a>
    <a href="#sec-energy">Energía</a>
    <a href="#sec-process">Proceso</a>
    <a href="#sec-portfolio">Cartera</a>
    <a href="#sec-table">Tabla</a>
    <a href="#sec-decision">Decisión</a>
  </div>

  <div class="header">
    <h1 style="margin:0">Cockpit Ejecutivo de Inteligencia Operativa Industrial</h1>
    <div class="header-subtitle">Vista ejecutiva para priorizar pérdidas, riesgos operativos y captura de valor en planta industrial.</div>
    <div class="header-stats">Cobertura: {meta['coverage_start']} a {meta['coverage_end']} | Plantas: {meta['n_plants']} | Líneas: {meta['n_lines']} | Equipos: {meta['n_equipment']}</div>
    <div class="callouts">{callout_html}</div>
    <div class="sr-only" aria-hidden="true">dashboard_mode: {meta['dashboard_mode']} | run_id: {meta['run_id']} | snapshot_id: {meta['snapshot_id']} | metric_version_set: {meta['metric_version_set']} | decision_model_version: {meta['decision_model_version']} | validation_state: {meta['validation_state']}</div>
  </div>

  <div class="kpi-grid">{kpi_html}</div>

  <div class="section" id="sec-insights">
    <h2>Insights Ejecutivos</h2>
    <div class="insight-grid">{insights_html}</div>
  </div>

  <div class="section" id="sec-eff">
    <h2>Vista de Eficiencia Operativa</h2>
    <div class="chart-grid">
      <div class="chart-card">
        <div class="chart-title">Tendencia SEC por línea</div>
        <div class="chart-canvas-wrap" data-chart-id="secTrend"><canvas id="secTrend"></canvas></div>
      </div>
      <div class="chart-card">
        <div class="chart-title">Tendencia OEE sintético por línea</div>
        <div class="chart-canvas-wrap" data-chart-id="oeeTrend"><canvas id="oeeTrend"></canvas></div>
      </div>
    </div>
  </div>

  <div class="section" id="sec-energy">
    <h2>Vista Energética y Coste</h2>
    <div class="chart-grid">
      <div class="chart-card">
        <div class="chart-title">Coste energético total por línea</div>
        <div class="chart-canvas-wrap" data-chart-id="energyCostLine"><canvas id="energyCostLine"></canvas></div>
      </div>
      <div class="chart-card">
        <div class="chart-title">Top equipos por anomalía de consumo</div>
        <div class="chart-canvas-wrap" data-chart-id="equipmentAnomaly"><canvas id="equipmentAnomaly"></canvas></div>
      </div>
    </div>
  </div>

  <div class="section" id="sec-process">
    <h2>Vista de Proceso y Causas Raíz</h2>
    <div class="chart-grid">
      <div class="chart-card">
        <div class="chart-title">Pérdida económica por causa raíz</div>
        <div class="chart-canvas-wrap" data-chart-id="rootCause"><canvas id="rootCause"></canvas></div>
      </div>
      <div class="chart-card">
        <div class="chart-title">Variabilidad operativa por línea-turno</div>
        <div class="chart-canvas-wrap" data-chart-id="shiftVariance"><canvas id="shiftVariance"></canvas></div>
      </div>
    </div>
  </div>

  <div class="section" id="sec-portfolio">
    <h2>Vista de Cartera y Riesgo</h2>
    <div class="chart-grid">
      <div class="chart-card">
        <div class="chart-title">Valor descontado vs valor downside-adjusted</div>
        <div class="chart-canvas-wrap" data-chart-id="scenarioValue"><canvas id="scenarioValue"></canvas></div>
      </div>
      <div class="chart-card">
        <div class="chart-title">Distribución de iniciativas por ola</div>
        <div class="chart-canvas-wrap" data-chart-id="portfolioWave"><canvas id="portfolioWave"></canvas></div>
      </div>
      <div class="chart-card">
        <div class="chart-title">Prioridad compuesta vs viabilidad</div>
        <div class="chart-canvas-wrap" data-chart-id="priorityScatter"><canvas id="priorityScatter"></canvas></div>
      </div>
    </div>
  </div>

  <div class="section" id="sec-table">
    <h2>Tabla Final Interactiva de Priorización</h2>
    <div class="table-toolbar">
      <input id="tableSearch" type="text" placeholder="Buscar iniciativa, línea, clase o decisión..." />
    </div>
    <div class="table-wrap"><table id="tbl"></table></div>
  </div>

  <div class="section" id="sec-decision">
    <h2>Decisión Ejecutiva</h2>
    <ul>
      <li>Línea a intervenir primero: <b>{decision['linea_prioritaria']}</b> (score {decision['line_criticality_score']:.2f}).</li>
      <li>Equipo a priorizar: <b>{decision['equipo_prioritario']}</b> (score {decision['equipment_energy_anomaly_score']:.2f}).</li>
      <li>Ahorro potencial top-5 ejecutar ahora (proxy): <b>{decision['ahorro_now_top5']:,.0f} EUR/año</b>.</li>
      <li>Riesgo de inacción 12m (proxy): <b>{decision['risk_of_inaction_12m']:,.0f} EUR</b>.</li>
      <li>{decision['committee_note']}</li>
    </ul>
  </div>
</div>

<div id="methodologyModal" class="modal-backdrop" role="dialog" aria-modal="true" aria-labelledby="methodologyTitle">
  <div class="modal-card">
    <div class="modal-header">
      <div id="methodologyTitle" class="modal-title">Methodology & Governance</div>
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
    wrap.innerHTML=`<div style="display:flex;align-items:center;justify-content:center;height:100%;text-align:center;color:#486581;font-size:12px;padding:10px;border:1px dashed #bfd2e6;border-radius:8px;background:#f8fbfe">${{message}}</div>`;
  }});
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
  const num=Number(value);
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
      tension:.18,
      pointRadius:2,
      pointHoverRadius:3,
      borderWidth:2
    }}));
    const secBounds=seriesBounds(secDatasets,0,100);
    const secPad=(secBounds.max-secBounds.min)*0.08;
    renderChart(
      'secTrend',
      'line',
      DATA.charts.sec_trend.labels,
      secDatasets,
      {{
        ...denseCategoryOptions(DATA.charts.sec_trend.labels,false),
        scales:{{
          y:{{
            min:secBounds.hasData ? secBounds.min-secPad : 0,
            max:secBounds.hasData ? secBounds.max+secPad : 100,
            ticks:{{maxTicksLimit:7,font:{{size:11}}}}
          }}
        }}
      }}
    );
    const oeeDatasets=DATA.charts.oee_trend.datasets.map((d,i)=>({{
      label:d.label,
      data:sanitizeSeries(d.data),
      borderColor:colors[i%colors.length],
      fill:false,
      tension:.18,
      pointRadius:2,
      pointHoverRadius:3,
      borderWidth:2
    }}));
    const oeeBounds=seriesBounds(oeeDatasets,0.8,1.0);
    const oeeMin=Math.max(0, (oeeBounds.hasData ? oeeBounds.min : 0.8)-0.01);
    const oeeMax=Math.min(1, (oeeBounds.hasData ? oeeBounds.max : 1.0)+0.01);
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

    const energyLabels=DATA.charts.energy_cost_line.map(r=>r.linea_id);
    renderChart(
      'energyCostLine',
      'bar',
      energyLabels,
      [{{label:'Coste energía (EUR)',data:DATA.charts.energy_cost_line.map(r=>toFiniteNumber(r.coste_energia_total)),backgroundColor:'#1d4ed8'}}],
      {{...denseCategoryOptions(energyLabels,true),plugins:{{legend:{{display:false}}}},scales:{{x:{{beginAtZero:true,ticks:{{maxTicksLimit:6,font:{{size:11}}}}}}}}}}
    );

    const eqLabels=DATA.charts.equipment_anomaly.map(r=>r.equipo_id);
    renderChart(
      'equipmentAnomaly',
      'bar',
      eqLabels,
      [{{label:'Anomaly score',data:DATA.charts.equipment_anomaly.map(r=>toFiniteNumber(r.anomaly_score)),backgroundColor:'#b91c1c'}}],
      {{...denseCategoryOptions(eqLabels,true),plugins:{{legend:{{display:false}}}},scales:{{x:{{beginAtZero:true,ticks:{{maxTicksLimit:6,font:{{size:11}}}}}}}}}}
    );

    const causeLabels=DATA.charts.root_cause.map(r=>r.causa_parada);
    renderChart(
      'rootCause',
      'bar',
      causeLabels,
      [{{label:'Loss root cause score',data:DATA.charts.root_cause.map(r=>toFiniteNumber(r.loss_root_cause_score)),backgroundColor:'#0f766e'}}],
      {{...denseCategoryOptions(causeLabels,true),plugins:{{legend:{{display:false}}}},scales:{{x:{{beginAtZero:true,ticks:{{maxTicksLimit:6,font:{{size:11}}}}}}}}}}
    );

    const shiftLabels=DATA.charts.shift_variance.map(r=>r.label);
    renderChart(
      'shiftVariance',
      'bar',
      shiftLabels,
      [{{label:'Shift variance score',data:DATA.charts.shift_variance.map(r=>toFiniteNumber(r.shift_variance_score)),backgroundColor:'#334155'}}],
      {{...denseCategoryOptions(shiftLabels,true),plugins:{{legend:{{display:false}}}},scales:{{x:{{beginAtZero:true,ticks:{{maxTicksLimit:6,font:{{size:11}}}}}}}}}}
    );

    const scenLabels=DATA.charts.scenario_value.map(r=>r.macro_scenario);
    renderChart(
      'scenarioValue',
      'bar',
      scenLabels,
      [
        {{label:'Discounted value',data:DATA.charts.scenario_value.map(r=>toFiniteNumber(r.discounted_value)),backgroundColor:'#0ea5e9'}},
        {{label:'Downside-adjusted value',data:DATA.charts.scenario_value.map(r=>toFiniteNumber(r.downside_adjusted_value)),backgroundColor:'#0f766e'}}
      ],
      {{...denseCategoryOptions(scenLabels,false)}}
    );

    renderChart(
      'portfolioWave',
      'doughnut',
      DATA.charts.portfolio_wave.map(r=>r.portfolio_wave),
      [{{data:DATA.charts.portfolio_wave.map(r=>toFiniteNumber(r.n_iniciativas)),backgroundColor:['#047857','#1d4ed8','#7c3aed','#64748b']}}],
      {{plugins:{{legend:{{display:true,position:'bottom'}}}}}}
    );

    renderChart(
      'priorityScatter',
      'bubble',
      [],
      [{{
        label:'Prioridad vs viabilidad',
        data:DATA.charts.priority_scatter
          .map(r=>({{x:toFiniteNumber(r.implementation_feasibility_score),y:toFiniteNumber(r.improvement_priority_index),r:toFiniteNumber(r.bubble_r)}}))
          .filter(p=>p.x!==null && p.y!==null && p.r!==null),
        backgroundColor:'rgba(180,83,9,.78)'
      }}],
      {{
        plugins:{{legend:{{display:false}}}},
        scales:{{
          x:{{title:{{display:true,text:'Viabilidad implementación'}},ticks:{{maxTicksLimit:7,font:{{size:11}}}}}},
          y:{{title:{{display:true,text:'Prioridad compuesta'}},ticks:{{maxTicksLimit:7,font:{{size:11}}}}}}
        }}
      }}
    );
  }} catch (error) {{
    console.error('Dashboard chart rendering failed:', error);
    setChartFallback('Ocorreu um erro ao renderizar os gráficos. Regera o dashboard ou verifica a consola do browser.');
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
  if(btnThemeLabel) btnThemeLabel.textContent=isDark ? 'Light mode' : 'Dark mode';
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
const table=document.getElementById('tbl');
const search=document.getElementById('tableSearch');
const rows=DATA.table;
const btnMethodology=document.getElementById('btnMethodology');
const btnPrint=document.getElementById('btnPrint');
const methodologyModal=document.getElementById('methodologyModal');
const btnCloseMethodology=document.getElementById('btnCloseMethodology');
function renderTable(filteredRows){{
  table.innerHTML='<thead><tr>'+columns.map(c=>`<th>${{c}}</th>`).join('')+'</tr></thead><tbody>'+filteredRows.map(r=>'<tr>'+columns.map(c=>`<td>${{r[c]}}</td>`).join('')+'</tr>').join('')+'</tbody>';
}}
renderTable(rows);
search.addEventListener('input',()=>{{
  const q=search.value.trim().toLowerCase();
  if(!q){{renderTable(rows);return;}}
  const filtered=rows.filter(r=>columns.some(c=>String(r[c]).toLowerCase().includes(q)));
  renderTable(filtered);
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

    report_lines = [
        "# Dashboard Build And Serving (Canónico)",
        "",
        "## Artefacto oficial",
        f"- HTML oficial único: `{CANONICAL_HTML}`",
        f"- Dataset oficial único: `{CANONICAL_DATASET_JSON}`",
        "",
        "## Gobernanza aplicada",
        "- Eliminados outputs legacy (`executive_light`, `full_offline`, `board_pack`).",
        "- Sin recálculo KPI crítico en frontend.",
        "- Version stamping obligatorio en header.",
        "",
        "## Auditoría de serving",
        audit.to_markdown(index=False),
    ]
    SERVING_REPORT_MD.write_text("\n".join(report_lines), encoding="utf-8")
    return CANONICAL_HTML


if __name__ == "__main__":
    build_dashboard()
