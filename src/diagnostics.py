from __future__ import annotations

import duckdb
import numpy as np
import pandas as pd

from .config import DATA_PROCESSED_DIR, DOCS_DIR, DUCKDB_PATH, OUTPUT_REPORTS_DIR
from .score_stability import (
    COMPARABILITY_ABSOLUTE,
    COMPARABILITY_CONTEXTUAL,
    SCORE_SCALE_VERSION,
    SCORE_SCALING_METHOD,
    anchored_score,
    legacy_local_minmax_score,
    outlier_impact_median_abs_delta,
    spearman_rank_corr,
    topk_overlap,
)


def _build_diagnostic_framework_doc(path: str) -> None:
    text = """# Diagnostic Framework

## 1) Energy intensity benchmarking
- **Inputs**: `line_day_features.sec`, `line_day_features.sec_vs_target`, `line_day_features.sec_vs_benchmark`, benchmark externo por familia.
- **Lógica**: se estima brecha diaria SEC y persistencia a 7 días para separar desvío puntual vs estructural.
- **Supuestos**: benchmark externo proxy representa un nivel técnicamente alcanzable.
- **Limitaciones**: no se modela curva completa de carga parcial por producto.
- **Valor para operación**: prioriza líneas con gap persistente, no solo picos aislados.

## 2) Anomaly detection interpretable
- **Inputs**: `equipment_day_features.energy_intensity_relative`, `overload_events`, `anomaly_count`, `performance_drift`.
- **Lógica**: score aditivo y transparente con umbrales físicos (sobrecarga, drift, paradas repetidas).
- **Supuestos**: sobreconsumo relativo por tipo de equipo es señal de degradación.
- **Limitaciones**: no hay datos de sensor en alta frecuencia sub-horaria.
- **Valor para operación**: lista de equipos a inspeccionar en mantenimiento/operación.

## 3) Root-cause ranking
- **Inputs**: `vw_loss_root_cause_summary`, impacto producción, energía, horas de parada.
- **Lógica**: ranking por peso económico-operativo y recurrencia mensual.
- **Supuestos**: impacto proxy de parada representa pérdida de contribución marginal.
- **Limitaciones**: causalidad aproximada, no inferencia causal formal.
- **Valor para operación**: orden de ataque a causas de mayor valor.

## 4) Line/equipment risk scoring
- **Inputs**: brecha SEC, OEE, pérdida económica, desvíos, paradas, criticidad.
- **Lógica**: score compuesto normalizado para `line_criticality_score` y riesgo equipo.
- **Supuestos**: pesos reflejan prioridad industrial (coste + continuidad operativa).
- **Limitaciones**: pesos fijos, no calibrados con comité técnico.
- **Valor para operación**: semáforo de riesgo por línea y por activo.

## 5) Improvement priority logic
- **Inputs**: ahorro potencial, payback, complejidad, urgencia operativa.
- **Lógica**: `opportunity_priority_score` balancea valor económico con factibilidad.
- **Supuestos**: payback y ahorro proxy son consistentes por iniciativa.
- **Limitaciones**: no incorpora restricciones de caja reales ni ventanas de parada mayor.
- **Valor para operación**: transforma diagnóstico técnico en secuencia de ejecución.
"""
    with open(path, "w", encoding="utf-8") as file:
        file.write(text)


def build_root_cause_ranking() -> dict[str, pd.DataFrame]:
    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    line_day = pd.read_csv(DATA_PROCESSED_DIR / "line_day_features.csv", parse_dates=["fecha"])
    equipment_day = pd.read_csv(DATA_PROCESSED_DIR / "equipment_day_features.csv", parse_dates=["fecha"])
    shift = pd.read_csv(DATA_PROCESSED_DIR / "shift_features.csv", parse_dates=["fecha"])
    opportunities = pd.read_csv(DATA_PROCESSED_DIR / "improvement_opportunity_features.csv")

    con = duckdb.connect(str(DUCKDB_PATH))
    root_cause = con.execute(
        """
        SELECT
            periodo,
            planta_id,
            linea_id,
            causa_parada,
            n_eventos,
            horas_parada,
            impacto_produccion_proxy,
            impacto_energia_proxy,
            emisiones_asociadas_kg,
            loss_root_cause_score,
            ranking_causa
        FROM vw_loss_root_cause_summary
        """
    ).fetchdf()
    con.close()

    # 1) SEC benchmark gap + persistencia
    line_day["sec_benchmark_gap"] = line_day["sec_vs_benchmark"]
    line_day["sec_gap_persistencia_14d"] = (
        line_day.sort_values(["linea_id", "fecha"]).groupby("linea_id")["sec_benchmark_gap"].transform(
            lambda s: s.rolling(14, min_periods=7).mean()
        )
    )

    # 2) Equipment energy anomaly score
    eq_energy_n = anchored_score(equipment_day["energy_intensity_relative"], low=0.85, high=1.25) / 100.0
    eq_anom_n = anchored_score(equipment_day["anomaly_count"], low=0.0, high=3.0) / 100.0
    eq_overload_n = anchored_score(equipment_day["overload_events"], low=0.0, high=1.0) / 100.0
    eq_drift_n = anchored_score(equipment_day["performance_drift"].clip(lower=0), low=0.0, high=0.006) / 100.0
    equipment_day["equipment_energy_anomaly_score"] = 100 * (
        0.35 * eq_energy_n
        + 0.25 * eq_anom_n
        + 0.20 * eq_overload_n
        + 0.20 * eq_drift_n
    )
    equipment_day["equipment_energy_anomaly_scaling_method"] = SCORE_SCALING_METHOD
    equipment_day["equipment_energy_anomaly_scale_version"] = SCORE_SCALE_VERSION
    equipment_day["equipment_energy_anomaly_comparability_tag"] = COMPARABILITY_ABSOLUTE

    # 3) Process deviation risk score
    proc_dev_n = anchored_score(line_day["process_deviation_density"], low=0.0, high=0.03) / 100.0
    quality_loss_n = anchored_score(line_day["quality_loss_proxy"], low=0.01, high=0.05) / 100.0
    stop_density_n = anchored_score(line_day["stop_density"], low=0.0, high=0.02) / 100.0
    perf_loss_n = anchored_score(line_day["performance_loss_proxy"], low=0.0, high=0.35) / 100.0
    line_day["process_deviation_risk_score"] = 100 * (
        0.45 * proc_dev_n
        + 0.25 * quality_loss_n
        + 0.20 * stop_density_n
        + 0.10 * perf_loss_n
    )
    line_day["process_deviation_risk_scaling_method"] = SCORE_SCALING_METHOD
    line_day["process_deviation_risk_scale_version"] = SCORE_SCALE_VERSION
    line_day["process_deviation_risk_comparability_tag"] = COMPARABILITY_ABSOLUTE

    # 4) Loss root cause score (agregado y ranking)
    root_cause_rank = (
        root_cause.groupby(["planta_id", "linea_id", "causa_parada"], as_index=False)
        .agg(
            n_eventos=("n_eventos", "sum"),
            horas_parada=("horas_parada", "sum"),
            impacto_produccion_proxy=("impacto_produccion_proxy", "sum"),
            impacto_energia_proxy=("impacto_energia_proxy", "sum"),
            emisiones_asociadas_kg=("emisiones_asociadas_kg", "sum"),
            loss_root_cause_score=("loss_root_cause_score", "sum"),
        )
        .sort_values("loss_root_cause_score", ascending=False)
    )

    # 5) Line criticality score
    line_agg = (
        line_day.groupby(["planta_id", "linea_id"], as_index=False)
        .agg(
            sec_benchmark_gap=("sec_benchmark_gap", "mean"),
            oee_synthetic=("oee_synthetic", "mean"),
            process_deviation_risk_score=("process_deviation_risk_score", "mean"),
            loss_value_proxy=("loss_value_proxy", "mean"),
            line_criticality_weight=("line_criticality_weight", "mean"),
            trend_sec_7d=("trend_sec_7d", "mean"),
        )
    )
    loss_n = anchored_score(line_agg["loss_value_proxy"], low=9_000.0, high=45_000.0) / 100.0
    sec_gap_n = anchored_score(line_agg["sec_benchmark_gap"].clip(lower=0), low=0.0, high=120.0) / 100.0
    oee_bad_n = anchored_score(line_agg["oee_synthetic"], low=0.75, high=1.0, invert=True) / 100.0
    proc_risk_n = anchored_score(line_agg["process_deviation_risk_score"], low=10.0, high=80.0) / 100.0
    line_weight_n = anchored_score(line_agg["line_criticality_weight"], low=0.4, high=1.0) / 100.0
    line_agg["line_criticality_score"] = 100 * (
        0.30 * loss_n
        + 0.20 * sec_gap_n
        + 0.20 * oee_bad_n
        + 0.20 * proc_risk_n
        + 0.10 * line_weight_n
    )
    line_agg["line_criticality_scaling_method"] = SCORE_SCALING_METHOD
    line_agg["line_criticality_scale_version"] = SCORE_SCALE_VERSION
    line_agg["line_criticality_comparability_tag"] = COMPARABILITY_CONTEXTUAL

    # 6) Opportunity priority score
    opp_saving_n = anchored_score(opportunities["annual_saving_proxy"], low=100_000.0, high=800_000.0) / 100.0
    opp_payback_n = anchored_score(opportunities["payback_months"], low=6.0, high=36.0, invert=True) / 100.0
    opp_urgency_n = anchored_score(opportunities["operational_urgency_score"], low=30.0, high=85.0) / 100.0
    opp_strategy_n = anchored_score(opportunities["strategic_alignment_score"], low=30.0, high=95.0) / 100.0
    opp_complexity_n = anchored_score(opportunities["implementation_complexity_score"], low=20.0, high=95.0, invert=True) / 100.0
    opportunities["opportunity_priority_score"] = 100 * (
        0.35 * opp_saving_n
        + 0.20 * opp_payback_n
        + 0.20 * opp_urgency_n
        + 0.15 * opp_strategy_n
        + 0.10 * opp_complexity_n
    )
    opportunities["opportunity_priority_scaling_method"] = SCORE_SCALING_METHOD
    opportunities["opportunity_priority_scale_version"] = SCORE_SCALE_VERSION
    opportunities["opportunity_priority_comparability_tag"] = COMPARABILITY_CONTEXTUAL

    # Auditoría de estabilidad: local min-max (legacy) vs anchored.
    legacy_equipment_score = 100 * (
        0.35 * (legacy_local_minmax_score(equipment_day["energy_intensity_relative"]) / 100.0)
        + 0.25 * (legacy_local_minmax_score(equipment_day["anomaly_count"]) / 100.0)
        + 0.20 * (legacy_local_minmax_score(equipment_day["overload_events"]) / 100.0)
        + 0.20 * (legacy_local_minmax_score(equipment_day["performance_drift"].clip(lower=0)) / 100.0)
    )
    legacy_process_score = 100 * (
        0.45 * (legacy_local_minmax_score(line_day["process_deviation_density"]) / 100.0)
        + 0.25 * (legacy_local_minmax_score(line_day["quality_loss_proxy"]) / 100.0)
        + 0.20 * (legacy_local_minmax_score(line_day["stop_density"]) / 100.0)
        + 0.10 * (legacy_local_minmax_score(line_day["performance_loss_proxy"]) / 100.0)
    )
    legacy_line_score = 100 * (
        0.30 * (legacy_local_minmax_score(line_agg["loss_value_proxy"]) / 100.0)
        + 0.20 * (legacy_local_minmax_score(line_agg["sec_benchmark_gap"].clip(lower=0)) / 100.0)
        + 0.20 * (legacy_local_minmax_score(line_agg["oee_synthetic"], invert=True) / 100.0)
        + 0.20 * (legacy_local_minmax_score(line_agg["process_deviation_risk_score"]) / 100.0)
        + 0.10 * (legacy_local_minmax_score(line_agg["line_criticality_weight"]) / 100.0)
    )
    legacy_opportunity_score = 100 * (
        0.35 * (legacy_local_minmax_score(opportunities["annual_saving_proxy"]) / 100.0)
        + 0.20 * (legacy_local_minmax_score(opportunities["payback_months"], invert=True) / 100.0)
        + 0.20 * (legacy_local_minmax_score(opportunities["operational_urgency_score"]) / 100.0)
        + 0.15 * (legacy_local_minmax_score(opportunities["strategic_alignment_score"]) / 100.0)
        + 0.10 * (legacy_local_minmax_score(opportunities["implementation_complexity_score"], invert=True) / 100.0)
    )

    stability_rows = [
        {
            "score_name": "equipment_energy_anomaly_score",
            "legacy_outlier_median_abs_shift": outlier_impact_median_abs_delta(
                equipment_day["energy_intensity_relative"],
                lambda s: legacy_local_minmax_score(s),
            ),
            "anchored_outlier_median_abs_shift": outlier_impact_median_abs_delta(
                equipment_day["energy_intensity_relative"],
                lambda s: anchored_score(s, low=0.85, high=1.25),
            ),
            "spearman_anchored_vs_legacy": spearman_rank_corr(equipment_day["equipment_energy_anomaly_score"], legacy_equipment_score),
            "top10_overlap_anchored_vs_legacy": topk_overlap(
                equipment_day["equipment_energy_anomaly_score"],
                legacy_equipment_score,
                equipment_day["equipo_id"],
                topk=10,
            ),
            "comparability_tag": COMPARABILITY_ABSOLUTE,
        },
        {
            "score_name": "process_deviation_risk_score",
            "legacy_outlier_median_abs_shift": outlier_impact_median_abs_delta(
                line_day["process_deviation_density"],
                lambda s: legacy_local_minmax_score(s),
            ),
            "anchored_outlier_median_abs_shift": outlier_impact_median_abs_delta(
                line_day["process_deviation_density"],
                lambda s: anchored_score(s, low=0.0, high=0.03),
            ),
            "spearman_anchored_vs_legacy": spearman_rank_corr(line_day["process_deviation_risk_score"], legacy_process_score),
            "top10_overlap_anchored_vs_legacy": topk_overlap(
                line_day["process_deviation_risk_score"],
                legacy_process_score,
                line_day["linea_id"].astype(str) + "|" + line_day["fecha"].astype(str),
                topk=10,
            ),
            "comparability_tag": COMPARABILITY_ABSOLUTE,
        },
        {
            "score_name": "line_criticality_score",
            "legacy_outlier_median_abs_shift": outlier_impact_median_abs_delta(
                line_agg["loss_value_proxy"],
                lambda s: legacy_local_minmax_score(s),
            ),
            "anchored_outlier_median_abs_shift": outlier_impact_median_abs_delta(
                line_agg["loss_value_proxy"],
                lambda s: anchored_score(s, low=9_000.0, high=45_000.0),
            ),
            "spearman_anchored_vs_legacy": spearman_rank_corr(line_agg["line_criticality_score"], legacy_line_score),
            "top10_overlap_anchored_vs_legacy": topk_overlap(
                line_agg["line_criticality_score"],
                legacy_line_score,
                line_agg["linea_id"],
                topk=10,
            ),
            "comparability_tag": COMPARABILITY_CONTEXTUAL,
        },
        {
            "score_name": "opportunity_priority_score",
            "legacy_outlier_median_abs_shift": outlier_impact_median_abs_delta(
                opportunities["annual_saving_proxy"],
                lambda s: legacy_local_minmax_score(s),
            ),
            "anchored_outlier_median_abs_shift": outlier_impact_median_abs_delta(
                opportunities["annual_saving_proxy"],
                lambda s: anchored_score(s, low=100_000.0, high=800_000.0),
            ),
            "spearman_anchored_vs_legacy": spearman_rank_corr(opportunities["opportunity_priority_score"], legacy_opportunity_score),
            "top10_overlap_anchored_vs_legacy": topk_overlap(
                opportunities["opportunity_priority_score"],
                legacy_opportunity_score,
                opportunities["iniciativa_id"],
                topk=10,
            ),
            "comparability_tag": COMPARABILITY_CONTEXTUAL,
        },
    ]
    score_stability_before_after = pd.DataFrame(stability_rows)

    # 7) Main loss driver + 8) recommended action
    line_day["loss_stop_proxy"] = line_day["stop_density"] * line_day["loss_value_proxy"]
    line_day["loss_process_proxy"] = line_day["process_loss_proxy"]
    line_day["loss_thermal_proxy"] = line_day["thermal_loss_proxy"]

    drivers = (
        line_day.groupby(["planta_id", "linea_id"], as_index=False)
        .agg(
            loss_stop_proxy=("loss_stop_proxy", "mean"),
            loss_process_proxy=("loss_process_proxy", "mean"),
            loss_thermal_proxy=("loss_thermal_proxy", "mean"),
            sec_gap_persistencia_14d=("sec_gap_persistencia_14d", "mean"),
        )
    )

    def _main_driver(row: pd.Series) -> str:
        candidates = {
            "Paradas": row["loss_stop_proxy"],
            "DesvioProceso": row["loss_process_proxy"],
            "IneficienciaTermica": row["loss_thermal_proxy"],
        }
        return max(candidates, key=candidates.get)

    drivers["main_loss_driver"] = drivers.apply(_main_driver, axis=1)

    def _recommended_action(row: pd.Series) -> str:
        if row["main_loss_driver"] == "Paradas":
            return "Priorizar mantenimiento y confiabilidad de equipos criticos"
        if row["main_loss_driver"] == "DesvioProceso":
            return "Optimizar parametros de proceso y disciplina operativa por turno"
        return "Ejecutar quick wins energeticos y plan de eficiencia termica"

    drivers["recommended_action_initial"] = drivers.apply(_recommended_action, axis=1)

    line_summary = line_agg.merge(drivers[["planta_id", "linea_id", "main_loss_driver", "recommended_action_initial"]], on=["planta_id", "linea_id"], how="left")

    # Persistencia temporal: ruido vs estructural
    persistence = (
        line_day.groupby(["planta_id", "linea_id"], as_index=False)
        .agg(
            sec_gap_avg=("sec_benchmark_gap", "mean"),
            sec_gap_std=("sec_benchmark_gap", "std"),
            sec_gap_persistencia_14d=("sec_gap_persistencia_14d", "mean"),
        )
    )
    persistence["signal_to_noise"] = persistence["sec_gap_avg"].abs() / persistence["sec_gap_std"].replace(0, np.nan)
    persistence["problema_estructural_flag"] = (
        (persistence["sec_gap_persistencia_14d"] > 0)
        & (persistence["signal_to_noise"].fillna(0) > 0.8)
    ).astype(int)

    # Comparación por planta, línea y turno
    turno_comparison = (
        shift.groupby(["planta_id", "linea_id", "turno"], as_index=False)
        .agg(
            sec_shift=("sec_shift", "mean"),
            shift_variance_score=("shift_variance_score", "mean"),
            scrap_pct=("scrap_pct", "mean"),
            stop_minutes=("stop_minutes", "mean"),
        )
        .sort_values(["planta_id", "linea_id", "shift_variance_score"], ascending=[True, True, False])
    )

    # Outputs
    line_day.to_csv(DATA_PROCESSED_DIR / "line_day_features_scored.csv", index=False)
    equipment_day.to_csv(DATA_PROCESSED_DIR / "equipment_energy_anomaly_scores.csv", index=False)
    root_cause_rank.to_csv(DATA_PROCESSED_DIR / "loss_root_cause_score.csv", index=False)
    line_summary.to_csv(DATA_PROCESSED_DIR / "line_criticality_scores.csv", index=False)
    opportunities.sort_values("opportunity_priority_score", ascending=False).to_csv(
        DATA_PROCESSED_DIR / "opportunity_priority_scores.csv", index=False
    )
    score_stability_before_after.to_csv(DATA_PROCESSED_DIR / "score_stability_before_after_diagnostics.csv", index=False)
    persistence.to_csv(DATA_PROCESSED_DIR / "line_persistence_analysis.csv", index=False)
    turno_comparison.to_csv(DATA_PROCESSED_DIR / "shift_comparison_scores.csv", index=False)

    top_line = line_summary.sort_values("line_criticality_score", ascending=False).head(1)
    top_eq = equipment_day.sort_values("equipment_energy_anomaly_score", ascending=False).head(1)

    report_lines = [
        "# Diagnóstico Analítico Industrial",
        "",
        "## Resumen de outputs obligatorios",
        "- sec_benchmark_gap: generado en `line_day_features_scored.csv`.",
        "- equipment_energy_anomaly_score: generado en `equipment_energy_anomaly_scores.csv`.",
        "- process_deviation_risk_score: generado en `line_day_features_scored.csv`.",
        "- loss_root_cause_score: generado en `loss_root_cause_score.csv`.",
        "- line_criticality_score: generado en `line_criticality_scores.csv`.",
        "- opportunity_priority_score: generado en `opportunity_priority_scores.csv`.",
        "- main_loss_driver y recommended_action_initial: generado en `line_criticality_scores.csv`.",
        "",
        "## Hallazgo principal",
        f"- Línea de mayor criticidad: **{top_line['linea_id'].iloc[0]}** ({top_line['line_criticality_score'].iloc[0]:.1f}/100).",
        f"- Equipo de mayor anomalía energética: **{top_eq['equipo_id'].iloc[0]}** ({top_eq['equipment_energy_anomaly_score'].iloc[0]:.1f}/100).",
        "",
        "## Diferenciación ruido vs estructural",
        f"- Líneas con problema estructural SEC: **{int(persistence['problema_estructural_flag'].sum())}** de {len(persistence)}.",
        "",
        "## Quick wins vs ingeniería",
        "- Si el driver dominante es térmico: quick wins de setpoints, aislamiento y disciplina operativa.",
        "- Si domina desvío de proceso: ingeniería de control, ventanas operativas y estandarización de receta.",
        "- Si dominan paradas: mantenimiento predictivo y fiabilidad de subsistemas críticos.",
    ]
    _build_diagnostic_framework_doc(str(DOCS_DIR / "diagnostic_framework.md"))

    return {
        "line_summary": line_summary,
        "equipment_scores": equipment_day,
        "root_cause": root_cause_rank,
        "opportunity_scores": opportunities,
        "score_stability_before_after": score_stability_before_after,
    }


if __name__ == "__main__":
    build_root_cause_ranking()
