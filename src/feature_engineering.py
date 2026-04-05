from __future__ import annotations

from dataclasses import dataclass

import duckdb
import numpy as np
import pandas as pd

from .config import DATA_PROCESSED_DIR, DOCS_DIR, DUCKDB_PATH
from .score_stability import (
    AnchoredBand,
    COMPARABILITY_ABSOLUTE,
    COMPARABILITY_CONTEXTUAL,
    SCORE_SCALE_VERSION,
    SCORE_SCALING_METHOD,
    anchored_score,
    anchored_score_by_group,
)


@dataclass(frozen=True)
class FeatureDef:
    tabla: str
    feature: str
    tipo: str
    logica_negocio: str


def _rolling_slope(series: pd.Series, window: int = 7) -> pd.Series:
    values = series.astype(float).values
    out = np.zeros(len(values), dtype=float)
    for idx in range(len(values)):
        start = max(0, idx - window + 1)
        segment = values[start : idx + 1]
        if len(segment) < max(3, window // 2):
            out[idx] = 0.0
            continue
        x = np.arange(len(segment), dtype=float)
        out[idx] = float(np.polyfit(x, segment, 1)[0])
    return pd.Series(out, index=series.index)


def _build_line_day_features(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    df = con.execute(
        """
        WITH benchmark_month AS (
            SELECT
                periodo,
                familia_proceso,
                AVG(sec_referencia) AS sec_referencia
            FROM stg_benchmark_external
            GROUP BY 1,2
        )
        SELECT
            CAST(m.fecha AS DATE) AS fecha,
            m.planta_id,
            m.linea_id,
            m.familia_proceso,
            l.criticidad_linea,
            m.production_volume_ton,
            m.utilization_rate,
            m.sec_kwh_ton,
            m.sec_target_kwh_ton,
            m.cost_energy_per_unit,
            m.cost_energy_total,
            m.emissions_kg,
            m.oee_synthetic,
            m.availability_rate,
            m.quality_rate_proxy,
            m.n_paradas,
            m.n_desvios,
            m.loss_value_proxy_eur,
            b.sec_referencia
        FROM mart_line_day m
        JOIN stg_lines l
          ON m.linea_id = l.linea_id
        LEFT JOIN benchmark_month b
          ON STRFTIME(m.fecha, '%Y-%m') = b.periodo
         AND m.familia_proceso = b.familia_proceso
        """
    ).fetchdf()

    df["production_volume"] = df["production_volume_ton"]
    df["planned_vs_actual_utilization"] = df["utilization_rate"] / 0.88
    df["sec"] = df["sec_kwh_ton"]
    df["sec_vs_target"] = df["sec_kwh_ton"] - df["sec_target_kwh_ton"]
    df["sec_vs_benchmark"] = df["sec_kwh_ton"] - df["sec_referencia"]
    df["cost_energy_per_unit"] = df["cost_energy_per_unit"]
    df["emissions_per_unit"] = np.where(
        df["production_volume_ton"] > 0,
        df["emissions_kg"] / df["production_volume_ton"],
        np.nan,
    )
    df["performance_loss_proxy"] = (1 - df["utilization_rate"]).clip(lower=0)
    df["quality_loss_proxy"] = (1 - df["quality_rate_proxy"]).clip(lower=0)
    df["stop_density"] = np.where(df["production_volume_ton"] > 0, df["n_paradas"] / df["production_volume_ton"], 0)
    df["process_deviation_density"] = np.where(
        df["production_volume_ton"] > 0,
        df["n_desvios"] / df["production_volume_ton"],
        0,
    )
    df["loss_value_proxy"] = df["loss_value_proxy_eur"]
    df["thermal_loss_proxy"] = (df["sec_vs_target"].clip(lower=0) * df["production_volume_ton"] * 0.09).fillna(0)
    process_dev_n = anchored_score(df["process_deviation_density"], low=0.0, high=0.03) / 100.0
    quality_loss_n = anchored_score(df["quality_loss_proxy"], low=0.01, high=0.05) / 100.0
    df["process_loss_proxy"] = (
        df["loss_value_proxy_eur"]
        * (0.45 * process_dev_n + 0.35 * quality_loss_n + 0.20)
    )
    df["line_criticality_weight"] = (df["criticidad_linea"] / 5.0).clip(0, 1)

    df = df.sort_values(["linea_id", "fecha"]).reset_index(drop=True)
    grouped = df.groupby("linea_id", group_keys=False)
    df["trend_sec_7d"] = grouped["sec"].transform(lambda s: _rolling_slope(s, 7))
    df["trend_oee_7d"] = grouped["oee_synthetic"].transform(lambda s: _rolling_slope(s, 7))

    cols = [
        "fecha",
        "planta_id",
        "linea_id",
        "familia_proceso",
        "production_volume",
        "planned_vs_actual_utilization",
        "sec",
        "sec_vs_target",
        "sec_vs_benchmark",
        "cost_energy_per_unit",
        "emissions_per_unit",
        "oee_synthetic",
        "availability_rate",
        "performance_loss_proxy",
        "quality_loss_proxy",
        "stop_density",
        "process_deviation_density",
        "loss_value_proxy",
        "thermal_loss_proxy",
        "process_loss_proxy",
        "line_criticality_weight",
        "trend_sec_7d",
        "trend_oee_7d",
    ]
    return df[cols]


def _build_equipment_day_features(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    df = con.execute(
        """
        SELECT
            CAST(m.fecha AS DATE) AS fecha,
            m.planta_id,
            m.linea_id,
            m.equipo_id,
            m.tipo_equipo,
            m.subsistema,
            m.consumo_relativo_tipo,
            m.factor_carga_prom,
            m.n_paradas,
            m.horas_parada,
            m.n_mantenimientos,
            m.horas_mantenimiento,
            m.disponibilidad_proxy,
            m.riesgo_equipo_proxy,
            m.coste_energia_eur,
            m.coste_mantenimiento_eur,
            m.criticidad_equipo,
            e.potencia_nominal_kw
        FROM mart_equipment_day m
        JOIN stg_equipment e
          ON m.equipo_id = e.equipo_id
        """
    ).fetchdf()

    df = df.sort_values(["equipo_id", "fecha"]).reset_index(drop=True)
    grouped = df.groupby("equipo_id", group_keys=False)

    df["energy_intensity_relative"] = df["consumo_relativo_tipo"]
    df["overload_events"] = (
        (df["factor_carga_prom"] >= 1.0)
        | (df["consumo_relativo_tipo"] >= 1.15)
        | (df["consumo_relativo_tipo"] * df["potencia_nominal_kw"] >= df["potencia_nominal_kw"] * 1.12)
    ).astype(int)

    anomalia_energia = (df["consumo_relativo_tipo"] >= 1.2).astype(int)
    anomalia_disponibilidad = (df["disponibilidad_proxy"] <= 0.9).astype(int)
    anomalia_paradas = (df["n_paradas"] >= 2).astype(int)
    anomalia_mantenimiento = (df["n_mantenimientos"] >= 1).astype(int)

    df["anomaly_count"] = anomalia_energia + anomalia_disponibilidad + anomalia_paradas + anomalia_mantenimiento
    df["downtime_hours"] = df["horas_parada"]
    df["downtime_frequency"] = df["n_paradas"]
    df["maintenance_load"] = df["horas_mantenimiento"] / 24.0
    df["repeated_stop_flag"] = grouped["n_paradas"].transform(lambda s: s.rolling(7, min_periods=3).sum() >= 5).astype(int)
    df["performance_drift"] = grouped["consumo_relativo_tipo"].transform(lambda s: _rolling_slope(s, 14))
    df["cost_impact_proxy"] = (
        df["coste_energia_eur"] + df["coste_mantenimiento_eur"] + df["horas_parada"] * 420.0
    )
    df["criticality_weight"] = (df["criticidad_equipo"] / 5.0).clip(0, 1)
    energy_intensity_n = anchored_score(df["consumo_relativo_tipo"], low=0.85, high=1.25) / 100.0
    downtime_n = anchored_score(df["downtime_hours"], low=0.0, high=8.0) / 100.0
    anomaly_n = anchored_score(df["anomaly_count"], low=0.0, high=3.0) / 100.0
    drift_n = anchored_score(df["performance_drift"].clip(lower=0), low=0.0, high=0.006) / 100.0
    criticality_n = anchored_score(df["criticality_weight"], low=0.2, high=1.0) / 100.0
    df["energy_risk_proxy"] = (
        100
        * (
            0.30 * energy_intensity_n
            + 0.20 * downtime_n
            + 0.20 * anomaly_n
            + 0.15 * drift_n
            + 0.15 * criticality_n
        )
    )
    df["energy_risk_proxy_scaling_method"] = SCORE_SCALING_METHOD
    df["energy_risk_proxy_scale_version"] = SCORE_SCALE_VERSION
    df["energy_risk_proxy_comparability_tag"] = COMPARABILITY_ABSOLUTE

    cols = [
        "fecha",
        "planta_id",
        "linea_id",
        "equipo_id",
        "tipo_equipo",
        "subsistema",
        "energy_intensity_relative",
        "overload_events",
        "anomaly_count",
        "downtime_hours",
        "downtime_frequency",
        "maintenance_load",
        "repeated_stop_flag",
        "performance_drift",
        "cost_impact_proxy",
        "criticality_weight",
        "energy_risk_proxy",
        "energy_risk_proxy_scaling_method",
        "energy_risk_proxy_scale_version",
        "energy_risk_proxy_comparability_tag",
    ]
    return df[cols]


def _build_shift_features(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    df = con.execute(
        """
        SELECT
            CAST(s.fecha AS DATE) AS fecha,
            s.turno,
            s.planta_id,
            s.linea_id,
            s.familia_proceso,
            s.toneladas_producidas_proxy,
            s.sec_shift_kwh_ton,
            s.n_desvios_shift,
            s.horas_parada_shift,
            s.scrap_pct,
            s.reproceso_pct,
            c.demanda_operativa_indice,
            c.tension_operativa_proxy,
            c.mix_complejidad_indice
        FROM int_line_shift s
        LEFT JOIN stg_context_shift c
          ON s.fecha = c.fecha
         AND s.turno = c.turno
         AND s.planta_id = c.planta_id
        """
    ).fetchdf()

    df["throughput"] = df["toneladas_producidas_proxy"]
    df["sec_shift"] = df["sec_shift_kwh_ton"]
    df["deviation_rate"] = np.where(df["throughput"] > 0, df["n_desvios_shift"] / df["throughput"], 0)
    df["stop_minutes"] = df["horas_parada_shift"] * 60.0
    df["rework_pct"] = df["reproceso_pct"]
    df["operator_pressure_proxy"] = (
        100.0
        * (0.45 * df["demanda_operativa_indice"].fillna(1.0) + 0.35 * df["tension_operativa_proxy"].fillna(1.0) + 0.20 * df["mix_complejidad_indice"].fillna(1.0))
    )

    sec_shift_bands = {
        "Laminacion": AnchoredBand(low=360.0, high=620.0),
        "TratamientoTermico": AnchoredBand(low=420.0, high=700.0),
        "Acabado": AnchoredBand(low=330.0, high=560.0),
        "InspeccionFinal": AnchoredBand(low=300.0, high=520.0),
    }
    sec_shift_n = anchored_score_by_group(
        df["sec_shift"],
        df["familia_proceso"],
        bands=sec_shift_bands,
        default_band=AnchoredBand(low=320.0, high=620.0),
    ) / 100.0
    deviation_n = anchored_score(df["deviation_rate"], low=0.0, high=0.05) / 100.0
    stop_n = anchored_score(df["stop_minutes"], low=0.0, high=480.0) / 100.0
    scrap_n = anchored_score(df["scrap_pct"], low=0.8, high=5.0) / 100.0
    pressure_n = anchored_score(df["operator_pressure_proxy"], low=75.0, high=112.0) / 100.0
    df["shift_variance_score"] = (
        100
        * (
            0.30 * sec_shift_n
            + 0.20 * deviation_n
            + 0.20 * stop_n
            + 0.15 * scrap_n
            + 0.15 * pressure_n
        )
    )
    df["shift_variance_score_scaling_method"] = SCORE_SCALING_METHOD
    df["shift_variance_score_scale_version"] = SCORE_SCALE_VERSION
    df["shift_variance_score_comparability_tag"] = COMPARABILITY_ABSOLUTE

    cols = [
        "fecha",
        "turno",
        "planta_id",
        "linea_id",
        "familia_proceso",
        "throughput",
        "sec_shift",
        "deviation_rate",
        "stop_minutes",
        "scrap_pct",
        "rework_pct",
        "operator_pressure_proxy",
        "shift_variance_score",
        "shift_variance_score_scaling_method",
        "shift_variance_score_scale_version",
        "shift_variance_score_comparability_tag",
    ]
    return df[cols]


def _build_improvement_opportunity_features(con: duckdb.DuckDBPyConnection, line_day_features: pd.DataFrame) -> pd.DataFrame:
    initiatives = con.execute(
        """
        SELECT
            i.iniciativa_id,
            i.planta_id,
            i.linea_id,
            i.equipo_id,
            i.categoria_iniciativa,
            i.tipo_iniciativa,
            i.descripcion_resumida,
            i.impacto_esperado_energia_pct,
            i.impacto_esperado_oee_pct,
            i.impacto_esperado_emisiones_pct,
            i.dificultad_implementacion,
            i.quick_win_flag,
            c.capex_estimado,
            c.opex_impacto_proxy,
            c.ahorro_anual_estimado,
            c.payback_estimado_meses,
            c.criticidad_estrategica,
            c.horizonte_implantacion,
            c.riesgo_implantacion
        FROM stg_initiatives i
        LEFT JOIN stg_capex c
          ON i.iniciativa_id = c.iniciativa_id
        """
    ).fetchdf()

    base_line = (
        line_day_features.groupby(["planta_id", "linea_id"], as_index=False)
        .agg(
            base_energy_cost=("cost_energy_per_unit", "mean"),
            base_emissions_per_unit=("emissions_per_unit", "mean"),
            base_loss=("loss_value_proxy", "mean"),
            base_line_criticality=("line_criticality_weight", "mean"),
        )
    )

    df = initiatives.merge(base_line, on=["planta_id", "linea_id"], how="left")
    df[["base_energy_cost", "base_emissions_per_unit", "base_loss", "base_line_criticality"]] = df[
        ["base_energy_cost", "base_emissions_per_unit", "base_loss", "base_line_criticality"]
    ].fillna(0)

    df["expected_energy_saving"] = df["ahorro_anual_estimado"] * (df["impacto_esperado_energia_pct"] / 100.0)
    df["expected_oee_gain"] = df["impacto_esperado_oee_pct"]
    df["expected_emissions_reduction"] = (
        df["base_emissions_per_unit"] * (df["impacto_esperado_emisiones_pct"] / 100.0) * 1000
    )
    df["annual_saving_proxy"] = (
        0.70 * df["ahorro_anual_estimado"]
        + 0.20 * df["expected_energy_saving"]
        + 0.10 * df["base_loss"] * (df["impacto_esperado_oee_pct"] / 100.0)
    )

    risk_map = {"Bajo": 1, "Medio": 3, "Alto": 5}
    horizon_map = {"0-12 meses": 1, "12-24 meses": 3, ">24 meses": 5}

    capex_n = anchored_score(df["capex_estimado"], low=50_000.0, high=2_300_000.0) / 100.0
    df["implementation_complexity_score"] = (
        100
        * (
            0.60 * (df["dificultad_implementacion"] / 5.0)
            + 0.25 * capex_n
            + 0.15 * df["horizonte_implantacion"].map(horizon_map).fillna(3) / 5.0
        )
    )
    df["execution_risk_score"] = (
        100
        * (
            0.50 * df["riesgo_implantacion"].map(risk_map).fillna(3) / 5.0
            + 0.30 * (df["dificultad_implementacion"] / 5.0)
            + 0.20 * (1 - np.where(df["quick_win_flag"] == 1, 1.0, 0.0))
        )
    )
    df["payback_months"] = df["payback_estimado_meses"]
    strategic_criticality_n = (df["criticidad_estrategica"] / 5.0).clip(0, 1)
    emissions_impact_n = anchored_score(df["impacto_esperado_emisiones_pct"], low=1.0, high=20.0) / 100.0
    df["strategic_alignment_score"] = (
        100
        * (
            0.45 * strategic_criticality_n
            + 0.30 * df["base_line_criticality"]
            + 0.25 * emissions_impact_n
        )
    )
    base_loss_n = anchored_score(df["base_loss"], low=8_000.0, high=45_000.0) / 100.0
    oee_gain_n = anchored_score(df["impacto_esperado_oee_pct"], low=1.0, high=15.0) / 100.0
    df["operational_urgency_score"] = (
        100
        * (
            0.40 * base_loss_n
            + 0.30 * df["base_line_criticality"]
            + 0.20 * oee_gain_n
            + 0.10 * np.where(df["quick_win_flag"] == 1, 1.0, 0.4)
        )
    )
    df["implementation_complexity_score_scaling_method"] = SCORE_SCALING_METHOD
    df["operational_urgency_score_scaling_method"] = SCORE_SCALING_METHOD
    df["strategic_alignment_score_scaling_method"] = SCORE_SCALING_METHOD
    df["score_scale_version"] = SCORE_SCALE_VERSION
    df["opportunity_feature_comparability_tag"] = COMPARABILITY_CONTEXTUAL

    cols = [
        "iniciativa_id",
        "planta_id",
        "linea_id",
        "equipo_id",
        "categoria_iniciativa",
        "tipo_iniciativa",
        "descripcion_resumida",
        "expected_energy_saving",
        "expected_oee_gain",
        "expected_emissions_reduction",
        "annual_saving_proxy",
        "implementation_complexity_score",
        "execution_risk_score",
        "payback_months",
        "strategic_alignment_score",
        "operational_urgency_score",
        "quick_win_flag",
        "capex_estimado",
        "implementation_complexity_score_scaling_method",
        "operational_urgency_score_scaling_method",
        "strategic_alignment_score_scaling_method",
        "score_scale_version",
        "opportunity_feature_comparability_tag",
    ]
    return df[cols]


def _feature_dictionary() -> list[FeatureDef]:
    return [
        FeatureDef("line_day_features", "production_volume", "observada", "Volumen diario de producción equivalente por línea."),
        FeatureDef("line_day_features", "sec_vs_target", "derivada", "Brecha de intensidad energética frente a objetivo de línea."),
        FeatureDef("line_day_features", "process_deviation_density", "derivada", "Desvíos por tonelada para diferenciar ruido vs problema estructural."),
        FeatureDef("line_day_features", "line_criticality_weight", "derivada", "Peso estratégico de línea para priorización operativa."),
        FeatureDef("equipment_day_features", "energy_intensity_relative", "derivada", "Consumo del equipo relativo al promedio de su tipo."),
        FeatureDef("equipment_day_features", "performance_drift", "derivada", "Pendiente móvil de deterioro de desempeño energético."),
        FeatureDef("equipment_day_features", "energy_risk_proxy", "derivada", "Score compuesto de sobreconsumo, paradas, anomalías y criticidad."),
        FeatureDef("shift_features", "operator_pressure_proxy", "derivada", "Proxy de presión operativa combinando demanda, tensión y mix."),
        FeatureDef("shift_features", "shift_variance_score", "derivada", "Varianza operativa de turno para comparar estabilidad."),
        FeatureDef("improvement_opportunity_features", "annual_saving_proxy", "derivada", "Ahorro anual potencial combinando ahorro declarado e impacto operativo."),
        FeatureDef("improvement_opportunity_features", "implementation_complexity_score", "derivada", "Complejidad técnica-económica considerando dificultad, CAPEX y horizonte."),
        FeatureDef("improvement_opportunity_features", "operational_urgency_score", "derivada", "Urgencia de intervención según pérdida y criticidad de línea."),
    ]


def _write_feature_dictionary(path: str) -> None:
    definitions = _feature_dictionary()
    rows = [
        "# Diccionario de Features",
        "",
        "Feature layer para benchmarking, anomalías, diagnóstico causal, escenarios y priorización.",
        "",
        "| Tabla | Feature | Tipo | Lógica de negocio |",
        "|---|---|---|---|",
    ]
    for item in definitions:
        rows.append(f"| {item.tabla} | {item.feature} | {item.tipo} | {item.logica_negocio} |")
    with open(path, "w", encoding="utf-8") as file:
        file.write("\n".join(rows))


def build_feature_layer() -> dict[str, pd.DataFrame]:
    """Construye y persiste la capa de features analíticas obligatorias."""
    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DUCKDB_PATH))

    line_day_features = _build_line_day_features(con)
    equipment_day_features = _build_equipment_day_features(con)
    shift_features = _build_shift_features(con)
    improvement_opportunity_features = _build_improvement_opportunity_features(con, line_day_features)

    line_day_features.to_csv(DATA_PROCESSED_DIR / "line_day_features.csv", index=False)
    equipment_day_features.to_csv(DATA_PROCESSED_DIR / "equipment_day_features.csv", index=False)
    shift_features.to_csv(DATA_PROCESSED_DIR / "shift_features.csv", index=False)
    improvement_opportunity_features.to_csv(DATA_PROCESSED_DIR / "improvement_opportunity_features.csv", index=False)

    con.close()

    _write_feature_dictionary(str(DOCS_DIR / "feature_dictionary.md"))

    summary = pd.DataFrame(
        {
            "tabla": [
                "line_day_features",
                "equipment_day_features",
                "shift_features",
                "improvement_opportunity_features",
            ],
            "filas": [
                len(line_day_features),
                len(equipment_day_features),
                len(shift_features),
                len(improvement_opportunity_features),
            ],
            "columnas": [
                line_day_features.shape[1],
                equipment_day_features.shape[1],
                shift_features.shape[1],
                improvement_opportunity_features.shape[1],
            ],
        }
    )
    summary.to_csv(DATA_PROCESSED_DIR / "feature_layer_summary.csv", index=False)

    return {
        "line_day_features": line_day_features,
        "equipment_day_features": equipment_day_features,
        "shift_features": shift_features,
        "improvement_opportunity_features": improvement_opportunity_features,
    }


if __name__ == "__main__":
    build_feature_layer()
