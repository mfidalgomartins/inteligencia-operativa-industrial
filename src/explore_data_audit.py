from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
REPORTS_DIR = PROJECT_ROOT / "outputs" / "reports"


TABLE_FILES = {
    "plantas": "plantas.csv",
    "lineas_proceso": "lineas_proceso.csv",
    "equipos": "equipos.csv",
    "consumos_energia_horarios": "consumos_energia_horarios.csv",
    "produccion_turno": "produccion_turno.csv",
    "desvios_proceso": "desvios_proceso.csv",
    "paradas": "paradas.csv",
    "mantenimientos": "mantenimientos.csv",
    "emisiones_estimadas": "emisiones_estimadas.csv",
    "iniciativas_mejora": "iniciativas_mejora.csv",
    "capex_proyectos": "capex_proyectos.csv",
    "contexto_operativo": "contexto_operativo.csv",
    "benchmark_externo_proxy": "benchmark_externo_proxy.csv",
    "escenarios_mejora": "escenarios_mejora.csv",
}


@dataclass(frozen=True)
class TableMeta:
    grain: str
    candidate_key: list[str]
    expected_fks: list[str]
    analytic_utility: str


TABLE_META: dict[str, TableMeta] = {
    "plantas": TableMeta(
        grain="1 fila por planta",
        candidate_key=["planta_id"],
        expected_fks=[],
        analytic_utility="Base maestra para slicing de KPI y benchmarking por planta.",
    ),
    "lineas_proceso": TableMeta(
        grain="1 fila por linea",
        candidate_key=["linea_id"],
        expected_fks=["planta_id -> plantas.planta_id"],
        analytic_utility="Define capacidades, objetivos de OEE/SEC y criticidad estructural.",
    ),
    "equipos": TableMeta(
        grain="1 fila por equipo",
        candidate_key=["equipo_id"],
        expected_fks=["linea_id -> lineas_proceso.linea_id"],
        analytic_utility="Soporta analitica de consumo, confiabilidad y riesgo por activo.",
    ),
    "consumos_energia_horarios": TableMeta(
        grain="1 fila por equipo-hora-fuente",
        candidate_key=["timestamp", "equipo_id", "fuente_energia"],
        expected_fks=["planta_id -> plantas", "linea_id -> lineas_proceso", "equipo_id -> equipos"],
        analytic_utility="Base para SEC, coste unitario, anomalías energéticas y emisiones.",
    ),
    "produccion_turno": TableMeta(
        grain="1 fila por linea-turno-dia",
        candidate_key=["fecha", "turno", "linea_id"],
        expected_fks=["planta_id -> plantas", "linea_id -> lineas_proceso"],
        analytic_utility="Base operativa para productividad, utilización, calidad y OEE sintético.",
    ),
    "desvios_proceso": TableMeta(
        grain="1 fila por evento de desvio",
        candidate_key=["desvio_id"],
        expected_fks=["planta_id", "linea_id", "equipo_id"],
        analytic_utility="Permite explicar degradación de proceso y pérdidas de calidad/energía.",
    ),
    "paradas": TableMeta(
        grain="1 fila por evento de parada",
        candidate_key=["parada_id"],
        expected_fks=["planta_id", "linea_id", "equipo_id"],
        analytic_utility="Impacto de disponibilidad y coste por causas operativas.",
    ),
    "mantenimientos": TableMeta(
        grain="1 fila por intervención",
        candidate_key=["mantenimiento_id"],
        expected_fks=["planta_id", "linea_id", "equipo_id"],
        analytic_utility="Relaciona carga de mantenimiento con desempeño y riesgo.",
    ),
    "emisiones_estimadas": TableMeta(
        grain="1 fila por linea-hora-fuente",
        candidate_key=["timestamp", "linea_id", "fuente_energia"],
        expected_fks=["planta_id", "linea_id"],
        analytic_utility="Cuantifica intensidad de emisiones y potencial de abatimiento.",
    ),
    "iniciativas_mejora": TableMeta(
        grain="1 fila por iniciativa",
        candidate_key=["iniciativa_id"],
        expected_fks=["planta_id", "linea_id", "equipo_id (opcional)"],
        analytic_utility="Base de priorización de valor operativo/energético.",
    ),
    "capex_proyectos": TableMeta(
        grain="1 fila por proyecto",
        candidate_key=["proyecto_id"],
        expected_fks=["iniciativa_id -> iniciativas_mejora.iniciativa_id"],
        analytic_utility="Evalúa factibilidad económica y payback de iniciativas.",
    ),
    "contexto_operativo": TableMeta(
        grain="1 fila por planta-turno-dia",
        candidate_key=["fecha", "turno", "planta_id"],
        expected_fks=["planta_id -> plantas"],
        analytic_utility="Contextualiza performance por demanda, mix y precio energético.",
    ),
    "benchmark_externo_proxy": TableMeta(
        grain="1 fila por periodo-familia",
        candidate_key=["periodo", "familia_proceso"],
        expected_fks=["familia_proceso -> lineas_proceso.familia_proceso"],
        analytic_utility="Comparador externo para detectar gaps estructurales.",
    ),
    "escenarios_mejora": TableMeta(
        grain="1 fila por periodo-escenario",
        candidate_key=["fecha", "escenario"],
        expected_fks=[],
        analytic_utility="Shocks exógenos para stress-testing del scenario engine.",
    ),
}


def _load_tables() -> dict[str, pd.DataFrame]:
    tables: dict[str, pd.DataFrame] = {}
    for name, file_name in TABLE_FILES.items():
        tables[name] = pd.read_csv(RAW_DIR / file_name)
    return tables


def _detect_col_type(col: str, series: pd.Series) -> str:
    lname = col.lower()
    if lname.endswith("_id") or lname in {"turno", "escenario"}:
        return "identificadores"
    if lname in {"fecha", "timestamp", "fecha_inicio", "fecha_fin", "periodo", "fecha_instalacion"}:
        return "temporales"
    if lname.endswith("_flag") or set(series.dropna().unique()).issubset({0, 1, True, False}):
        return "booleanas"
    if pd.api.types.is_numeric_dtype(series):
        return "metricas"
    if series.dtype == object:
        if series.nunique(dropna=True) <= max(100, len(series) * 0.02):
            return "dimensiones"
        return "estructurales"
    return "estructurales"


def _main_temporal_coverage(df: pd.DataFrame) -> tuple[str, str, str]:
    temporal_cols = [c for c in ["timestamp", "fecha", "fecha_inicio", "periodo"] if c in df.columns]
    if not temporal_cols:
        return "", "", ""
    col = temporal_cols[0]
    s = pd.to_datetime(df[col], errors="coerce")
    if s.notna().sum() == 0:
        return col, "", ""
    return col, s.min().isoformat(), s.max().isoformat()


def _candidate_key_duplicates(df: pd.DataFrame, key_cols: list[str]) -> int:
    if not key_cols or not all(col in df.columns for col in key_cols):
        return -1
    return int(df.duplicated(subset=key_cols).sum())


def _build_table_summary(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for name, df in tables.items():
        meta = TABLE_META[name]
        tcol, start, end = _main_temporal_coverage(df)
        null_rate = float(df.isna().mean().mean()) if len(df) else 0.0
        key_dup = _candidate_key_duplicates(df, meta.candidate_key)
        rows.append(
            {
                "tabla": name,
                "grain": meta.grain,
                "candidate_key": " + ".join(meta.candidate_key),
                "foreign_keys_esperadas": "; ".join(meta.expected_fks),
                "filas": len(df),
                "columnas": len(df.columns),
                "columna_temporal_referencia": tcol,
                "cobertura_inicio": start,
                "cobertura_fin": end,
                "null_rate_promedio": round(null_rate, 6),
                "duplicados_candidate_key": key_dup,
                "cardinalidad_promedio_columnas": round(float(df.nunique(dropna=True).mean()), 2),
                "utilidad_analitica": meta.analytic_utility,
            }
        )
    return pd.DataFrame(rows)


def _build_column_summary(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for tname, df in tables.items():
        for col in df.columns:
            s = df[col]
            ctype = _detect_col_type(col, s)
            row: dict[str, Any] = {
                "tabla": tname,
                "columna": col,
                "clasificacion": ctype,
                "dtype": str(s.dtype),
                "null_rate": round(float(s.isna().mean()), 6),
                "n_unique": int(s.nunique(dropna=True)),
            }
            if pd.api.types.is_numeric_dtype(s):
                row["min"] = float(np.nanmin(s)) if s.notna().any() else np.nan
                row["p50"] = float(np.nanpercentile(s.dropna(), 50)) if s.notna().any() else np.nan
                row["p95"] = float(np.nanpercentile(s.dropna(), 95)) if s.notna().any() else np.nan
                row["max"] = float(np.nanmax(s)) if s.notna().any() else np.nan
            else:
                row["min"] = ""
                row["p50"] = ""
                row["p95"] = ""
                row["max"] = ""
            rows.append(row)
    return pd.DataFrame(rows)


def _build_issue_log(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    issues: list[dict[str, Any]] = []

    def add(severity: str, table: str, issue: str, count: int, impact: str, recommendation: str) -> None:
        if count <= 0:
            return
        issues.append(
            {
                "severity": severity,
                "table": table,
                "issue": issue,
                "issue_count": int(count),
                "impact": impact,
                "recommendation": recommendation,
            }
        )

    # Nulls críticos en claves.
    for name, df in tables.items():
        key_cols = [c for c in TABLE_META[name].candidate_key if c in df.columns]
        for c in key_cols:
            add(
                "P1",
                name,
                f"nulls_criticos_en_{c}",
                int(df[c].isna().sum()),
                "Riesgo de joins rotos y agregaciones sesgadas.",
                "Imputar/descartar filas y reforzar regla de not-null en staging.",
            )

    # Duplicados candidate key.
    for name, df in tables.items():
        dup = _candidate_key_duplicates(df, TABLE_META[name].candidate_key)
        if dup > 0:
            add(
                "P1",
                name,
                "duplicados_en_candidate_key",
                dup,
                "Doble conteo en KPIs y errores de integridad.",
                "Definir PK técnica y deduplicación determinista por timestamp de carga.",
            )

    # Incoherencias temporales.
    par = tables["paradas"].copy()
    par_start = pd.to_datetime(par["fecha_inicio"], errors="coerce")
    par_end = pd.to_datetime(par["fecha_fin"], errors="coerce")
    bad_par_time = int((par_end < par_start).sum())
    add("P1", "paradas", "incoherencia_temporal_inicio_mayor_fin", bad_par_time, "Duraciones negativas invalidan disponibilidad.", "Corregir evento con regla end=max(end,start+epsilon).")

    man = tables["mantenimientos"].copy()
    man_start = pd.to_datetime(man["fecha_inicio"], errors="coerce")
    man_end = pd.to_datetime(man["fecha_fin"], errors="coerce")
    bad_man_time = int((man_end < man_start).sum())
    add("P1", "mantenimientos", "incoherencia_temporal_inicio_mayor_fin", bad_man_time, "Costes y horas inconsistentes.", "Normalizar temporalidad y recalcular horas_mantenimiento.")

    # Consumos negativos o imposibles.
    ene = tables["consumos_energia_horarios"]
    add("P1", "consumos_energia_horarios", "consumo_kwh_no_positivo", int((ene["consumo_kwh"] <= 0).sum()), "SEC no interpretable.", "Aplicar regla consumo_kwh>0 en staging.")
    add("P2", "consumos_energia_horarios", "factor_carga_fuera_rango", int(((ene["factor_carga"] < 0) | (ene["factor_carga"] > 1.2)).sum()), "Anomalías falsas/espurias.", "Clipping y control de sensor.")

    # OEE sintético fuera de rango.
    prod = tables["produccion_turno"].copy()
    oee_syn = (prod["horas_operativas"] / prod["horas_planificadas"]) * (1 - prod["scrap_pct"] / 100) * prod["utilization_rate"]
    add("P2", "produccion_turno", "oee_sintetico_fuera_rango", int(((oee_syn < 0) | (oee_syn > 1.05)).sum()), "Comparativas de eficiencia distorsionadas.", "Revisar denominadores y clipping de utilization/scrap.")

    # SEC imposible.
    e = ene.copy()
    e["timestamp"] = pd.to_datetime(e["timestamp"]) 
    e["fecha"] = e["timestamp"].dt.date.astype(str)
    e["turno"] = e["timestamp"].dt.hour.map(lambda h: "A" if 6 <= h < 14 else ("B" if 14 <= h < 22 else "C"))
    sec_df = (
        e.groupby(["fecha", "turno", "linea_id"], as_index=False)["consumo_kwh"].sum()
        .merge(prod[["fecha", "turno", "linea_id", "toneladas_producidas_proxy"]], on=["fecha", "turno", "linea_id"], how="left")
    )
    sec_df["sec"] = sec_df["consumo_kwh"] / sec_df["toneladas_producidas_proxy"].replace(0, np.nan)
    add("P2", "linea_turno_sec", "sec_fuera_rango", int(((sec_df["sec"] < 50) | (sec_df["sec"] > 3000)).fillna(False).sum()), "Benchmark energético comprometido.", "Revisar producción neta y consumo agregado por turno.")

    # Emisiones inconsistentes con consumo.
    emi = tables["emisiones_estimadas"].copy()
    ratio = emi["emisiones_co2e_kg"] / emi["consumo_kwh"].replace(0, np.nan)
    diff = (ratio - emi["factor_emision_proxy"]).abs()
    add("P1", "emisiones_estimadas", "emisiones_no_coherentes_con_consumo", int((diff > 1e-3).sum()), "Riesgo en narrativa de sostenibilidad.", "Recalcular emisiones = consumo * factor con tolerancia definida.")

    # Desvíos sin impacto.
    des = tables["desvios_proceso"]
    add("P2", "desvios_proceso", "desvios_sin_impacto", int((des["impacto_en_calidad_proxy"] <= 0).sum()), "Subestima pérdidas de calidad/proceso.", "Definir impacto mínimo >0 para eventos reales.")

    # Paradas sin duración.
    add("P1", "paradas", "paradas_sin_duracion", int((tables["paradas"]["duracion_horas"] <= 0).sum()), "Disponibilidad inválida.", "Corregir o eliminar eventos de duración cero/negativa.")

    # Mantenimientos incoherentes.
    incoh_mant = int(((man["correctivo_flag"] == 1) & (man["programado_flag"] == 1)).sum())
    add("P2", "mantenimientos", "flags_programado_correctivo_incoherentes", incoh_mant, "Clasificación de intervenciones sesgada.", "Forzar exclusión mutua o permitir híbrido explícito con tercer estado.")

    # Iniciativas sin lógica económica.
    cap = tables["capex_proyectos"]
    add("P2", "capex_proyectos", "payback_no_razonable", int(((cap["payback_estimado_meses"] < 1) | (cap["payback_estimado_meses"] > 240)).sum()), "Priorización errónea de portafolio.", "Acotar supuestos económicos y revisar ahorro anual proxy.")
    add("P2", "capex_proyectos", "ahorro_anual_no_positivo", int((cap["ahorro_anual_estimado"] <= 0).sum()), "Iniciativas inviables mezcladas con viables.", "Filtrar iniciativas sin ahorro o reclasificar como compliance-only.")

    if not issues:
        issues.append(
            {
                "severity": "P3",
                "table": "global",
                "issue": "sin_issues_criticos_detectados",
                "issue_count": 0,
                "impact": "Sin impacto crítico identificado en profiling base.",
                "recommendation": "Mantener monitoreo continuo y controles de regresión.",
            }
        )

    out = pd.DataFrame(issues)
    severity_order = {"P1": 1, "P2": 2, "P3": 3}
    out["_ord"] = out["severity"].map(severity_order)
    out = out.sort_values(["_ord", "issue_count"], ascending=[True, False]).drop(columns="_ord")
    return out


def _official_joins_proposal() -> pd.DataFrame:
    rows = [
        {"join_name": "linea_turno_contexto", "left_table": "produccion_turno", "right_table": "contexto_operativo", "join_keys": "fecha, turno, planta_id", "join_type": "LEFT", "purpose": "Contextualizar productividad por presión operativa y precio"},
        {"join_name": "linea_turno_energia", "left_table": "produccion_turno", "right_table": "consumos_energia_horarios (agregado)", "join_keys": "fecha, turno, linea_id", "join_type": "LEFT", "purpose": "Calcular SEC y coste unitario por turno"},
        {"join_name": "linea_eventos_parada", "left_table": "paradas", "right_table": "lineas_proceso", "join_keys": "linea_id", "join_type": "INNER", "purpose": "Monetizar impacto de paradas por capacidad nominal"},
        {"join_name": "equipo_eventos_desvio", "left_table": "desvios_proceso", "right_table": "equipos", "join_keys": "equipo_id", "join_type": "INNER", "purpose": "Cuantificar riesgo y deterioro por subsistema"},
        {"join_name": "iniciativa_capex", "left_table": "iniciativas_mejora", "right_table": "capex_proyectos", "join_keys": "iniciativa_id", "join_type": "LEFT", "purpose": "Priorización técnico-económica"},
        {"join_name": "linea_benchmark", "left_table": "lineas_proceso", "right_table": "benchmark_externo_proxy", "join_keys": "familia_proceso + periodo", "join_type": "LEFT", "purpose": "Gap interno vs referencia externa"},
    ]
    return pd.DataFrame(rows)


def _marts_candidate_proposal() -> pd.DataFrame:
    rows = [
        {"mart_name": "mart_line_day", "grain": "linea-dia", "main_inputs": "produccion_turno + energia agregada + emisiones + paradas + desvios", "business_use": "Diagnóstico diario de eficiencia y pérdidas"},
        {"mart_name": "mart_equipment_day", "grain": "equipo-dia", "main_inputs": "consumos_energia_horarios + paradas + mantenimientos + desvios", "business_use": "Riesgo y sobreconsumo por equipo"},
        {"mart_name": "mart_line_shift", "grain": "linea-turno-dia", "main_inputs": "produccion_turno + contexto_operativo + energia agregada", "business_use": "Variabilidad operativa por turno"},
        {"mart_name": "mart_root_cause", "grain": "causa-linea-mes", "main_inputs": "paradas + desvios + impacto economico proxy", "business_use": "Priorización de causas raíz"},
        {"mart_name": "mart_initiative_value_pool", "grain": "iniciativa", "main_inputs": "iniciativas_mejora + capex_proyectos + criticidad", "business_use": "Cartera de mejora y secuencia de ejecución"},
    ]
    return pd.DataFrame(rows)


def _build_markdown_report(
    table_summary: pd.DataFrame,
    column_summary: pd.DataFrame,
    issues: pd.DataFrame,
    joins: pd.DataFrame,
    marts: pd.DataFrame,
) -> str:
    top_nulls = column_summary.sort_values("null_rate", ascending=False).head(20)
    critical = issues[issues["severity"] == "P1"]

    md = []
    md.append("# Explore-Data Audit Report\n")
    md.append("## Objetivo")
    md.append("Auditar data quality y data readiness del sistema industrial antes de modelado avanzado, priorizando riesgos que alteran decisiones operativas, energéticas y de inversión.\n")

    md.append("## Resumen por dataset")
    md.append(table_summary.to_markdown(index=False))

    md.append("\n## Clasificación de columnas y distribución")
    md.append(
        column_summary.groupby(["tabla", "clasificacion"], as_index=False)
        .size()
        .rename(columns={"size": "n_columnas"})
        .to_markdown(index=False)
    )

    md.append("\n## Null rates principales")
    md.append(top_nulls[["tabla", "columna", "clasificacion", "null_rate", "n_unique"]].to_markdown(index=False))

    md.append("\n## Issues priorizados")
    md.append(issues.to_markdown(index=False))

    md.append("\n## Interpretación senior y efecto potencial")
    if critical.empty:
        md.append("- No se detectaron issues P1 activos en esta ejecución base.")
    else:
        md.append("- Los issues P1 detectados tienen impacto directo sobre confiabilidad de KPI ejecutivos y ranking de iniciativas.")
    md.append("- Los checks de consistencia consumo-emisión y temporalidad son claves para evitar sobreinterpretación en narrativa de descarbonización.")
    md.append("- La robustez de candidate keys habilita integración SQL defendible en entrevista técnica.")

    md.append("\n## Recomendaciones para transformación analítica")
    md.append("- Aplicar contratos de datos con not-null y uniqueness en candidate keys de eventos y hechos energéticos.")
    md.append("- Introducir capa de integración línea-hora para estandarizar denominadores de SEC/OEE.")
    md.append("- Versionar supuestos económicos (margen, coste energía, coste emisiones) para trazabilidad de impacto.")
    md.append("- Implementar quality gates automáticos pre-dashboard en cada corrida del pipeline.")

    md.append("\n## Propuesta de joins oficiales")
    md.append(joins.to_markdown(index=False))

    md.append("\n## Propuesta de marts analíticos")
    md.append(marts.to_markdown(index=False))

    return "\n".join(md)


def _build_html_report(md_text: str, table_summary: pd.DataFrame, issues: pd.DataFrame) -> str:
    kpi_cards = {
        "datasets": len(table_summary),
        "filas_totales": int(table_summary["filas"].sum()),
        "issues_p1": int((issues["severity"] == "P1").sum()),
        "issues_totales": len(issues),
    }

    html = f"""
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="utf-8" />
<title>Explore Data Audit</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 26px; background:#f7fafc; color:#1f2937; }}
h1, h2 {{ color:#0f172a; }}
.grid {{ display:grid; grid-template-columns: repeat(4, minmax(160px,1fr)); gap:12px; margin-bottom:16px; }}
.card {{ background:white; border-radius:10px; padding:12px; box-shadow:0 1px 4px rgba(0,0,0,0.1); }}
.card .v {{ font-size:22px; font-weight:700; }}
pre {{ white-space: pre-wrap; background:white; padding:16px; border-radius:10px; box-shadow:0 1px 4px rgba(0,0,0,0.08); }}
</style>
</head>
<body>
<h1>Explore-Data Audit</h1>
<div class="grid">
  <div class="card"><div>Datasets</div><div class="v">{kpi_cards['datasets']}</div></div>
  <div class="card"><div>Filas totales</div><div class="v">{kpi_cards['filas_totales']:,}</div></div>
  <div class="card"><div>Issues P1</div><div class="v">{kpi_cards['issues_p1']}</div></div>
  <div class="card"><div>Issues totales</div><div class="v">{kpi_cards['issues_totales']}</div></div>
</div>
<h2>Informe completo</h2>
<pre>{md_text}</pre>
</body>
</html>
"""
    return html


def run_explore_data_audit() -> dict[str, pd.DataFrame]:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    tables = _load_tables()
    table_summary = _build_table_summary(tables)
    column_summary = _build_column_summary(tables)
    issues = _build_issue_log(tables)
    joins = _official_joins_proposal()
    marts = _marts_candidate_proposal()

    table_summary.to_csv(PROCESSED_DIR / "explore_data_table_summary.csv", index=False)
    column_summary.to_csv(PROCESSED_DIR / "explore_data_column_profile.csv", index=False)
    issues.to_csv(PROCESSED_DIR / "explore_data_issues.csv", index=False)
    joins.to_csv(PROCESSED_DIR / "explore_data_official_joins.csv", index=False)
    marts.to_csv(PROCESSED_DIR / "explore_data_mart_candidates.csv", index=False)

    report_md = _build_markdown_report(table_summary, column_summary, issues, joins, marts)
    (REPORTS_DIR / "explore_data_audit_report.md").write_text(report_md, encoding="utf-8")

    report_html = _build_html_report(report_md, table_summary, issues)
    (REPORTS_DIR / "explore_data_audit_report.html").write_text(report_html, encoding="utf-8")

    return {
        "table_summary": table_summary,
        "column_summary": column_summary,
        "issues": issues,
        "joins": joins,
        "marts": marts,
    }


if __name__ == "__main__":
    run_explore_data_audit()
