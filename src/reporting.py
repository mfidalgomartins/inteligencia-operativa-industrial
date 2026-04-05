from __future__ import annotations

from pathlib import Path

import pandas as pd

from .config import DATA_PROCESSED_DIR, OUTPUT_REPORTS_DIR, PROJECT_ROOT
from .runtime_claims import sync_readme_runtime_claims


def _list_files(base: Path) -> list[str]:
    if not base.exists():
        return []
    return sorted(str(p.relative_to(PROJECT_ROOT)) for p in base.rglob("*") if p.is_file())


def build_reports() -> None:
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    line_scores = pd.read_csv(DATA_PROCESSED_DIR / "line_criticality_scores.csv")
    invest = pd.read_csv(DATA_PROCESSED_DIR / "investment_prioritization_final.csv")
    snapshot = pd.read_csv(DATA_PROCESSED_DIR / "executive_kpi_snapshot.csv").iloc[0]
    decision = pd.read_csv(DATA_PROCESSED_DIR / "executive_decision_snapshot.csv").iloc[0]
    financial_audit = pd.read_csv(DATA_PROCESSED_DIR / "financial_logic_audit_table.csv")
    financial_before_after = pd.read_csv(DATA_PROCESSED_DIR / "financial_metrics_before_after.csv")
    findings = pd.read_csv(DATA_PROCESSED_DIR / "main_findings_prioritized.csv")
    validation = pd.read_csv(DATA_PROCESSED_DIR / "validation_checklist_final.csv")
    readiness = pd.read_csv(DATA_PROCESSED_DIR / "validation_readiness_summary.csv").iloc[0]
    readiness_level = str(readiness.get("overall_readiness_level", "unknown"))
    publish_decision = str(readiness.get("publish_decision", "unknown"))
    committee_state = str(readiness.get("committee_readiness_state", "unknown"))

    top_line_id = str(decision["linea_prioritaria"])
    top_line = (
        line_scores[line_scores["linea_id"].astype(str) == top_line_id].head(1).iloc[0]
        if (line_scores["linea_id"].astype(str) == top_line_id).any()
        else line_scores.sort_values("line_criticality_score", ascending=False).iloc[0]
    )
    top_equipment_id = str(decision["equipo_prioritario"])
    ahorro_ahora = float(decision["ahorro_now_top5"])
    screening_count = int((invest["financial_maturity_stage"] == "screening").sum()) if "financial_maturity_stage" in invest.columns else 0
    pre_feas_count = int((invest["financial_maturity_stage"] == "pre_feasibility").sum()) if "financial_maturity_stage" in invest.columns else 0
    bc_candidate_count = int((invest["financial_maturity_stage"] == "business_case_candidate").sum()) if "financial_maturity_stage" in invest.columns else 0

    memo = [
        "# Memo Ejecutivo (Screening y Priorización Industrial)",
        "",
        "## 1. Contexto",
        "La planta opera bajo presión combinada de coste energético, disponibilidad y variabilidad de proceso.",
        "",
        "## 2. Problema",
        "La pérdida de eficiencia y confiabilidad se concentra en nodos concretos (línea-equipo-turno), afectando coste y riesgo operativo.",
        "",
        "## 3. Enfoque metodológico",
        "Plataforma analítica gobernada (SQL + features + scoring interpretable + escenarios + priorización) para pre-screening y secuenciación de iniciativas.",
        "",
        "## 3.1 Alcance decisional explícito",
        "- Este sistema soporta priorización operativa y pre-feasibility.",
        "- Este sistema NO reemplaza un business case corporativo final ni la aprobación financiera formal de comité.",
        "",
        "## 4. Hallazgos principales",
        findings.to_markdown(index=False),
        "",
        "## 5. Implicaciones operativas",
        f"Priorizar la línea **{top_line['linea_id']}** por su criticidad compuesta.",
        f"Intervenir el equipo **{top_equipment_id}** para contener sobreconsumo y riesgo de fallo.",
        "",
        "## 6. Implicaciones energéticas",
        "La brecha SEC persistente sugiere pérdidas estructurales en régimen operativo y disciplina de proceso.",
        "",
        "## 7. Implicaciones económicas",
        f"Actuar en iniciativas de ejecución inmediata captura un ahorro potencial **proxy** aproximado de **{ahorro_ahora:,.0f} EUR/año**.",
        f"Madurez financiera del pipeline: screening={screening_count}, pre-feasibility={pre_feas_count}, business-case-candidate={bc_candidate_count}.",
        "NPV/IRR/VaR se usan con propósito de screening y priorización; no equivalen a una valoración financiera corporativa auditada.",
        "El modelo es apto para screening/pre-feasibility; no reemplaza business case corporativo auditado.",
        f"Estado de release: readiness={readiness_level}, committee_state={committee_state}, publish_decision={publish_decision}.",
        "",
        "## 8. Implicaciones ambientales",
        "La reducción de SEC y paradas reduce simultáneamente emisiones proxy por unidad producida.",
        "",
        "## 9. Trade-offs principales",
        "- Ahorro rápido (quick wins) vs transformación estructural (CAPEX).",
        "- Payback corto vs criticidad alta.",
        "- Viabilidad de ejecución vs impacto estratégico.",
        "",
        "## 10. Prioridades de mejora",
        "1. Ejecutar quick wins y mantenimiento en líneas críticas (ola 1).",
        "2. Consolidar optimización de proceso en turnos con mayor varianza (ola 2).",
        "3. Preparar CAPEX en iniciativas con impacto estructural y gating técnico-financiero (ola 3).",
        "",
        "## 11. Limitaciones",
        "- El modelo utiliza datos sintéticos y proxies económicos/ambientales.",
        "- La optimización de cartera es formal para su formulación actual, pero sigue sujeta a supuestos de datos sintéticos y restricciones modeladas.",
        "- La adaptación sectorial está parametrizada en taxonomía/benchmark/factores/constraints; la validación final requiere datos reales de planta/refino.",
        "- Si `publish_decision` es `no publish` o `publish with caveat`, no debe presentarse como material final de comité.",
        "",
        "## 12. Próximos pasos",
        "Validar supuestos con operaciones, energía y mantenimiento; ejecutar pilotos controlados por línea crítica y cerrar business cases de iniciativas top antes de comité final.",
    ]
    (OUTPUT_REPORTS_DIR / "memo_ejecutivo.md").write_text("\n".join(memo), encoding="utf-8")

    financial_report = [
        "# Financial Logic Audit - Before/After",
        "",
        "## Clasificación de la lógica financiera actual",
        financial_audit.to_markdown(index=False),
        "",
        "## Puente before/after de métricas financieras",
        financial_before_after.to_markdown(index=False),
        "",
        "## Nota de credibilidad",
        "- El framework actual es defendible para screening y pre-feasibility.",
        "- No debe presentarse como evaluación financiera final de comité de inversión.",
    ]
    (OUTPUT_REPORTS_DIR / "financial_before_after_report.md").write_text("\n".join(financial_report), encoding="utf-8")

    # Ensamblaje final
    files_created = _list_files(PROJECT_ROOT / "src")
    files_sql = _list_files(PROJECT_ROOT / "sql")
    files_docs = _list_files(PROJECT_ROOT / "docs")
    files_outputs = _list_files(PROJECT_ROOT / "outputs")

    summary = [
        "# Ensamblaje Final del Proyecto",
        "",
        "## Estructura y artefactos",
        f"- Archivos `src/`: {len(files_created)}",
        f"- Archivos `sql/`: {len(files_sql)}",
        f"- Archivos `docs/`: {len(files_docs)}",
        f"- Archivos `outputs/`: {len(files_outputs)}",
        "",
        "## Resultado ejecutivo",
        f"- Línea a intervenir primero: **{decision['linea_prioritaria']}**.",
        f"- Equipo a priorizar: **{decision['equipo_prioritario']}**.",
        f"- Ahorro potencial de actuar ahora: **{ahorro_ahora:,.0f} EUR/año**.",
        f"- Readiness global: **{readiness_level}** | committee_state: **{committee_state}** | publish_decision: **{publish_decision}**.",
        f"- Checks de validación ejecutados: **{len(validation)}**.",
    ]
    (OUTPUT_REPORTS_DIR / "final_project_assembly.md").write_text("\n".join(summary), encoding="utf-8")

    # Manifesto de entrega final
    sql_log = pd.read_csv(DATA_PROCESSED_DIR / "sql_execution_log.csv")
    manifest = [
        "# Final Delivery Manifest",
        "",
        "## 1) Estructura final del repositorio",
        "- `data/raw`, `data/processed`, `sql`, `src`, `notebooks`, `outputs`, `docs`.",
        "",
        "## 2) Lista de archivos creados/actualizados (resumen)",
        f"- `src/`: {len(files_created)} archivos.",
        f"- `sql/`: {len(files_sql)} archivos.",
        f"- `docs/`: {len(files_docs)} archivos.",
        f"- `outputs/`: {len(files_outputs)} archivos.",
        "",
        "## 3) Scripts ejecutados",
        "- `generate_synthetic_data`",
        "- `run_explore_data_audit`",
        "- `run_sql_pipeline`",
        "- `build_feature_layer`",
        "- `build_root_cause_ranking`",
        "- `run_scenario_engine`",
        "- `run_investment_prioritization`",
        "- `run_sector_parameterization`",
        "- `run_advanced_analysis`",
        "- `generate_charts`",
        "- `build_dashboard`",
        "- `run_full_validation`",
        "- `run_repro_packaging`",
        "",
        "## 4) SQL scripts ejecutados",
        sql_log.to_markdown(index=False),
        "",
        "## 5) Datos generados",
        "- Tablas raw industriales (14 datasets) + resúmenes de plausibilidad y cardinalidad.",
        "",
        "## 6) Tablas analíticas creadas",
        "- Integración: `int_line_hour`, `int_equipment_day`, `int_line_shift`.",
        "- Marts: `mart_line_day`, `mart_equipment_day`, `mart_plant_week`.",
        "- Features/scoring: `line_day_features_scored`, `equipment_energy_anomaly_scores`, `line_criticality_scores`, `investment_prioritization_final`.",
        "",
        "## 7) Outputs generados",
        "- Gráficos premium (18).",
        "- Dashboard HTML final.",
        "- Reportes analíticos, validación y memo ejecutivo.",
        "",
        "## 8) Dashboard HTML final",
        "- `outputs/dashboard/dashboard_inteligencia_operativa.html`.",
        "",
        "## 9) Resumen ejecutivo final",
        f"- Línea prioritaria: **{decision['linea_prioritaria']}**.",
        f"- Equipo prioritario: **{decision['equipo_prioritario']}**.",
        f"- Ahorro potencial inmediato: **{ahorro_ahora:,.0f} EUR/año**.",
        "",
        "## 10) Hallazgos principales",
        findings.to_markdown(index=False),
        "",
        "## 11) Resumen de validación",
        f"- Checks ejecutados: **{len(validation)}**.",
        f"- Checks con `ERROR`: **{int((validation['status'] == 'ERROR').sum())}**.",
        f"- Readiness global: **{readiness_level}**.",
        f"- Committee state: **{committee_state}**.",
        f"- Publish decision: **{publish_decision}**.",
        "",
        "## 12) Limitaciones",
        "- Dataset sintético y proxies económicos/ambientales.",
        "- Requiere calibración con datos reales de operación.",
        "",
        "## 13) Próximos pasos",
        "1. Integración con datos reales MES/historian/ERP.",
        "2. Ajuste de pesos de priorización con comité industrial.",
        "3. Pilotaje por línea crítica y despliegue por olas.",
        "",
        "## 14) Publicación en GitHub (sugerencia exacta)",
        "1. Subir repo con estructura limpia y README principal.",
        "2. Añadir capturas de 4-6 gráficos clave en `README`.",
        "3. Destacar `outputs/dashboard/dashboard_inteligencia_operativa.html` como demo.",
        "4. Incluir sección de decisión ejecutiva y limitaciones explícitas.",
        "5. Versionar con tags: `v1.0-data`, `v1.1-sql`, `v1.2-dashboard`.",
    ]
    (OUTPUT_REPORTS_DIR / "final_delivery_manifest.md").write_text("\n".join(manifest), encoding="utf-8")

    sync_readme_runtime_claims(
        readme_path=PROJECT_ROOT / "README.md",
        snapshot_row=snapshot,
        decision_row=decision,
        readiness_row=readiness,
    )


if __name__ == "__main__":
    build_reports()
