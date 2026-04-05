from __future__ import annotations

import pandas as pd

from .config import DATA_PROCESSED_DIR, OUTPUT_REPORTS_DIR


def run_advanced_analysis() -> dict[str, pd.DataFrame]:
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    line = pd.read_csv(DATA_PROCESSED_DIR / "line_criticality_scores.csv")
    equip = pd.read_csv(DATA_PROCESSED_DIR / "equipment_energy_anomaly_scores.csv")
    causes = pd.read_csv(DATA_PROCESSED_DIR / "loss_root_cause_score.csv")
    shift = pd.read_csv(DATA_PROCESSED_DIR / "shift_comparison_scores.csv")
    scenario = pd.read_csv(DATA_PROCESSED_DIR / "scenario_ranking.csv")
    invest = pd.read_csv(DATA_PROCESSED_DIR / "investment_prioritization_final.csv")
    decision = pd.read_csv(DATA_PROCESSED_DIR / "executive_decision_snapshot.csv").iloc[0]

    top_line_id = str(decision["linea_prioritaria"])
    top_equip_id = str(decision["equipo_prioritario"])
    top_line = (
        line[line["linea_id"].astype(str) == top_line_id].head(1).iloc[0]
        if (line["linea_id"].astype(str) == top_line_id).any()
        else line.sort_values("line_criticality_score", ascending=False).head(1).iloc[0]
    )
    top_equip_score = float(decision["equipment_energy_anomaly_score"])
    top_cause = causes.sort_values("loss_root_cause_score", ascending=False).head(1).iloc[0]
    top_shift = shift.sort_values("shift_variance_score", ascending=False).head(1).iloc[0]

    total_loss_proxy = float(line["loss_value_proxy"].sum())
    avg_sec_gap = float(line["sec_benchmark_gap"].mean())
    avg_oee = float(line["oee_synthetic"].mean())

    quick_wins = invest[invest["quick_win_flag"] == 1].sort_values("improvement_priority_index", ascending=False).head(5)
    capex = invest[invest["capex_estimado"] >= 700_000].sort_values("improvement_priority_index", ascending=False).head(5)

    findings = pd.DataFrame(
        [
            {
                "prioridad": 1,
                "hallazgo": f"La línea {top_line['linea_id']} concentra la mayor criticidad integrada.",
                "impacto": "Riesgo alto de pérdida económica y degradación de continuidad.",
            },
            {
                "prioridad": 2,
                "hallazgo": f"El equipo {top_equip_id} lidera el riesgo de anomalía energética.",
                "impacto": "Penaliza coste energético y estabilidad operacional.",
            },
            {
                "prioridad": 3,
                "hallazgo": f"La causa raíz dominante es {top_cause['causa_parada']}.",
                "impacto": "Explica la mayor fracción de pérdida operativa agregada.",
            },
            {
                "prioridad": 4,
                "hallazgo": f"El turno {top_shift['turno']} en {top_shift['linea_id']} muestra mayor varianza operativa.",
                "impacto": "Necesita estandarización operativa y control de rutina.",
            },
        ]
    )

    findings.to_csv(DATA_PROCESSED_DIR / "main_findings_prioritized.csv", index=False)

    report = [
        "# Informe Analítico Avanzado",
        "",
        "## 1. Salud global de la operación",
        f"- **Insight principal**: la operación presenta brecha media SEC de {avg_sec_gap:.2f} kWh/t y OEE sintético medio {avg_oee:.3f}.",
        f"- **Evidencia cuantitativa**: pérdida económica proxy agregada {total_loss_proxy:,.0f} EUR.",
        "- **Lectura operativa**: predominan pérdidas por ineficiencia en líneas críticas y variabilidad de turno.",
        "- **Lectura económica**: el coste energético y paradas explican la mayor porción del valor perdido.",
        "- **Lectura estratégica**: hay margen para capturar ahorro rápido sin esperar CAPEX largo.",
        "- **Caveats**: modelo basado en proxies sintéticos; requiere calibración con históricos reales.",
        "- **Recomendación asociada**: activar frente combinado de quick wins + mantenimiento focalizado.",
        "",
        "## 2. Líneas y equipos críticos",
        f"- **Insight principal**: línea crítica prioritaria {top_line['linea_id']} y equipo crítico {top_equip_id}.",
        f"- **Evidencia cuantitativa**: score línea {top_line['line_criticality_score']:.1f}; score equipo {top_equip_score:.1f}.",
        "- **Lectura operativa**: hay patrón de deterioro persistente, no solo eventos aislados.",
        "- **Lectura económica**: concentración de impacto facilita priorización por valor.",
        "- **Lectura estratégica**: intervenir nodos críticos mejora el perfil de riesgo global.",
        "- **Caveats**: score con escala anclada (v1), comparable en contexto operativo similar; evitar lectura cross-contexto sin calibración.",
        "- **Recomendación asociada**: intervención secuencial por línea-equipo con verificación semanal.",
        "",
        "## 3. Desvíos y causas raíz",
        f"- **Insight principal**: la causa raíz dominante es {top_cause['causa_parada']}.",
        f"- **Evidencia cuantitativa**: score causal agregado {top_cause['loss_root_cause_score']:.1f}.",
        "- **Lectura operativa**: desvíos de proceso y paradas no planificadas convergen sobre pocas causas.",
        "- **Lectura económica**: la recurrencia causal amplifica pérdida por indisponibilidad.",
        "- **Lectura estratégica**: cerrar bucle causa-acción tiene mejor retorno que medidas dispersas.",
        "- **Caveats**: no se modela causalidad contrafactual.",
        "- **Recomendación asociada**: plan de mitigación por causa top-5 con owner operativo.",
        "",
        "## 4. Coste industrial y emisiones",
        "- **Insight principal**: energía y disponibilidad explican la mayor parte de pérdidas monetizadas proxy.",
        "- **Evidencia cuantitativa**: ranking de escenarios muestra ahorro anual relevante con reducción de SEC y paradas.",
        "- **Lectura operativa**: mejorar SEC reduce simultáneamente coste y presión térmica de proceso.",
        "- **Lectura económica**: mejoras de payback corto financian parte de transformaciones CAPEX.",
        "- **Lectura estratégica**: priorizar líneas de mayor intensidad de emisiones evita coste futuro regulatorio.",
        "- **Caveats**: factor de emisiones y precio energético son proxies sintéticos.",
        "- **Recomendación asociada**: roadmap dual de eficiencia energética + confiabilidad.",
        "",
        "## 5. Oportunidades de mejora",
        "- **Insight principal**: quick wins dominan el corto plazo y CAPEX domina impacto estructural.",
        f"- **Evidencia cuantitativa**: quick wins top score medio {quick_wins['improvement_priority_index'].mean():.1f}; CAPEX top score medio {capex['improvement_priority_index'].mean():.1f}.",
        "- **Lectura operativa**: quick wins estabilizan; CAPEX cambia frontera de desempeño.",
        "- **Lectura económica**: existe trade-off retorno rápido vs valor acumulado a largo plazo.",
        "- **Lectura estratégica**: conviene cartera balanceada por olas de ejecución.",
        "- **Caveats**: ejecución real puede alterar plazos y retorno.",
        "- **Recomendación asociada**: ejecutar ola 1 con quick wins + mantenimiento, preparar CAPEX con gating técnico.",
        "",
        "## 6. Implicaciones estratégicas",
        f"- **Insight principal**: actuar primero sobre {top_line['linea_id']} reduce riesgo operativo y pérdida económica más rápidamente.",
        f"- **Evidencia cuantitativa**: primera iniciativa de ranking {scenario.iloc[0]['iniciativa_id']} con score {scenario.iloc[0]['score_tradeoff']:.1f}.",
        "- **Lectura operativa**: diferir intervención en activos críticos eleva riesgo de eventos no planificados.",
        "- **Lectura económica**: la secuencia recomendada maximiza ahorro acumulado ajustado por factibilidad.",
        "- **Lectura estratégica**: mantener pipeline vivo para no perder capacidad de transformación.",
        "- **Caveats**: sensibilidad de ranking a cambios de pesos y shocks energéticos.",
        "- **Recomendación asociada**: gobernanza mensual de portafolio con revisión de sensibilidad.",
    ]

    (OUTPUT_REPORTS_DIR / "advanced_analysis_report.md").write_text("\n".join(report), encoding="utf-8")

    support_tables = {
        "line_criticality_scores": line,
        "equipment_energy_anomaly_scores": equip,
        "loss_root_cause_score": causes,
        "shift_comparison_scores": shift,
        "scenario_ranking": scenario,
        "investment_prioritization_final": invest,
    }
    for name, df in support_tables.items():
        df.to_csv(DATA_PROCESSED_DIR / f"analysis_support_{name}.csv", index=False)

    return {
        "findings": findings,
        "line": line,
        "equipment": equip,
        "invest": invest,
    }


if __name__ == "__main__":
    run_advanced_analysis()
