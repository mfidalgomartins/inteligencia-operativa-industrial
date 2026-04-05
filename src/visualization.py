from __future__ import annotations

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from .config import DATA_PROCESSED_DIR, DATA_RAW_DIR, OUTPUT_CHARTS_DIR, OUTPUT_REPORTS_DIR


plt.style.use("seaborn-v0_8-whitegrid")


def _save(fig: plt.Figure, filename: str) -> None:
    OUTPUT_CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(OUTPUT_CHARTS_DIR / filename, dpi=150)
    plt.close(fig)


def generate_charts() -> None:
    OUTPUT_CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    line_day = pd.read_csv(DATA_PROCESSED_DIR / "line_day_features_scored.csv", parse_dates=["fecha"])
    line_scores = pd.read_csv(DATA_PROCESSED_DIR / "line_criticality_scores.csv")
    equip_scores = pd.read_csv(DATA_PROCESSED_DIR / "equipment_energy_anomaly_scores.csv")
    root_cause = pd.read_csv(DATA_PROCESSED_DIR / "loss_root_cause_score.csv")
    shift = pd.read_csv(DATA_PROCESSED_DIR / "shift_comparison_scores.csv")
    scenario_table = pd.read_csv(DATA_PROCESSED_DIR / "scenario_table.csv")
    scenario_waterfall = pd.read_csv(DATA_PROCESSED_DIR / "scenario_waterfall_value.csv")
    invest = pd.read_csv(DATA_PROCESSED_DIR / "investment_prioritization_final.csv")
    sensitivity = pd.read_csv(DATA_PROCESSED_DIR / "investment_prioritization_sensitivity.csv")

    lineas = line_day["linea_id"].drop_duplicates().head(6).tolist()

    chart_index: list[dict[str, str]] = []

    # 1. trend de sec por línea
    fig, ax = plt.subplots(figsize=(12, 6))
    for linea in lineas:
        tmp = line_day[line_day["linea_id"] == linea]
        ax.plot(tmp["fecha"], tmp["sec"], label=linea, linewidth=1.2)
    ax.set_title("Tendencia SEC por línea")
    ax.set_xlabel("Fecha")
    ax.set_ylabel("kWh/t")
    ax.legend(ncol=3, fontsize=8)
    _save(fig, "01_trend_sec_por_linea.png")
    chart_index.append({"grafico": "trend sec por línea", "archivo": "01_trend_sec_por_linea.png", "insight": "Evolución temporal de intensidad energética."})

    # 2. trend de OEE por línea
    fig, ax = plt.subplots(figsize=(12, 6))
    for linea in lineas:
        tmp = line_day[line_day["linea_id"] == linea]
        ax.plot(tmp["fecha"], tmp["oee_synthetic"], label=linea, linewidth=1.2)
    ax.set_title("Tendencia OEE sintético por línea")
    ax.set_xlabel("Fecha")
    ax.set_ylabel("OEE")
    ax.legend(ncol=3, fontsize=8)
    _save(fig, "02_trend_oee_por_linea.png")
    chart_index.append({"grafico": "trend OEE por línea", "archivo": "02_trend_oee_por_linea.png", "insight": "Identifica deterioro sostenido de desempeño."})

    # 3. consumo energético y coste por línea
    agg_line = line_day.groupby("linea_id", as_index=False).agg(sec=("sec", "mean"), coste=("cost_energy_per_unit", "mean"))
    fig, ax = plt.subplots(figsize=(11, 6))
    scatter = ax.scatter(agg_line["sec"], agg_line["coste"], s=90, c=np.arange(len(agg_line)), cmap="viridis")
    for row in agg_line.itertuples(index=False):
        ax.annotate(row.linea_id, (row.sec, row.coste), fontsize=8)
    ax.set_title("Consumo energético y coste por línea")
    ax.set_xlabel("SEC medio (kWh/t)")
    ax.set_ylabel("Coste energía por unidad")
    fig.colorbar(scatter, ax=ax, label="Índice línea")
    _save(fig, "03_consumo_coste_por_linea.png")
    chart_index.append({"grafico": "consumo energético y coste por línea", "archivo": "03_consumo_coste_por_linea.png", "insight": "Detecta líneas con doble penalización energía-coste."})

    # 4. top líneas por pérdidas económicas
    top_loss = line_scores.sort_values("loss_value_proxy", ascending=False).head(12)
    fig, ax = plt.subplots(figsize=(11, 6))
    ax.bar(top_loss["linea_id"], top_loss["loss_value_proxy"], color="#c2410c")
    ax.set_title("Top líneas por pérdidas económicas proxy")
    ax.set_xlabel("Línea")
    ax.set_ylabel("EUR proxy")
    ax.tick_params(axis="x", rotation=55)
    _save(fig, "04_top_lineas_perdidas.png")
    chart_index.append({"grafico": "top líneas por pérdidas", "archivo": "04_top_lineas_perdidas.png", "insight": "Concentración de pérdida por línea."})

    # 5. top equipos por sobreconsumo
    top_eq = equip_scores.groupby("equipo_id", as_index=False).agg(score=("equipment_energy_anomaly_score", "mean")).sort_values("score", ascending=False).head(15)
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(top_eq["equipo_id"], top_eq["score"], color="#991b1b")
    ax.set_title("Top equipos por sobreconsumo/anomalía")
    ax.set_xlabel("Equipo")
    ax.set_ylabel("Score anomalía energética")
    ax.tick_params(axis="x", rotation=70)
    _save(fig, "05_top_equipos_sobreconsumo.png")
    chart_index.append({"grafico": "top equipos por sobreconsumo", "archivo": "05_top_equipos_sobreconsumo.png", "insight": "Prioriza activos para inspección energética."})

    # 6. mapa de anomalías por equipo
    heat = (
        equip_scores.groupby(["linea_id", "equipo_id"], as_index=False)["equipment_energy_anomaly_score"]
        .mean()
        .pivot(index="equipo_id", columns="linea_id", values="equipment_energy_anomaly_score")
        .fillna(0)
    )
    fig, ax = plt.subplots(figsize=(12, 8))
    im = ax.imshow(heat.values, aspect="auto", cmap="magma")
    ax.set_title("Mapa de anomalías por equipo y línea")
    ax.set_xticks(range(len(heat.columns)))
    ax.set_xticklabels(heat.columns, rotation=80, fontsize=7)
    ax.set_yticks(range(len(heat.index)))
    ax.set_yticklabels(heat.index, fontsize=7)
    fig.colorbar(im, ax=ax, label="Score anomalía")
    _save(fig, "06_mapa_anomalias_equipo.png")
    chart_index.append({"grafico": "mapa de anomalías por equipo", "archivo": "06_mapa_anomalias_equipo.png", "insight": "Patrón espacial de anomalías por activos."})

    # 7. desviaciones de proceso por familia
    desvi = pd.read_csv(DATA_RAW_DIR / "desvios_proceso.csv")
    line_meta = pd.read_csv(DATA_RAW_DIR / "lineas_proceso.csv")[["linea_id", "familia_proceso"]]
    desvi = desvi.merge(line_meta, on="linea_id", how="left")
    desvi_fam = desvi.groupby("familia_proceso", as_index=False).agg(desviacion_abs=("desviacion_pct", lambda s: np.mean(np.abs(s))))
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(desvi_fam["familia_proceso"], desvi_fam["desviacion_abs"], color="#0f766e")
    ax.set_title("Desviaciones de proceso por familia")
    ax.set_xlabel("Familia de proceso")
    ax.set_ylabel("Desviación absoluta media (%)")
    ax.tick_params(axis="x", rotation=45)
    _save(fig, "07_desviaciones_por_familia.png")
    chart_index.append({"grafico": "desviaciones de proceso por familia", "archivo": "07_desviaciones_por_familia.png", "insight": "Áreas de inestabilidad de proceso."})

    # 8. paradas por causa
    paradas = pd.read_csv(DATA_RAW_DIR / "paradas.csv")
    stops = paradas.groupby("causa_parada", as_index=False).agg(n_paradas=("parada_id", "count")).sort_values("n_paradas", ascending=False).head(12)
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.barh(stops["causa_parada"], stops["n_paradas"], color="#4b5563")
    ax.set_title("Paradas por causa")
    ax.set_xlabel("Número de paradas")
    _save(fig, "08_paradas_por_causa.png")
    chart_index.append({"grafico": "paradas por causa", "archivo": "08_paradas_por_causa.png", "insight": "Frecuencia de eventos por causa operativa."})

    # 9. pérdida económica por causa raíz
    rc = root_cause.groupby("causa_parada", as_index=False).agg(loss=("loss_root_cause_score", "sum")).sort_values("loss", ascending=False).head(12)
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.barh(rc["causa_parada"], rc["loss"], color="#b91c1c")
    ax.set_title("Pérdida económica proxy por causa raíz")
    ax.set_xlabel("Score de pérdida")
    _save(fig, "09_perdida_por_causa_raiz.png")
    chart_index.append({"grafico": "pérdida económica por causa raíz", "archivo": "09_perdida_por_causa_raiz.png", "insight": "Jerarquiza causas por impacto económico."})

    # 10. emisiones por línea
    em = line_day.groupby("linea_id", as_index=False).agg(emisiones=("emissions_per_unit", "mean")).sort_values("emisiones", ascending=False)
    fig, ax = plt.subplots(figsize=(11, 6))
    ax.bar(em["linea_id"], em["emisiones"], color="#0891b2")
    ax.set_title("Emisiones por línea (proxy por unidad)")
    ax.set_xlabel("Línea")
    ax.set_ylabel("kgCO2e por unidad (proxy)")
    ax.tick_params(axis="x", rotation=60)
    _save(fig, "10_emisiones_por_linea.png")
    chart_index.append({"grafico": "emisiones por línea", "archivo": "10_emisiones_por_linea.png", "insight": "Prioriza líneas para abatimiento de emisiones."})

    # 11. quick wins vs CAPEX
    invest["tipo_macro"] = np.where(invest["quick_win_flag"] == 1, "QuickWin", np.where(invest["capex_estimado"] >= 700_000, "CAPEX", "Intermedio"))
    comp = invest.groupby("tipo_macro", as_index=False).agg(score=("improvement_priority_index", "mean"), ahorro=("annual_saving_proxy", "sum"))
    fig, ax = plt.subplots(figsize=(9, 6))
    ax.bar(comp["tipo_macro"], comp["ahorro"], color=["#10b981", "#f59e0b", "#6366f1"])
    for row in comp.itertuples(index=False):
        ax.text(row.tipo_macro, row.ahorro, f"score {row.score:.1f}", ha="center", va="bottom", fontsize=9)
    ax.set_title("Quick Wins vs CAPEX: ahorro agregado")
    ax.set_ylabel("Ahorro anual proxy")
    _save(fig, "11_quick_wins_vs_capex.png")
    chart_index.append({"grafico": "quick wins vs CAPEX", "archivo": "11_quick_wins_vs_capex.png", "insight": "Compara retorno agregado por tipo de iniciativa."})

    # 12. payback vs impacto
    fig, ax = plt.subplots(figsize=(10, 6))
    sc = ax.scatter(invest["payback_months"], invest["improvement_priority_index"], c=invest["capex_estimado"], cmap="plasma", s=80)
    ax.set_title("Payback vs impacto de iniciativa")
    ax.set_xlabel("Payback (meses)")
    ax.set_ylabel("Improvement priority index")
    fig.colorbar(sc, ax=ax, label="CAPEX estimado")
    _save(fig, "12_payback_vs_impacto.png")
    chart_index.append({"grafico": "payback vs impacto", "archivo": "12_payback_vs_impacto.png", "insight": "Trade-off retorno financiero y prioridad."})

    # 13. waterfall de ahorro potencial
    waterfall_metric = "ahorro_ajustado_riesgo" if "ahorro_ajustado_riesgo" in scenario_waterfall.columns else "ahorro_energia"
    wf = scenario_waterfall.sort_values(waterfall_metric, ascending=False)
    fig, ax = plt.subplots(figsize=(11, 6))
    cumulative = wf[waterfall_metric].cumsum()
    ax.bar(wf["escenario_label"], wf[waterfall_metric], color="#2563eb")
    ax.plot(wf["escenario_label"], cumulative, color="#dc2626", marker="o", linewidth=2)
    ax.set_title("Waterfall de ahorro potencial por escenario")
    ax.set_ylabel("Ahorro económico anual")
    ax.tick_params(axis="x", rotation=45)
    _save(fig, "13_waterfall_ahorro_potencial.png")
    chart_index.append({"grafico": "waterfall de ahorro potencial", "archivo": "13_waterfall_ahorro_potencial.png", "insight": "Contribución incremental de escenarios al valor."})

    # 14. ranking de iniciativas
    rank_metric = "risk_adjusted_annual_savings" if "risk_adjusted_annual_savings" in scenario_table.columns else "ahorro_economico_anual_proxy"
    top_rank = (
        scenario_table.groupby("iniciativa_id", as_index=False)[rank_metric]
        .mean()
        .sort_values(rank_metric, ascending=False)
        .head(15)
    )
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(top_rank["iniciativa_id"], top_rank[rank_metric], color="#1d4ed8")
    ax.set_title("Ranking de iniciativas por ahorro potencial")
    ax.set_ylabel("Ahorro anual proxy")
    ax.tick_params(axis="x", rotation=65)
    _save(fig, "14_ranking_iniciativas.png")
    chart_index.append({"grafico": "ranking de iniciativas", "archivo": "14_ranking_iniciativas.png", "insight": "Iniciativas con mayor valor anual estimado."})

    # 15. matriz criticidad vs viabilidad
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(
        invest["strategic_priority_score"],
        invest["implementation_feasibility_score"],
        s=100,
        c=invest["improvement_priority_index"],
        cmap="viridis",
    )
    ax.set_title("Matriz criticidad estratégica vs viabilidad")
    ax.set_xlabel("Prioridad estratégica")
    ax.set_ylabel("Viabilidad de implementación")
    _save(fig, "15_matriz_criticidad_viabilidad.png")
    chart_index.append({"grafico": "matriz criticidad vs viabilidad", "archivo": "15_matriz_criticidad_viabilidad.png", "insight": "Selecciona cartera equilibrada por impacto y factibilidad."})

    # 16. comparación por turno
    turn = shift.groupby("turno", as_index=False).agg(sec_shift=("sec_shift", "mean"), scrap=("scrap_pct", "mean"), variance=("shift_variance_score", "mean"))
    fig, ax = plt.subplots(figsize=(9, 6))
    ax.plot(turn["turno"], turn["sec_shift"], marker="o", label="SEC")
    ax.plot(turn["turno"], turn["scrap"], marker="o", label="Scrap %")
    ax.plot(turn["turno"], turn["variance"], marker="o", label="Shift variance")
    ax.set_title("Comparación de desempeño por turno")
    ax.set_xlabel("Turno")
    ax.legend()
    _save(fig, "16_comparacion_por_turno.png")
    chart_index.append({"grafico": "comparación por turno", "archivo": "16_comparacion_por_turno.png", "insight": "Identifica turnos con mayor penalización operativa."})

    # 17. benchmark interno y externo
    bench = pd.read_csv(DATA_RAW_DIR / "benchmark_externo_proxy.csv")
    line_bench = line_day.groupby("familia_proceso", as_index=False).agg(sec_interno=("sec", "mean"), oee_interno=("oee_synthetic", "mean"))
    ext = bench.groupby("familia_proceso", as_index=False).agg(sec_externo=("sec_referencia", "mean"), oee_externo=("oee_referencia", "mean"))
    cmp = line_bench.merge(ext, on="familia_proceso", how="left")
    fig, ax = plt.subplots(figsize=(11, 6))
    x = np.arange(len(cmp))
    w = 0.35
    ax.bar(x - w / 2, cmp["sec_interno"], width=w, label="SEC interno")
    ax.bar(x + w / 2, cmp["sec_externo"], width=w, label="SEC externo")
    ax.set_xticks(x)
    ax.set_xticklabels(cmp["familia_proceso"], rotation=45)
    ax.set_title("Benchmark interno vs externo (SEC por familia)")
    ax.legend()
    _save(fig, "17_benchmark_interno_externo.png")
    chart_index.append({"grafico": "benchmark interno y externo", "archivo": "17_benchmark_interno_externo.png", "insight": "Brecha estructural frente a referencia externa."})

    # 18. sensibilidad al precio de energía
    sens = sensitivity[sensitivity["scenario"] != "base"].copy()
    sens_rank = sens.groupby(["scenario", "iniciativa_id"], as_index=False)["ranking"].mean()
    top_sens = sens_rank[sens_rank["iniciativa_id"].isin(invest.head(10)["iniciativa_id"])].pivot(index="iniciativa_id", columns="scenario", values="ranking").fillna(0)
    fig, ax = plt.subplots(figsize=(11, 7))
    im = ax.imshow(top_sens.values, aspect="auto", cmap="coolwarm_r")
    ax.set_title("Sensibilidad del ranking al precio de energía")
    ax.set_xticks(range(len(top_sens.columns)))
    ax.set_xticklabels(top_sens.columns)
    ax.set_yticks(range(len(top_sens.index)))
    ax.set_yticklabels(top_sens.index, fontsize=8)
    fig.colorbar(im, ax=ax, label="Posición ranking")
    _save(fig, "18_sensibilidad_precio_energia.png")
    chart_index.append({"grafico": "sensibilidad al precio de energía", "archivo": "18_sensibilidad_precio_energia.png", "insight": "Robustez del portafolio ante shocks energéticos."})

    pd.DataFrame(chart_index).to_csv(OUTPUT_REPORTS_DIR / "inventario_graficos.csv", index=False)


if __name__ == "__main__":
    generate_charts()
