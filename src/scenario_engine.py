from __future__ import annotations

import duckdb
import numpy as np
import pandas as pd

from .config import DATA_PROCESSED_DIR, DUCKDB_PATH, OUTPUT_REPORTS_DIR


SCENARIO_LIBRARY = {
    "quick_wins_operativos": {
        "label": "Quick Wins Operativos",
        "reduction_consumption": 0.045,
        "sec_improvement": 0.050,
        "oee_improvement": 0.015,
        "stops_reduction": 0.060,
        "emission_reduction": 0.040,
        "execution_ease": 88,
        "ramp_months": 3,
        "start_delay_months": 1,
    },
    "mantenimiento_restauracion": {
        "label": "Mantenimiento y Restauración de Performance",
        "reduction_consumption": 0.025,
        "sec_improvement": 0.030,
        "oee_improvement": 0.028,
        "stops_reduction": 0.180,
        "emission_reduction": 0.022,
        "execution_ease": 67,
        "ramp_months": 5,
        "start_delay_months": 2,
    },
    "optimizacion_parametros": {
        "label": "Optimización de Parámetros de Proceso",
        "reduction_consumption": 0.065,
        "sec_improvement": 0.070,
        "oee_improvement": 0.035,
        "stops_reduction": 0.110,
        "emission_reduction": 0.060,
        "execution_ease": 58,
        "ramp_months": 6,
        "start_delay_months": 2,
    },
    "reduccion_scrap_reproceso": {
        "label": "Reducción de Scrap/Reproceso",
        "reduction_consumption": 0.020,
        "sec_improvement": 0.025,
        "oee_improvement": 0.030,
        "stops_reduction": 0.040,
        "emission_reduction": 0.018,
        "execution_ease": 72,
        "ramp_months": 4,
        "start_delay_months": 1,
    },
    "electrificacion_sustitucion": {
        "label": "Electrificación o Sustitución de Equipos",
        "reduction_consumption": 0.090,
        "sec_improvement": 0.085,
        "oee_improvement": 0.025,
        "stops_reduction": 0.080,
        "emission_reduction": 0.120,
        "execution_ease": 42,
        "ramp_months": 9,
        "start_delay_months": 4,
    },
    "recuperacion_energetica": {
        "label": "Recuperación Energética",
        "reduction_consumption": 0.110,
        "sec_improvement": 0.100,
        "oee_improvement": 0.018,
        "stops_reduction": 0.030,
        "emission_reduction": 0.095,
        "execution_ease": 54,
        "ramp_months": 8,
        "start_delay_months": 3,
    },
    "capex_mayor": {
        "label": "CAPEX Mayor",
        "reduction_consumption": 0.160,
        "sec_improvement": 0.145,
        "oee_improvement": 0.060,
        "stops_reduction": 0.210,
        "emission_reduction": 0.170,
        "execution_ease": 30,
        "ramp_months": 14,
        "start_delay_months": 6,
    },
}

RISK_FACTOR = {
    "Bajo": 0.92,
    "Medio": 0.78,
    "Alto": 0.60,
}

# Supuestos financieros explícitos: screening/pre-feasibility (no committee-grade).
ANALYSIS_HORIZON_YEARS = 6
DISCOUNT_RATE = 0.10
BASE_CARBON_COST_EUR_TON = 90.0

MACRO_SENSITIVITY = {
    "Base": {
        "energy_price_mult": 1.00,
        "carbon_cost_mult": 1.00,
        "margin_operativo_mult": 1.00,
        "delay_mult": 1.00,
        "capex_overrun_pct": 0.00,
        "capture_shortfall_pct": 0.00,
    },
    "ShockEnergia": {
        "energy_price_mult": 1.24,
        "carbon_cost_mult": 1.05,
        "margin_operativo_mult": 0.98,
        "delay_mult": 1.08,
        "capex_overrun_pct": 0.03,
        "capture_shortfall_pct": 0.04,
    },
    "AltaDemanda": {
        "energy_price_mult": 1.10,
        "carbon_cost_mult": 1.00,
        "margin_operativo_mult": 1.12,
        "delay_mult": 1.15,
        "capex_overrun_pct": 0.05,
        "capture_shortfall_pct": 0.06,
    },
    "RestriccionOperativa": {
        "energy_price_mult": 1.06,
        "carbon_cost_mult": 1.00,
        "margin_operativo_mult": 0.95,
        "delay_mult": 1.32,
        "capex_overrun_pct": 0.08,
        "capture_shortfall_pct": 0.12,
    },
}


def _safe_ratio(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator else 0.0


def _scenario_multiplier(shock_row: pd.Series) -> float:
    return (
        0.50 * float(shock_row["precio_energia_shock"])
        + 0.30 * float(shock_row["presion_demanda_indice"])
        + 0.20 * float(shock_row["restriccion_operativa_indice"])
    )


def _macro_sensitivity(macro_scenario: str) -> dict[str, float]:
    return MACRO_SENSITIVITY.get(str(macro_scenario), MACRO_SENSITIVITY["Base"])


def _map_scenario(row: pd.Series) -> str:
    tipo = str(row["tipo_iniciativa"])
    categoria = str(row["categoria_iniciativa"])
    capex = float(row["capex_estimado"])

    if int(row["quick_win_flag"]) == 1:
        return "quick_wins_operativos"
    if "Mantenimiento" in tipo:
        return "mantenimiento_restauracion"
    if "Optim" in tipo or "Proceso" in categoria:
        return "optimizacion_parametros"
    if "Scrap" in tipo:
        return "reduccion_scrap_reproceso"
    if "Electr" in tipo:
        return "electrificacion_sustitucion"
    if "Recuperacion" in tipo or "Energet" in categoria:
        return "recuperacion_energetica" if capex <= 700_000 else "electrificacion_sustitucion"
    if capex >= 900_000:
        return "capex_mayor"
    return "reduccion_scrap_reproceso"


def _build_dependency_and_conflict_tables(initiatives: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    dep_rows: list[dict[str, object]] = []
    conf_rows: list[dict[str, object]] = []

    for (_, linea_id), group in initiatives.groupby(["planta_id", "linea_id"]):
        group = group.sort_values("capex_estimado", ascending=False).reset_index(drop=True)

        capex_ids = group[group["capex_estimado"] >= 700_000]["iniciativa_id"].tolist()
        pre_candidates = group[
            group["tipo_iniciativa"].str.contains("Mantenimiento|Optim", case=False, na=False)
        ]["iniciativa_id"].tolist()

        for capex_id in capex_ids:
            if pre_candidates:
                dep_rows.append(
                    {
                        "iniciativa_id": capex_id,
                        "depends_on_iniciativa_id": pre_candidates[0],
                        "dependency_strength": "alta",
                        "dependency_penalty_pct": 12.0,
                        "linea_id": linea_id,
                    }
                )

        pairs = group[["iniciativa_id", "categoria_iniciativa", "quick_win_flag", "capex_estimado"]].to_dict("records")
        for i in range(len(pairs)):
            for j in range(i + 1, len(pairs)):
                p1 = pairs[i]
                p2 = pairs[j]
                same_category = p1["categoria_iniciativa"] == p2["categoria_iniciativa"]
                close_capex = abs(float(p1["capex_estimado"]) - float(p2["capex_estimado"])) <= 100_000
                both_qw = int(p1["quick_win_flag"]) == 1 and int(p2["quick_win_flag"]) == 1
                if same_category and (close_capex or both_qw):
                    conf_rows.append(
                        {
                            "iniciativa_id_1": p1["iniciativa_id"],
                            "iniciativa_id_2": p2["iniciativa_id"],
                            "linea_id": linea_id,
                            "conflict_type": "solape_recursos",
                            "conflict_penalty_pct": 8.0 if both_qw else 12.0,
                        }
                    )

    deps = pd.DataFrame(dep_rows)
    conflicts = pd.DataFrame(conf_rows)
    if deps.empty:
        deps = pd.DataFrame(columns=["iniciativa_id", "depends_on_iniciativa_id", "dependency_strength", "dependency_penalty_pct", "linea_id"])
    if conflicts.empty:
        conflicts = pd.DataFrame(columns=["iniciativa_id_1", "iniciativa_id_2", "linea_id", "conflict_type", "conflict_penalty_pct"])
    return deps, conflicts


def _monthly_capture(month_idx: int, start_delay: int, ramp_months: int) -> float:
    if month_idx < start_delay:
        return 0.0
    elapsed = month_idx - start_delay + 1
    if elapsed >= ramp_months:
        return 1.0
    return elapsed / max(ramp_months, 1)


def _irr_candidate_from_annual_cashflows(cashflows: list[float]) -> float:
    # Cálculo aproximado por bisección para pre-feasibility (no IRR formal de comité).
    if not cashflows or all(v <= 0 for v in cashflows) or all(v >= 0 for v in cashflows):
        return float("nan")

    def npv(rate: float) -> float:
        return sum(cf / ((1 + rate) ** t) for t, cf in enumerate(cashflows))

    low = -0.90
    high = 3.00
    npv_low = npv(low)
    npv_high = npv(high)
    if npv_low * npv_high > 0:
        return float("nan")

    for _ in range(120):
        mid = (low + high) / 2
        npv_mid = npv(mid)
        if abs(npv_mid) < 1e-7:
            return 100.0 * mid
        if npv_low * npv_mid <= 0:
            high = mid
            npv_high = npv_mid
        else:
            low = mid
            npv_low = npv_mid

    return 100.0 * ((low + high) / 2)


def _financials(
    capex_effective: float,
    gross_technical_value_annual: float,
    implementation_opex_annual: float,
    expected_capture_rate: float,
    downside_capture_rate: float,
    start_delay_months: int,
    ramp_months: int,
    discount_rate: float,
) -> dict[str, float]:
    gross_technical_value_annual = max(0.0, gross_technical_value_annual)
    implementation_opex_annual = max(0.0, implementation_opex_annual)

    net_operational_value_annual = max(0.0, gross_technical_value_annual - implementation_opex_annual)
    net_captured_value_annual = max(0.0, gross_technical_value_annual * expected_capture_rate - implementation_opex_annual)
    downside_adjusted_annual = max(0.0, gross_technical_value_annual * downside_capture_rate - implementation_opex_annual)

    monthly_expected = net_captured_value_annual / 12.0
    monthly_downside = downside_adjusted_annual / 12.0
    monthly_operational = net_operational_value_annual / 12.0

    npv_base = -capex_effective
    discounted_value_npv = -capex_effective
    downside_adjusted_value_npv = -capex_effective
    discounted_cumulative = -capex_effective
    discounted_payback_months = 999.0

    monthly_expected_flows: list[float] = []

    for month in range(ANALYSIS_HORIZON_YEARS * 12):
        capture = _monthly_capture(month, start_delay_months, ramp_months)
        flow_operational = monthly_operational * capture
        flow_expected = monthly_expected * capture
        flow_downside = monthly_downside * capture
        disc = (1 + discount_rate) ** ((month + 1) / 12.0)

        npv_base += flow_operational / disc
        discounted_value_npv += flow_expected / disc
        downside_adjusted_value_npv += flow_downside / disc
        discounted_cumulative += flow_expected / disc
        monthly_expected_flows.append(flow_expected)

        if discounted_payback_months == 999.0 and discounted_cumulative >= 0:
            discounted_payback_months = float(month + 1)

    npv_delayed = -capex_effective
    for month in range(ANALYSIS_HORIZON_YEARS * 12):
        capture = _monthly_capture(month, start_delay_months + 12, ramp_months)
        flow = monthly_expected * capture
        disc = (1 + discount_rate) ** ((month + 1) / 12.0)
        npv_delayed += flow / disc

    first_year_capture = sum(monthly_expected_flows[:12]) if monthly_expected_flows else 0.0
    ramp_up_penalty_value = max(0.0, net_captured_value_annual - first_year_capture)

    annual_cashflows = [-capex_effective]
    for year in range(ANALYSIS_HORIZON_YEARS):
        start = year * 12
        end = start + 12
        annual_cashflows.append(sum(monthly_expected_flows[start:end]))

    formal_irr_candidate_pct = _irr_candidate_from_annual_cashflows(annual_cashflows)
    formal_irr_candidate_flag = int(pd.notna(formal_irr_candidate_pct) and capex_effective >= 250_000)

    screening_irr_pct = 0.0 if capex_effective <= 0 else min(250.0, 100.0 * _safe_ratio(net_captured_value_annual, capex_effective))

    screening_var_95_annual = max(0.0, net_captured_value_annual - downside_adjusted_annual)
    screening_var_95_npv = max(0.0, discounted_value_npv - downside_adjusted_value_npv)
    robust_risk_metric_candidate_npv = 1.25 * screening_var_95_npv

    capital_efficiency = _safe_ratio(downside_adjusted_value_npv, capex_effective)
    cost_of_delay_12m = max(0.0, discounted_value_npv - npv_delayed)

    return {
        "gross_technical_value_annual": float(gross_technical_value_annual),
        "net_operational_value_annual": float(net_operational_value_annual),
        "net_captured_value_annual": float(net_captured_value_annual),
        "downside_adjusted_annual": float(downside_adjusted_annual),
        "discounted_value_npv": float(discounted_value_npv),
        "downside_adjusted_value_npv": float(downside_adjusted_value_npv),
        "cost_of_delay_12m": float(cost_of_delay_12m),
        "capital_efficiency": float(capital_efficiency),
        "screening_irr_pct": float(screening_irr_pct),
        "formal_irr_candidate_pct": float(formal_irr_candidate_pct) if pd.notna(formal_irr_candidate_pct) else np.nan,
        "formal_irr_candidate_flag": float(formal_irr_candidate_flag),
        "discounted_payback_months": float(discounted_payback_months),
        "screening_var_95_annual": float(screening_var_95_annual),
        "screening_var_95_npv": float(screening_var_95_npv),
        "robust_risk_metric_candidate_npv": float(robust_risk_metric_candidate_npv),
        "ramp_up_penalty_value": float(ramp_up_penalty_value),
        # Alias legacy para compatibilidad aguas abajo
        "npv_base": float(npv_base),
        "npv_risk_adjusted": float(discounted_value_npv),
        "irr_proxy_pct": float(screening_irr_pct),
        "downside_annual_savings_p20": float(downside_adjusted_annual),
        "value_at_risk_95": float(screening_var_95_annual),
    }


def _classify_financial_stage(capex_effective: float, formal_candidate_flag: int, npv: float, var_npv: float) -> str:
    if capex_effective >= 900_000 and formal_candidate_flag == 1 and npv > 0 and var_npv <= max(npv, 1.0) * 0.65:
        return "business_case_candidate"
    if capex_effective >= 250_000:
        return "pre_feasibility"
    return "screening"


def run_scenario_engine() -> dict[str, pd.DataFrame]:
    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DUCKDB_PATH))

    baseline_line = con.execute(
        """
        SELECT
            m.planta_id,
            m.linea_id,
            AVG(m.sec_kwh_ton) AS sec_base,
            AVG(m.oee_synthetic) AS oee_base,
            SUM(m.cost_energy_total) AS cost_energy_base,
            SUM(m.emissions_kg) AS emissions_base_kg,
            SUM(m.loss_value_proxy_eur) AS loss_base_eur,
            SUM(m.n_paradas) AS stops_base,
            AVG(l.criticidad_linea) AS criticidad_linea
        FROM mart_line_day m
        JOIN stg_lines l ON m.linea_id = l.linea_id
        GROUP BY 1,2
        """
    ).fetchdf()

    initiatives = con.execute(
        """
        SELECT
            i.iniciativa_id,
            i.planta_id,
            i.linea_id,
            i.categoria_iniciativa,
            i.tipo_iniciativa,
            i.quick_win_flag,
            i.impacto_esperado_energia_pct,
            i.impacto_esperado_oee_pct,
            i.impacto_esperado_emisiones_pct,
            i.dificultad_implementacion,
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

    shocks = con.execute(
        """
        SELECT
            escenario,
            AVG(precio_energia_shock) AS precio_energia_shock,
            AVG(presion_demanda_indice) AS presion_demanda_indice,
            AVG(disponibilidad_personal_indice) AS disponibilidad_personal_indice,
            AVG(restriccion_operativa_indice) AS restriccion_operativa_indice
        FROM stg_scenarios
        GROUP BY 1
        """
    ).fetchdf()

    con.close()

    initiatives["scenario_key"] = initiatives.apply(_map_scenario, axis=1)
    initiatives = initiatives.merge(
        baseline_line[["planta_id", "linea_id", "criticidad_linea"]],
        on=["planta_id", "linea_id"],
        how="left",
    )

    dependencies, conflicts = _build_dependency_and_conflict_tables(initiatives)
    dep_penalty = dependencies.groupby("iniciativa_id", as_index=False)["dependency_penalty_pct"].max()

    conf_a = conflicts[["iniciativa_id_1", "conflict_penalty_pct"]].rename(columns={"iniciativa_id_1": "iniciativa_id"})
    conf_b = conflicts[["iniciativa_id_2", "conflict_penalty_pct"]].rename(columns={"iniciativa_id_2": "iniciativa_id"})
    conf_penalty = pd.concat([conf_a, conf_b], ignore_index=True).groupby("iniciativa_id", as_index=False)["conflict_penalty_pct"].sum()

    records: list[dict[str, float | str | int]] = []

    for row in initiatives.itertuples(index=False):
        base_line = baseline_line[(baseline_line["planta_id"] == row.planta_id) & (baseline_line["linea_id"] == row.linea_id)]
        if base_line.empty:
            continue
        b = base_line.iloc[0]
        lib = SCENARIO_LIBRARY[row.scenario_key]

        dep_pct = float(
            dep_penalty.loc[dep_penalty["iniciativa_id"] == row.iniciativa_id, "dependency_penalty_pct"].max()
        ) if row.iniciativa_id in dep_penalty["iniciativa_id"].values else 0.0
        conf_pct = float(
            conf_penalty.loc[conf_penalty["iniciativa_id"] == row.iniciativa_id, "conflict_penalty_pct"].max()
        ) if row.iniciativa_id in conf_penalty["iniciativa_id"].values else 0.0
        conflict_adjustment = min(0.25, conf_pct / 100.0)
        dependency_adjustment = min(0.20, dep_pct / 100.0)

        for shock in shocks.itertuples(index=False):
            macro_sens = _macro_sensitivity(str(shock.escenario))
            impact_factor = (1 + 0.08 * (float(row.criticidad_linea) - 3)) * _scenario_multiplier(pd.Series(shock._asdict()))

            reduction_consumption_pct = np.clip(lib["reduction_consumption"] * impact_factor, 0.0, 0.50)
            sec_improvement_pct = np.clip(lib["sec_improvement"] * impact_factor, 0.0, 0.40)
            oee_improvement_pct = np.clip(lib["oee_improvement"] * (1 + float(row.impacto_esperado_oee_pct) / 100.0), 0.0, 0.35)
            stops_reduction_pct = np.clip(lib["stops_reduction"] * impact_factor, 0.0, 0.55)
            emission_reduction_pct = np.clip(lib["emission_reduction"] * impact_factor, 0.0, 0.55)

            energy_price_mult = float(shock.precio_energia_shock) * macro_sens["energy_price_mult"]
            carbon_cost = BASE_CARBON_COST_EUR_TON * macro_sens["carbon_cost_mult"]
            margin_mult = macro_sens["margin_operativo_mult"]
            capex_overrun_pct = macro_sens["capex_overrun_pct"]
            capture_shortfall_pct = macro_sens["capture_shortfall_pct"]

            baseline_energy_exposure = max(0.0, b["cost_energy_base"] * energy_price_mult)
            baseline_non_energy_exposure = max(0.0, (b["loss_base_eur"] - b["cost_energy_base"]) * margin_mult)
            baseline_emission_cost = max(0.0, (b["emissions_base_kg"] / 1000.0) * carbon_cost)
            baseline_economic_exposure = baseline_energy_exposure + baseline_non_energy_exposure + baseline_emission_cost

            gross_technical_value_energy = baseline_energy_exposure * reduction_consumption_pct
            avoid_factor = np.clip(0.48 * stops_reduction_pct + 0.32 * oee_improvement_pct + 0.20 * sec_improvement_pct, 0.0, 0.72)
            avoided_loss = baseline_non_energy_exposure * avoid_factor
            gross_technical_value_emissions = baseline_emission_cost * emission_reduction_pct
            gross_technical_value_total = max(0.0, gross_technical_value_energy + avoided_loss + gross_technical_value_emissions)

            implementation_opex = float(row.opex_impacto_proxy) if pd.notna(row.opex_impacto_proxy) else 0.0
            implementation_opex = max(0.0, implementation_opex * (0.90 + 0.10 * float(shock.presion_demanda_indice)))

            execution_ease = float(lib["execution_ease"]) / 100.0
            risk_factor = RISK_FACTOR.get(str(row.riesgo_implantacion), 0.72)
            staff_factor = float(shock.disponibilidad_personal_indice)
            raw_success_probability = np.clip(0.22 + 0.46 * execution_ease + 0.20 * risk_factor + 0.12 * staff_factor, 0.20, 0.95)
            success_probability = np.clip(raw_success_probability * (1 - capture_shortfall_pct), 0.15, 0.93)

            expected_capture_rate = np.clip(
                success_probability * (1 - dependency_adjustment) * (1 - conflict_adjustment),
                0.10,
                0.92,
            )
            downside_capture_rate = np.clip(expected_capture_rate * 0.72, 0.05, expected_capture_rate)

            ramp_months = int(
                round(
                    lib["ramp_months"]
                    * macro_sens["delay_mult"]
                    * (1 + max(0.0, float(shock.restriccion_operativa_indice) - 1.0))
                )
            )
            start_delay = int(
                round(
                    lib["start_delay_months"]
                    * macro_sens["delay_mult"]
                    * (1 + max(0.0, 1.0 - float(shock.disponibilidad_personal_indice)))
                )
            )

            capex_base = float(row.capex_estimado) if pd.notna(row.capex_estimado) else 0.0
            capex_effective = max(0.0, capex_base * (1 + capex_overrun_pct))

            fin = _financials(
                capex_effective=capex_effective,
                gross_technical_value_annual=gross_technical_value_total,
                implementation_opex_annual=implementation_opex,
                expected_capture_rate=float(expected_capture_rate),
                downside_capture_rate=float(downside_capture_rate),
                start_delay_months=max(start_delay, 0),
                ramp_months=max(ramp_months, 1),
                discount_rate=DISCOUNT_RATE,
            )

            financial_maturity_stage = _classify_financial_stage(
                capex_effective=capex_effective,
                formal_candidate_flag=int(fin["formal_irr_candidate_flag"]),
                npv=float(fin["discounted_value_npv"]),
                var_npv=float(fin["screening_var_95_npv"]),
            )
            if financial_maturity_stage == "business_case_candidate":
                financial_use_label = "business_case_candidate_not_final"
            elif financial_maturity_stage == "pre_feasibility":
                financial_use_label = "pre_feasibility"
            else:
                financial_use_label = "screening_financiero"

            committee_wording_status = (
                "candidate_with_caveat"
                if financial_maturity_stage == "business_case_candidate"
                else "not_committee_grade_proxy"
            )

            implementation_burden = np.clip(
                100
                * (
                    0.35 * np.log1p(max(capex_effective, 0.0)) / np.log1p(2_200_000)
                    + 0.25 * (float(row.dificultad_implementacion) / 5.0)
                    + 0.20 * (ramp_months / 18.0)
                    + 0.20 * (1 - execution_ease)
                ),
                0,
                100,
            )
            strategic_relevance = np.clip(
                100
                * (
                    0.40 * _safe_ratio(float(row.criticidad_estrategica), 5.0)
                    + 0.35 * _safe_ratio(float(row.impacto_esperado_emisiones_pct), 20.0)
                    + 0.25 * _safe_ratio(float(row.criticidad_linea), 5.0)
                ),
                0,
                100,
            )

            records.append(
                {
                    "iniciativa_id": row.iniciativa_id,
                    "planta_id": row.planta_id,
                    "linea_id": row.linea_id,
                    "macro_scenario": shock.escenario,
                    "scenario_key": row.scenario_key,
                    "escenario_label": lib["label"],
                    "reduccion_consumo_pct": 100 * reduction_consumption_pct,
                    "mejora_sec_pct": 100 * sec_improvement_pct,
                    "mejora_oee_pct": 100 * oee_improvement_pct,
                    "reduccion_paradas_pct": 100 * stops_reduction_pct,
                    "reduccion_emisiones_pct": 100 * emission_reduction_pct,
                    "baseline_energy_exposure": baseline_energy_exposure,
                    "baseline_non_energy_exposure": baseline_non_energy_exposure,
                    "baseline_emission_cost": baseline_emission_cost,
                    "baseline_economic_exposure": baseline_economic_exposure,
                    "gross_technical_value_energy": gross_technical_value_energy,
                    "avoided_loss": avoided_loss,
                    "gross_technical_value_emissions": gross_technical_value_emissions,
                    "gross_technical_value": fin["gross_technical_value_annual"],
                    "implementation_opex": implementation_opex,
                    "net_operational_value": fin["net_operational_value_annual"],
                    "net_captured_value": fin["net_captured_value_annual"],
                    "downside_adjusted_annual": fin["downside_adjusted_annual"],
                    "discounted_value": fin["discounted_value_npv"],
                    "downside_adjusted_value": fin["downside_adjusted_value_npv"],
                    "cost_of_delay_12m": fin["cost_of_delay_12m"],
                    "capital_efficiency": fin["capital_efficiency"],
                    "screening_irr_pct": fin["screening_irr_pct"],
                    "formal_irr_candidate_pct": fin["formal_irr_candidate_pct"],
                    "formal_irr_candidate_flag": fin["formal_irr_candidate_flag"],
                    "screening_var_95_npv": fin["screening_var_95_npv"],
                    "robust_risk_metric_candidate_npv": fin["robust_risk_metric_candidate_npv"],
                    "capex_estimado": capex_effective,
                    "capex_base": capex_base,
                    "capex_overrun_pct": capex_overrun_pct,
                    "success_probability": float(success_probability),
                    "expected_capture_rate": float(expected_capture_rate),
                    "downside_capture_rate": float(downside_capture_rate),
                    "capture_shortfall_pct": capture_shortfall_pct,
                    "start_delay_months": float(start_delay),
                    "ramp_up_months": float(ramp_months),
                    "ramp_up_penalty_value": fin["ramp_up_penalty_value"],
                    "dependency_penalty_pct": dep_pct,
                    "conflict_penalty_pct": conf_pct,
                    "npv_base": fin["npv_base"],
                    "npv_risk_adjusted": fin["npv_risk_adjusted"],
                    "irr_proxy_pct": fin["irr_proxy_pct"],
                    "payback_meses": fin["discounted_payback_months"],
                    "downside_annual_savings_p20": fin["downside_annual_savings_p20"],
                    "value_at_risk_95": fin["value_at_risk_95"],
                    "implementation_burden": float(implementation_burden),
                    "strategic_relevance_score": float(strategic_relevance),
                    "facilidad_ejecucion": lib["execution_ease"],
                    "financial_maturity_stage": financial_maturity_stage,
                    "financial_use_label": financial_use_label,
                    "committee_wording_status": committee_wording_status,
                    "assumption_energy_price_mult": energy_price_mult,
                    "assumption_carbon_cost_eur_ton": carbon_cost,
                    "assumption_margin_operativo_mult": margin_mult,
                }
            )

    scenario_table = pd.DataFrame(records)
    scenario_table = scenario_table.replace([np.inf, -np.inf], np.nan).fillna(0)

    # Alias legacy para compatibilidad con capas ya existentes
    scenario_table["gross_annual_savings"] = scenario_table["gross_technical_value"]
    scenario_table["net_annual_savings"] = scenario_table["net_operational_value"]
    scenario_table["risk_adjusted_annual_savings"] = scenario_table["net_captured_value"]

    base_only = scenario_table[scenario_table["macro_scenario"].str.lower() == "base"].copy()
    if base_only.empty:
        base_only = scenario_table.copy()

    per_line = base_only.sort_values("net_captured_value", ascending=False).groupby(["planta_id", "linea_id"], as_index=False).first()
    baseline_snapshot = baseline_line.merge(
        per_line[
            [
                "planta_id",
                "linea_id",
                "reduccion_consumo_pct",
                "mejora_sec_pct",
                "mejora_oee_pct",
                "reduccion_paradas_pct",
                "reduccion_emisiones_pct",
                "net_captured_value",
                "downside_adjusted_annual",
                "cost_of_delay_12m",
            ]
        ],
        on=["planta_id", "linea_id"],
        how="left",
    )

    baseline_snapshot["sec_mejorado"] = baseline_snapshot["sec_base"] * (1 - baseline_snapshot["mejora_sec_pct"].fillna(0) / 100.0)
    baseline_snapshot["oee_mejorado"] = baseline_snapshot["oee_base"] * (1 + baseline_snapshot["mejora_oee_pct"].fillna(0) / 100.0)
    baseline_snapshot["coste_energia_mejorado"] = baseline_snapshot["cost_energy_base"] * (
        1 - baseline_snapshot["reduccion_consumo_pct"].fillna(0) / 100.0
    )
    baseline_snapshot["emisiones_mejoradas_kg"] = baseline_snapshot["emissions_base_kg"] * (
        1 - baseline_snapshot["reduccion_emisiones_pct"].fillna(0) / 100.0
    )

    initiative_comparison = (
        scenario_table.groupby(["iniciativa_id", "planta_id", "linea_id", "scenario_key", "escenario_label", "capex_estimado"], as_index=False)
        .agg(
            gross_technical_value_mean=("gross_technical_value", "mean"),
            net_captured_value_mean=("net_captured_value", "mean"),
            downside_adjusted_annual_mean=("downside_adjusted_annual", "mean"),
            discounted_value_mean=("discounted_value", "mean"),
            downside_adjusted_value_mean=("downside_adjusted_value", "mean"),
            cost_of_delay_12m_mean=("cost_of_delay_12m", "mean"),
            capital_efficiency_mean=("capital_efficiency", "mean"),
            screening_var_95_npv_mean=("screening_var_95_npv", "mean"),
            discounted_payback_meses_mean=("payback_meses", "mean"),
            implementation_burden=("implementation_burden", "mean"),
            strategic_relevance_score=("strategic_relevance_score", "mean"),
            success_probability=("success_probability", "mean"),
            reduccion_emisiones_pct=("reduccion_emisiones_pct", "mean"),
            risk_adjusted_annual_savings_mean=("risk_adjusted_annual_savings", "mean"),
            npv_risk_adjusted_mean=("npv_risk_adjusted", "mean"),
            payback_meses=("payback_meses", "mean"),
            value_at_risk_95=("value_at_risk_95", "mean"),
        )
    )

    max_downside_npv = initiative_comparison["downside_adjusted_value_mean"].max() or 1.0
    max_var = initiative_comparison["screening_var_95_npv_mean"].max() or 1.0
    max_burden = initiative_comparison["implementation_burden"].max() or 1.0

    initiative_comparison["score_tradeoff"] = 100 * (
        0.33 * (initiative_comparison["downside_adjusted_value_mean"] / max_downside_npv)
        + 0.15 * (1 - initiative_comparison["discounted_payback_meses_mean"] / initiative_comparison["discounted_payback_meses_mean"].max())
        + 0.15 * (initiative_comparison["strategic_relevance_score"] / 100.0)
        + 0.12 * initiative_comparison["success_probability"]
        + 0.10 * (initiative_comparison["reduccion_emisiones_pct"] / initiative_comparison["reduccion_emisiones_pct"].max())
        + 0.10 * (initiative_comparison["capital_efficiency_mean"] / max(initiative_comparison["capital_efficiency_mean"].max(), 1e-6))
        + 0.05 * (1 - initiative_comparison["implementation_burden"] / max_burden)
        - 0.12 * (initiative_comparison["screening_var_95_npv_mean"] / max_var)
    )
    scenario_ranking = initiative_comparison.sort_values("score_tradeoff", ascending=False).reset_index(drop=True)
    scenario_ranking["ranking"] = scenario_ranking.index + 1

    waterfall = (
        base_only.groupby("escenario_label", as_index=False)
        .agg(
            ahorro_bruto=("gross_technical_value", "sum"),
            ahorro_neto=("net_captured_value", "sum"),
            ahorro_ajustado_riesgo=("downside_adjusted_annual", "sum"),
            var_95=("value_at_risk_95", "sum"),
            valor_descontado=("discounted_value", "sum"),
            valor_downside=("downside_adjusted_value", "sum"),
            coste_retraso_12m=("cost_of_delay_12m", "sum"),
        )
        .sort_values("ahorro_ajustado_riesgo", ascending=False)
    )

    financial_assumptions = pd.DataFrame(
        [
            {
                "assumption_id": "base_carbon_cost",
                "description": "Coste de carbono de screening/pre-feasibility",
                "value": BASE_CARBON_COST_EUR_TON,
                "unit": "EUR/tCO2e",
                "maturity": "proxy",
            },
            {
                "assumption_id": "discount_rate",
                "description": "Tasa de descuento para discounted value de screening",
                "value": DISCOUNT_RATE,
                "unit": "ratio",
                "maturity": "proxy",
            },
            {
                "assumption_id": "analysis_horizon_years",
                "description": "Horizonte de evaluación financiera",
                "value": ANALYSIS_HORIZON_YEARS,
                "unit": "years",
                "maturity": "defendible",
            },
        ]
    )

    sens_rows: list[dict[str, object]] = []
    for k, vals in MACRO_SENSITIVITY.items():
        sens_rows.append(
            {
                "macro_scenario": k,
                "energy_price_mult": vals["energy_price_mult"],
                "carbon_cost_mult": vals["carbon_cost_mult"],
                "margin_operativo_mult": vals["margin_operativo_mult"],
                "delay_mult": vals["delay_mult"],
                "capex_overrun_pct": vals["capex_overrun_pct"],
                "capture_shortfall_pct": vals["capture_shortfall_pct"],
            }
        )
    scenario_sensitivity_table = pd.DataFrame(sens_rows)

    financial_logic_audit = pd.DataFrame(
        [
            {
                "component": "Ahorro energético anual",
                "classification": "defendible",
                "maturity": "screening/pre-feasibility",
                "committee_suitability": "usable_with_caveat",
                "rationale": "Deriva de exposición energética baseline * reducción técnica * sensibilidad explícita de precio.",
            },
            {
                "component": "Gross technical value",
                "classification": "defendible",
                "maturity": "screening/pre-feasibility",
                "committee_suitability": "usable_with_caveat",
                "rationale": "Separación explícita energía/avoidance/emisiones con guardas anti doble conteo.",
            },
            {
                "component": "Discounted value / downside-adjusted value",
                "classification": "proxy útil",
                "maturity": "pre-feasibility",
                "committee_suitability": "not_final_business_case",
                "rationale": "Depende de curvas de captura sintéticas y tasa única de descuento.",
            },
            {
                "component": "Screening IRR",
                "classification": "proxy útil",
                "maturity": "screening",
                "committee_suitability": "not_formal_irr",
                "rationale": "No equivale a IRR corporativa auditada; se usa solo para priorización temprana.",
            },
            {
                "component": "Formal IRR candidate",
                "classification": "débil",
                "maturity": "pre-feasibility",
                "committee_suitability": "candidate_only",
                "rationale": "Bisección sobre flujos sintéticos; requiere cashflow real para comité.",
            },
            {
                "component": "Screening VaR / robust risk candidate",
                "classification": "débil",
                "maturity": "screening",
                "committee_suitability": "not_committee_grade",
                "rationale": "No usa distribución histórica calibrada ni correlaciones reales.",
            },
            {
                "component": "Cost of delay",
                "classification": "proxy útil",
                "maturity": "pre-feasibility",
                "committee_suitability": "usable_with_caveat",
                "rationale": "Calculado por desplazamiento +12 meses de captura descontada.",
            },
            {
                "component": "Business case final",
                "classification": "no apta para comité",
                "maturity": "out_of_scope",
                "committee_suitability": "no",
                "rationale": "El motor actual no sustituye FID con supuestos auditados ERP/controlling.",
            },
        ]
    )

    before_after = pd.DataFrame(
        [
            {
                "metric_before": "gross_annual_savings",
                "metric_after": "gross_technical_value",
                "semantic_change": "Se explicita composición energía + avoided_loss + emisiones.",
                "decision_use": "screening/pre-feasibility",
            },
            {
                "metric_before": "net_annual_savings",
                "metric_after": "net_captured_value",
                "semantic_change": "Ahora incorpora tasa de captura esperada y penalizaciones dependencia/conflicto.",
                "decision_use": "priorización",
            },
            {
                "metric_before": "npv_risk_adjusted",
                "metric_after": "discounted_value",
                "semantic_change": "Mantiene estructura NPV, pero separa explícitamente downside-adjusted.",
                "decision_use": "pre-feasibility",
            },
            {
                "metric_before": "value_at_risk_95",
                "metric_after": "screening_var_95_npv",
                "semantic_change": "Se añade versión NPV y robust risk candidate para sensibilidad.",
                "decision_use": "screening de riesgo",
            },
            {
                "metric_before": "irr_proxy_pct",
                "metric_after": "screening_irr_pct + formal_irr_candidate_pct",
                "semantic_change": "Se separa métrica de screening vs candidato pre-feasibility.",
                "decision_use": "screening / gating de business case",
            },
        ]
    )

    scenario_table.to_csv(DATA_PROCESSED_DIR / "scenario_table.csv", index=False)
    baseline_snapshot.to_csv(DATA_PROCESSED_DIR / "scenario_base_vs_improved.csv", index=False)
    scenario_ranking.to_csv(DATA_PROCESSED_DIR / "scenario_ranking.csv", index=False)
    scenario_ranking.to_csv(DATA_PROCESSED_DIR / "scenario_initiative_comparison.csv", index=False)
    waterfall.to_csv(DATA_PROCESSED_DIR / "scenario_waterfall_value.csv", index=False)
    dependencies.to_csv(DATA_PROCESSED_DIR / "scenario_dependencies.csv", index=False)
    conflicts.to_csv(DATA_PROCESSED_DIR / "scenario_conflicts.csv", index=False)
    financial_assumptions.to_csv(DATA_PROCESSED_DIR / "financial_assumptions_table.csv", index=False)
    scenario_sensitivity_table.to_csv(DATA_PROCESSED_DIR / "scenario_financial_sensitivity.csv", index=False)
    financial_logic_audit.to_csv(DATA_PROCESSED_DIR / "financial_logic_audit_table.csv", index=False)
    before_after.to_csv(DATA_PROCESSED_DIR / "financial_metrics_before_after.csv", index=False)

    return {
        "scenario_table": scenario_table,
        "baseline_vs_improved": baseline_snapshot,
        "scenario_ranking": scenario_ranking,
        "waterfall": waterfall,
        "dependencies": dependencies,
        "conflicts": conflicts,
        "financial_assumptions": financial_assumptions,
        "financial_logic_audit": financial_logic_audit,
        "before_after": before_after,
    }


if __name__ == "__main__":
    run_scenario_engine()
