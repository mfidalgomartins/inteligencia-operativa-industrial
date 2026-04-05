from __future__ import annotations

import numpy as np
import pandas as pd

from .config import SOURCE_EMISSION_FACTOR, SOURCE_PRICE_MULTIPLIER
from .context import build_hourly_shift_frame


def _turno_from_hour(hour: int) -> str:
    if 6 <= hour < 14:
        return "A"
    if 14 <= hour < 22:
        return "B"
    return "C"


def _expand_paradas_hourly(paradas: pd.DataFrame) -> pd.DataFrame:
    if paradas.empty:
        return pd.DataFrame(columns=["linea_id", "timestamp", "downtime_frac"])

    acc: dict[tuple[str, pd.Timestamp], float] = {}
    for row in paradas.itertuples(index=False):
        start = pd.Timestamp(row.fecha_inicio)
        end = pd.Timestamp(row.fecha_fin)
        if end <= start:
            continue

        current = start.floor("h")
        while current < end:
            next_hour = current + pd.Timedelta(hours=1)
            overlap = (min(end, next_hour) - max(start, current)).total_seconds() / 3600.0
            if overlap > 0:
                key = (row.linea_id, current)
                acc[key] = acc.get(key, 0.0) + overlap
            current = next_hour

    rows = [
        {
            "linea_id": k[0],
            "timestamp": k[1],
            "downtime_frac": min(1.0, v),
        }
        for k, v in acc.items()
    ]
    return pd.DataFrame(rows)


def _desvios_hourly(desvios: pd.DataFrame) -> pd.DataFrame:
    if desvios.empty:
        return pd.DataFrame(columns=["linea_id", "timestamp", "indice_desvio_hora"])

    sev_map = {"Bajo": 1.0, "Medio": 2.0, "Alto": 3.0, "Critico": 4.0}
    d = desvios.copy()
    d["timestamp"] = pd.to_datetime(d["timestamp"]).dt.floor("h")
    d["sev"] = d["severidad_desvio"].map(sev_map).fillna(1.0)
    d["score"] = d["sev"] * np.log1p(d["duracion_min"]) * (0.75 + d["impacto_en_calidad_proxy"])

    agg = d.groupby(["linea_id", "timestamp"], as_index=False)["score"].sum()
    agg["indice_desvio_hora"] = np.clip(0.07 * agg["score"], 0.0, 3.5)
    return agg[["linea_id", "timestamp", "indice_desvio_hora"]]


def generate_consumos_energia_horarios(
    plantas: pd.DataFrame,
    lineas: pd.DataFrame,
    equipos: pd.DataFrame,
    contexto_operativo: pd.DataFrame,
    paradas: pd.DataFrame,
    desvios: pd.DataFrame,
    start_ts: str,
    end_ts: str,
    rng: np.random.Generator,
) -> pd.DataFrame:
    hourly = build_hourly_shift_frame(start_ts, end_ts)
    ctx = contexto_operativo.copy()

    line_map = lineas.set_index("linea_id")
    downtime_h = _expand_paradas_hourly(paradas)
    desvio_h = _desvios_hourly(desvios)

    downtime_series: dict[str, pd.Series] = {}
    for line_id, grp in downtime_h.groupby("linea_id"):
        s = grp.set_index("timestamp")["downtime_frac"].sort_index()
        downtime_series[line_id] = s

    desvio_series: dict[str, pd.Series] = {}
    for line_id, grp in desvio_h.groupby("linea_id"):
        s = grp.set_index("timestamp")["indice_desvio_hora"].sort_index()
        desvio_series[line_id] = s

    ctx_by_plant: dict[str, pd.DataFrame] = {}
    for planta_id in plantas["planta_id"]:
        tmp = hourly.copy()
        tmp["planta_id"] = planta_id
        tmp = tmp.merge(ctx, on=["fecha", "turno", "planta_id"], how="left")
        ctx_by_plant[planta_id] = tmp

    source_map = {
        "HornoProceso": "GasNatural",
        "LaminadorPrincipal": "Electricidad",
        "CompresorAire": "Electricidad",
        "BombaRecirculacion": "Electricidad",
        "SistemaControl": "Electricidad",
        "ManipuladorCarga": "Electricidad",
    }

    type_base_factor = {
        "HornoProceso": 0.52,
        "LaminadorPrincipal": 0.48,
        "CompresorAire": 0.43,
        "BombaRecirculacion": 0.40,
        "SistemaControl": 0.36,
        "ManipuladorCarga": 0.39,
    }

    outputs: list[pd.DataFrame] = []
    for eq in equipos.itertuples(index=False):
        line_data = line_map.loc[eq.linea_id]
        planta_id = line_data["planta_id"]
        fam = line_data["familia_proceso"]

        ctxp = ctx_by_plant[planta_id]
        n = len(ctxp)
        ts = ctxp["timestamp"]

        dt_s = downtime_series.get(eq.linea_id, pd.Series(dtype=float))
        dv_s = desvio_series.get(eq.linea_id, pd.Series(dtype=float))
        downtime = dt_s.reindex(ts, fill_value=0.0).values if not dt_s.empty else np.zeros(n)
        dev = dv_s.reindex(ts, fill_value=0.0).values if not dv_s.empty else np.zeros(n)

        turn_factor = ctxp["turno"].map({"A": 1.03, "B": 1.0, "C": 0.95}).values
        demand_idx = ctxp["demanda_operativa_indice"].values
        mix_idx = ctxp["mix_complejidad_indice"].values
        temp = ctxp["temperatura_ambiente"].values
        tension = ctxp["tension_operativa_proxy"].values

        age_years = (pd.Timestamp("2026-01-01") - pd.Timestamp(eq.fecha_instalacion)).days / 365.0
        degradation = np.linspace(0, min(0.09, age_years * 0.0045) * (eq.criticidad_equipo / 5), n)

        base = float(eq.potencia_nominal_kw) * type_base_factor.get(eq.tipo_equipo, 0.42)
        raw_load = (0.57 * demand_idx + 0.21 * mix_idx + 0.22 * turn_factor) * rng.normal(1.0, 0.045, n)
        factor_carga = np.clip(raw_load * (1 - 0.65 * downtime), 0.05, 1.15)

        low_load_penalty = np.where(factor_carga < 0.52, (0.52 - factor_carga) * 0.44, 0.0)
        peak_prob = 0.0018 + 0.0009 * eq.criticidad_equipo
        peak = rng.random(n) < peak_prob
        peak_mult = np.where(peak, rng.uniform(1.12, 1.55, n), 1.0)

        thermal_loss = np.zeros(n)
        if fam == "TratamientoTermico" or eq.tipo_equipo == "HornoProceso":
            thermal_loss = np.maximum(temp - 24.0, 0.0) * 0.0038

        demand_kw = (
            base
            * (0.34 + 0.78 * factor_carga)
            * peak_mult
            * (1 + (1 - eq.eficiencia_nominal) * 0.42 + degradation + low_load_penalty + 0.11 * dev + thermal_loss)
            * (1 + 0.05 * np.maximum(0, tension - 1.0))
        )
        demand_kw = np.clip(demand_kw, eq.potencia_nominal_kw * 0.04, eq.potencia_nominal_kw * 1.35)

        consumo_kwh = demand_kw
        fuente = source_map.get(eq.tipo_equipo, "Electricidad")
        if eq.tipo_equipo == "HornoProceso" and fam == "TratamientoTermico" and rng.random() < 0.35:
            fuente = "Vapor"

        reactive = np.where(
            fuente == "Electricidad",
            consumo_kwh * (0.11 + 0.09 * np.maximum(0, factor_carga - 0.84)),
            consumo_kwh * 0.018,
        )

        hour = ts.dt.hour.values
        tariff_peak = np.where(np.isin(hour, [9, 10, 11, 12, 13, 18, 19, 20]), 1.18, 0.93)
        price = (
            ctxp["precio_energia_mercado_proxy"].values
            * SOURCE_PRICE_MULTIPLIER[fuente]
            * tariff_peak
            * rng.normal(1.0, 0.02, n)
        )
        price = np.clip(price, 42.0, 285.0)

        coste = consumo_kwh / 1000.0 * price

        outputs.append(
            pd.DataFrame(
                {
                    "timestamp": ts.dt.strftime("%Y-%m-%dT%H:%M:%S"),
                    "planta_id": planta_id,
                    "linea_id": eq.linea_id,
                    "equipo_id": eq.equipo_id,
                    "fuente_energia": fuente,
                    "consumo_kwh": np.round(consumo_kwh, 4),
                    "demanda_kw": np.round(demand_kw, 4),
                    "factor_carga": np.round(factor_carga, 5),
                    "energia_reactiva_proxy": np.round(reactive, 4),
                    "coste_energia_proxy": np.round(coste, 4),
                    "precio_energia_proxy": np.round(price, 4),
                }
            )
        )

    out = pd.concat(outputs, ignore_index=True)
    return out


def generate_emisiones_estimadas(
    consumos_energia_horarios: pd.DataFrame,
    produccion_turno: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    c = consumos_energia_horarios.copy()
    c["timestamp"] = pd.to_datetime(c["timestamp"])

    agg = (
        c.groupby(["timestamp", "planta_id", "linea_id", "fuente_energia"], as_index=False)["consumo_kwh"]
        .sum()
        .sort_values(["timestamp", "planta_id", "linea_id"])
    )

    month = agg["timestamp"].dt.month
    seasonal_elec_adj = np.where(month.isin([12, 1, 2]), 0.03, np.where(month.isin([7, 8]), -0.02, 0.0))
    base_factor = agg["fuente_energia"].map(SOURCE_EMISSION_FACTOR).values
    factor = base_factor + np.where(agg["fuente_energia"].values == "Electricidad", seasonal_elec_adj, 0.0)
    factor = factor * rng.normal(1.0, 0.015, len(agg))
    factor = np.clip(factor, 0.12, 0.34)

    agg["factor_emision_proxy"] = np.round(factor, 5)
    agg["emisiones_co2e_kg"] = np.round(agg["consumo_kwh"] * agg["factor_emision_proxy"], 5)

    p = produccion_turno.copy()
    p["key"] = p["fecha"] + "|" + p["turno"] + "|" + p["planta_id"] + "|" + p["linea_id"]
    p_map = p.set_index("key")["toneladas_producidas_proxy"].to_dict()

    agg["fecha"] = agg["timestamp"].dt.date.astype(str)
    agg["turno"] = agg["timestamp"].dt.hour.map(_turno_from_hour)
    keys = agg["fecha"] + "|" + agg["turno"] + "|" + agg["planta_id"] + "|" + agg["linea_id"]

    ton_shift = keys.map(p_map).fillna(0.0).values
    ton_hour = np.where(ton_shift > 0, ton_shift / 8.0, np.nan)
    agg["intensidad_emisiones_por_unidad"] = np.round(np.where(ton_hour > 0, agg["emisiones_co2e_kg"] / ton_hour, np.nan), 5)
    agg["intensidad_emisiones_por_unidad"] = agg["intensidad_emisiones_por_unidad"].fillna(0.0)

    out = agg[
        [
            "timestamp",
            "planta_id",
            "linea_id",
            "fuente_energia",
            "consumo_kwh",
            "factor_emision_proxy",
            "emisiones_co2e_kg",
            "intensidad_emisiones_por_unidad",
        ]
    ].copy()
    out["timestamp"] = out["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S")
    return out
