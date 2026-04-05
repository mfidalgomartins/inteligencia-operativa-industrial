from __future__ import annotations

import numpy as np
import pandas as pd


def _turno_from_hour(hour: int) -> str:
    if 6 <= hour < 14:
        return "A"
    if 14 <= hour < 22:
        return "B"
    return "C"


def build_hourly_shift_frame(start_ts: str, end_ts: str) -> pd.DataFrame:
    ts = pd.date_range(start=start_ts, end=end_ts, freq="h")
    out = pd.DataFrame({"timestamp": ts})
    out["fecha"] = out["timestamp"].dt.date.astype(str)
    out["turno"] = out["timestamp"].dt.hour.map(_turno_from_hour)
    out["day_of_year"] = out["timestamp"].dt.dayofyear
    out["month"] = out["timestamp"].dt.month
    out["dow"] = out["timestamp"].dt.dayofweek
    return out


def generate_contexto_operativo(
    plantas: pd.DataFrame,
    start_ts: str,
    end_ts: str,
    rng: np.random.Generator,
) -> pd.DataFrame:
    daily = pd.date_range(start=start_ts[:10], end=end_ts[:10], freq="D")
    rows: list[dict[str, object]] = []

    for planta in plantas.itertuples(index=False):
        temp_base = 14.5 if planta.region == "Pais Vasco" else 13.2
        energy_premium = 1.02 if planta.criticidad_operativa >= 5 else 1.0

        for fecha in daily:
            day_of_year = fecha.day_of_year
            season = np.sin(2 * np.pi * day_of_year / 365.0)
            mix_base = 1 + 0.06 * np.cos(2 * np.pi * day_of_year / 180.0)
            temp_day = temp_base + 9.5 * season + rng.normal(0, 2.2)

            monthly_shock = 1.0
            if fecha.month in {2, 3}:
                monthly_shock = 1.08
            elif fecha.month in {7, 8}:
                monthly_shock = 0.94

            for turno, shift_adj in (("A", 0.04), ("B", 0.0), ("C", -0.05)):
                demanda = (
                    1
                    + 0.10 * season
                    + shift_adj
                    + 0.04 * (1 if fecha.dayofweek < 5 else -1)
                    + rng.normal(0, 0.045)
                )
                demanda = float(np.clip(demanda, 0.68, 1.38))

                mix = float(np.clip(mix_base + rng.normal(0, 0.05), 0.72, 1.32))
                precio = (
                    108.0
                    + 22.0 * (1 + 0.9 * season)
                    + 9.5 * (1 if turno == "B" else 0)
                    + 16.0 * (1 if fecha.dayofweek < 5 else 0)
                    + rng.normal(0, 6.5)
                )
                precio = float(np.clip(precio * monthly_shock * energy_premium, 62.0, 245.0))

                tension = (
                    0.45 * demanda
                    + 0.35 * mix
                    + 0.20 * (1 if turno == "C" else 0)
                    + rng.normal(0, 0.05)
                )
                tension = float(np.clip(tension, 0.35, 1.55))

                rows.append(
                    {
                        "fecha": fecha.date().isoformat(),
                        "turno": turno,
                        "planta_id": planta.planta_id,
                        "demanda_operativa_indice": round(demanda, 4),
                        "mix_complejidad_indice": round(mix, 4),
                        "temperatura_ambiente": round(temp_day + rng.normal(0, 1.2), 2),
                        "precio_energia_mercado_proxy": round(precio, 3),
                        "tension_operativa_proxy": round(tension, 4),
                    }
                )

    return pd.DataFrame(rows)


def generate_benchmark_externo_proxy(
    lineas_proceso: pd.DataFrame,
    start_ts: str,
    end_ts: str,
    rng: np.random.Generator,
) -> pd.DataFrame:
    meses = pd.date_range(start=start_ts[:10], end=end_ts[:10], freq="MS")
    familias = sorted(lineas_proceso["familia_proceso"].unique())

    refs = {
        "Laminacion": (545, 86, 102, 95),
        "TratamientoTermico": (705, 82, 118, 93),
        "Acabado": (455, 88, 84, 96),
        "InspeccionFinal": (295, 91, 51, 97),
    }

    rows: list[dict[str, object]] = []
    for mes in meses:
        for fam in familias:
            sec_ref, oee_ref, emi_ref, disp_ref = refs[fam]
            trend = (mes.year - 2024) * 0.9 + (mes.month - 1) * 0.05
            rows.append(
                {
                    "periodo": mes.strftime("%Y-%m"),
                    "familia_proceso": fam,
                    "sec_referencia": round(sec_ref * rng.uniform(0.97, 1.04) - trend, 3),
                    "oee_referencia": round(oee_ref * rng.uniform(0.98, 1.02) + 0.08 * trend, 3),
                    "emisiones_referencia": round(emi_ref * rng.uniform(0.95, 1.06) - 0.45 * trend, 3),
                    "disponibilidad_referencia": round(disp_ref * rng.uniform(0.99, 1.01) + 0.04 * trend, 3),
                }
            )

    return pd.DataFrame(rows)


def generate_escenarios_mejora(start_ts: str, end_ts: str, rng: np.random.Generator) -> pd.DataFrame:
    meses = pd.date_range(start=start_ts[:10], end=end_ts[:10], freq="MS")
    escenarios = [
        ("Base", 1.0, 1.0, 1.0, 1.0),
        ("ShockEnergia", 1.35, 1.02, 0.98, 1.1),
        ("AltaDemanda", 1.1, 1.25, 0.96, 1.06),
        ("RestriccionOperativa", 1.07, 1.05, 0.88, 1.28),
    ]

    rows: list[dict[str, object]] = []
    for mes in meses:
        for esc, p_shock, p_dem, p_staff, p_rest in escenarios:
            rows.append(
                {
                    "fecha": mes.date().isoformat(),
                    "escenario": esc,
                    "precio_energia_shock": round(p_shock * rng.uniform(0.97, 1.04), 3),
                    "presion_demanda_indice": round(p_dem * rng.uniform(0.96, 1.05), 3),
                    "disponibilidad_personal_indice": round(p_staff * rng.uniform(0.95, 1.04), 3),
                    "restriccion_operativa_indice": round(p_rest * rng.uniform(0.95, 1.05), 3),
                }
            )

    return pd.DataFrame(rows)
