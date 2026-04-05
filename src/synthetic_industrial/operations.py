from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from .context import build_hourly_shift_frame


SEVERITY_TO_NUM = {"Bajo": 1, "Medio": 2, "Alto": 3, "Critico": 4}


def _turno_from_hour(hour: int) -> str:
    if 6 <= hour < 14:
        return "A"
    if 14 <= hour < 22:
        return "B"
    return "C"


def _shift_interval(fecha: str, turno: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    dt = pd.Timestamp(fecha)
    if turno == "A":
        start = dt + pd.Timedelta(hours=6)
        end = dt + pd.Timedelta(hours=14)
    elif turno == "B":
        start = dt + pd.Timedelta(hours=14)
        end = dt + pd.Timedelta(hours=22)
    else:
        start = dt + pd.Timedelta(hours=22)
        end = dt + pd.Timedelta(days=1, hours=6)
    return start, end


def _cause_list_for_family(familia: str, planned: bool) -> list[str]:
    if planned:
        return [
            "MantenimientoPreventivoProgramado",
            "CambioFormatoPlanificado",
            "InspeccionSeguridadReglamentaria",
            "ParadaPlanificadaUtilidades",
        ]

    if familia == "Laminacion":
        return ["DesalineacionRodillos", "InestabilidadTorque", "FalloHidraulico", "AtascoMaterial"]
    if familia == "TratamientoTermico":
        return ["CurvaTermicaFueraBanda", "FalloQuemador", "CaidaGasIndustrial", "TermoparDescalibrado"]
    if familia == "Acabado":
        return ["DefectoSuperficial", "BloqueoManipulador", "VariacionEspesor", "FalloVision"]
    return ["NoConformidadLote", "FalloTransportador", "ErrorInspeccion", "AnomaliaInstrumentacion"]


def _variable_targets_by_family(familia: str) -> list[tuple[str, float]]:
    if familia == "Laminacion":
        return [("torque_eje", 420.0), ("presion_hidraulica", 128.0), ("vibracion", 3.2)]
    if familia == "TratamientoTermico":
        return [("temperatura_horno", 790.0), ("caudal_gas", 162.0), ("oxigeno_residual", 2.8)]
    if familia == "Acabado":
        return [("rugosidad", 1.6), ("velocidad_lineal", 92.0), ("tension_traccion", 68.0)]
    return [("precision_medicion", 99.4), ("caudal_aire", 44.0), ("vibracion", 2.5)]


def generate_paradas(
    lineas: pd.DataFrame,
    equipos: pd.DataFrame,
    contexto_operativo: pd.DataFrame,
    start_ts: str,
    end_ts: str,
    rng: np.random.Generator,
) -> pd.DataFrame:
    hourly = build_hourly_shift_frame(start_ts, end_ts)
    ctx = contexto_operativo.copy()

    line_to_equips = {
        line_id: grp.copy() for line_id, grp in equipos.merge(lineas[["linea_id", "planta_id"]], on="linea_id").groupby("linea_id")
    }

    rows: list[dict[str, object]] = []
    event_id = 1

    for linea in lineas.itertuples(index=False):
        line_hour = hourly.copy()
        line_hour["planta_id"] = linea.planta_id
        line_hour = line_hour.merge(ctx, on=["fecha", "turno", "planta_id"], how="left")

        next_free_ts = pd.Timestamp(start_ts)
        eq_df = line_to_equips[linea.linea_id]
        probs = eq_df["criticidad_equipo"].values.astype(float)
        probs = probs / probs.sum()

        for row in line_hour.itertuples(index=False):
            ts = row.timestamp
            if ts < next_free_ts:
                continue

            is_planned_window = (ts.day in {1, 15}) and (ts.hour in {6, 14, 22})
            planned_prob = 0.058 if is_planned_window else 0.0012
            unplanned_prob = (
                0.0018
                + 0.00065 * linea.criticidad_linea
                + (0.0012 if row.turno == "C" else 0.0)
                + 0.0013 * max(0.0, row.tension_operativa_proxy - 1.0)
            )

            planned_flag = False
            if rng.random() < planned_prob:
                planned_flag = True
            elif rng.random() >= unplanned_prob:
                continue

            if planned_flag:
                dur = float(np.clip(rng.gamma(shape=2.8, scale=1.6), 1.2, 14.0))
                start = ts
            else:
                dur = float(np.clip(rng.gamma(shape=1.7, scale=1.4), 0.35, 9.5))
                start = ts + pd.to_timedelta(int(rng.integers(0, 46)), unit="m")

            end = start + pd.to_timedelta(dur, unit="h")
            next_free_ts = end

            eq = eq_df.sample(1, weights=probs, random_state=int(rng.integers(0, 10_000_000))).iloc[0]
            cause = rng.choice(_cause_list_for_family(linea.familia_proceso, planned_flag))

            impacto_prod = float(np.round(dur * linea.capacidad_nominal_hora * rng.uniform(0.78, 1.16), 3))
            impacto_ene = float(np.round(dur * eq.potencia_nominal_kw * rng.uniform(0.11, 0.36), 3))

            rows.append(
                {
                    "parada_id": f"PAR_{event_id:08d}",
                    "fecha_inicio": start.isoformat(),
                    "fecha_fin": end.isoformat(),
                    "planta_id": linea.planta_id,
                    "linea_id": linea.linea_id,
                    "equipo_id": eq.equipo_id,
                    "tipo_parada": "Planificada" if planned_flag else "NoPlanificada",
                    "planificada_flag": int(planned_flag),
                    "causa_parada": cause,
                    "duracion_horas": round(dur, 3),
                    "impacto_produccion_proxy": impacto_prod,
                    "impacto_energia_proxy": impacto_ene,
                }
            )
            event_id += 1

    return pd.DataFrame(rows)


def generate_desvios_proceso(
    lineas: pd.DataFrame,
    equipos: pd.DataFrame,
    contexto_operativo: pd.DataFrame,
    start_ts: str,
    end_ts: str,
    rng: np.random.Generator,
) -> pd.DataFrame:
    hourly = build_hourly_shift_frame(start_ts, end_ts)
    ctx = contexto_operativo.copy()

    equipos_linea = {
        line_id: grp.copy() for line_id, grp in equipos.merge(lineas[["linea_id", "planta_id"]], on="linea_id").groupby("linea_id")
    }

    rows: list[dict[str, object]] = []
    desvio_id = 1

    base_prob = {
        "Laminacion": 0.026,
        "TratamientoTermico": 0.033,
        "Acabado": 0.023,
        "InspeccionFinal": 0.018,
    }

    for linea in lineas.itertuples(index=False):
        line_hour = hourly.copy()
        line_hour["planta_id"] = linea.planta_id
        line_hour = line_hour.merge(ctx, on=["fecha", "turno", "planta_id"], how="left")

        eq_df = equipos_linea[linea.linea_id]
        probs = eq_df["criticidad_equipo"].values.astype(float)
        probs = probs / probs.sum()
        variables = _variable_targets_by_family(linea.familia_proceso)

        for row in line_hour.itertuples(index=False):
            prob_evt = (
                base_prob[linea.familia_proceso]
                + 0.008 * max(0.0, row.tension_operativa_proxy - 1.0)
                + (0.004 if row.turno == "C" else 0.0)
            )
            if rng.random() >= prob_evt:
                continue

            n_events = 2 if rng.random() < 0.07 else 1
            for _ in range(n_events):
                eq = eq_df.sample(1, weights=probs, random_state=int(rng.integers(0, 10_000_000))).iloc[0]
                variable, objetivo = variables[int(rng.integers(0, len(variables)))]

                if rng.random() < 0.82:
                    desviacion_pct = float(rng.normal(0, 4.8))
                else:
                    desviacion_pct = float(rng.normal(0, 11.5))
                if abs(desviacion_pct) < 0.7:
                    desviacion_pct = float(np.sign(desviacion_pct if desviacion_pct != 0 else 1) * rng.uniform(0.8, 1.8))

                valor_observado = objetivo * (1 + desviacion_pct / 100)
                abs_dev = abs(desviacion_pct)
                if abs_dev < 3.5:
                    severidad = "Bajo"
                elif abs_dev < 7.5:
                    severidad = "Medio"
                elif abs_dev < 12.0:
                    severidad = "Alto"
                else:
                    severidad = "Critico"

                duracion = float(np.clip(rng.gamma(shape=2.0 + 0.25 * SEVERITY_TO_NUM[severidad], scale=9.0), 4, 240))
                causa = rng.choice(
                    [
                        "SetpointFueraBanda",
                        "DerivaInstrumentacion",
                        "MateriaPrimaVariable",
                        "InestabilidadControlPID",
                        "TransitorioArranque",
                    ]
                )
                impacto_calidad = float(
                    np.clip(0.12 * SEVERITY_TO_NUM[severidad] + 0.02 * abs_dev + rng.normal(0.0, 0.08), 0.02, 0.98)
                )

                rows.append(
                    {
                        "desvio_id": f"DES_{desvio_id:08d}",
                        "timestamp": (row.timestamp + pd.to_timedelta(int(rng.integers(0, 59)), unit="m")).isoformat(),
                        "planta_id": linea.planta_id,
                        "linea_id": linea.linea_id,
                        "equipo_id": eq.equipo_id,
                        "variable_proceso": variable,
                        "valor_observado": round(float(valor_observado), 4),
                        "valor_objetivo": round(float(objetivo), 4),
                        "desviacion_pct": round(float(desviacion_pct), 4),
                        "severidad_desvio": severidad,
                        "duracion_min": round(duracion, 3),
                        "causa_probable_proxy": causa,
                        "impacto_en_calidad_proxy": round(impacto_calidad, 4),
                    }
                )
                desvio_id += 1

    return pd.DataFrame(rows)


def aggregate_downtime_by_shift(paradas: pd.DataFrame) -> pd.DataFrame:
    acc: dict[tuple[str, str, str], float] = {}
    for row in paradas.itertuples(index=False):
        start = pd.Timestamp(row.fecha_inicio)
        end = pd.Timestamp(row.fecha_fin)
        day_start = (start - pd.Timedelta(days=1)).normalize()
        day_end = (end + pd.Timedelta(days=1)).normalize()

        for day in pd.date_range(day_start, day_end, freq="D"):
            fecha = day.date().isoformat()
            for turno in ("A", "B", "C"):
                shift_start, shift_end = _shift_interval(fecha, turno)
                overlap = (min(end, shift_end) - max(start, shift_start)).total_seconds() / 3600.0
                if overlap > 0:
                    key = (row.linea_id, fecha, turno)
                    acc[key] = acc.get(key, 0.0) + overlap

    rows = [
        {
            "linea_id": k[0],
            "fecha": k[1],
            "turno": k[2],
            "downtime_horas_turno": round(v, 4),
        }
        for k, v in acc.items()
    ]
    if not rows:
        return pd.DataFrame(columns=["linea_id", "fecha", "turno", "downtime_horas_turno"])
    out = pd.DataFrame(rows)
    out["downtime_horas_turno"] = out["downtime_horas_turno"].clip(0, 8)
    return out


def aggregate_desvios_by_shift(desvios: pd.DataFrame) -> pd.DataFrame:
    if desvios.empty:
        return pd.DataFrame(columns=["linea_id", "fecha", "turno", "indice_desvio_turno", "num_desvios_turno"])

    d = desvios.copy()
    d["timestamp"] = pd.to_datetime(d["timestamp"])
    d["fecha"] = d["timestamp"].dt.date.astype(str)
    d["turno"] = d["timestamp"].dt.hour.map(_turno_from_hour)
    d["sev_num"] = d["severidad_desvio"].map(SEVERITY_TO_NUM)
    d["score_evt"] = d["sev_num"] * np.log1p(d["duracion_min"]) * (0.8 + d["impacto_en_calidad_proxy"])

    agg = (
        d.groupby(["linea_id", "fecha", "turno"], as_index=False)
        .agg(
            score_total=("score_evt", "sum"),
            num_desvios_turno=("desvio_id", "count"),
            severidad_media=("sev_num", "mean"),
        )
        .reset_index(drop=True)
    )

    agg["indice_desvio_turno"] = np.clip(
        0.18 * agg["severidad_media"] + 0.14 * np.log1p(agg["num_desvios_turno"]) + 0.06 * np.log1p(agg["score_total"]),
        0.0,
        4.0,
    )
    return agg[["linea_id", "fecha", "turno", "indice_desvio_turno", "num_desvios_turno"]]


def generate_produccion_turno(
    lineas: pd.DataFrame,
    contexto_operativo: pd.DataFrame,
    paradas: pd.DataFrame,
    desvios: pd.DataFrame,
    start_ts: str,
    end_ts: str,
    rng: np.random.Generator,
) -> pd.DataFrame:
    fechas = pd.date_range(start=start_ts[:10], end=end_ts[:10], freq="D")

    downtime_shift = aggregate_downtime_by_shift(paradas)
    dev_shift = aggregate_desvios_by_shift(desvios)

    product_mix = {
        "Laminacion": [("OCTG", 0.09, 118.0), ("Conduccion", 0.075, 125.0), ("Mecanizado", 0.061, 112.0)],
        "TratamientoTermico": [("AleacionAlta", 0.054, 88.0), ("AleacionMedia", 0.062, 94.0), ("Inoxidable", 0.048, 82.0)],
        "Acabado": [("PremiumRosca", 0.041, 84.0), ("Industrial", 0.058, 92.0), ("Especial", 0.045, 78.0)],
        "InspeccionFinal": [("LoteCritico", 0.052, 76.0), ("LoteEstandar", 0.065, 84.0), ("LoteRapido", 0.073, 95.0)],
    }
    base_scrap = {"Laminacion": 2.8, "TratamientoTermico": 3.5, "Acabado": 2.2, "InspeccionFinal": 1.6}
    base_repro = {"Laminacion": 1.4, "TratamientoTermico": 1.9, "Acabado": 1.2, "InspeccionFinal": 0.9}

    idx_ctx = contexto_operativo.set_index(["fecha", "turno", "planta_id"])
    idx_dt = downtime_shift.set_index(["linea_id", "fecha", "turno"]) if not downtime_shift.empty else None
    idx_dv = dev_shift.set_index(["linea_id", "fecha", "turno"]) if not dev_shift.empty else None

    rows: list[dict[str, object]] = []

    for linea in lineas.itertuples(index=False):
        fam_mix = product_mix[linea.familia_proceso]
        for fecha in fechas:
            fecha_str = fecha.date().isoformat()
            for turno in ("A", "B", "C"):
                ctx_key = (fecha_str, turno, linea.planta_id)
                if ctx_key not in idx_ctx.index:
                    continue
                ctx = idx_ctx.loc[ctx_key]

                dt = 0.0
                if idx_dt is not None and (linea.linea_id, fecha_str, turno) in idx_dt.index:
                    dt = float(idx_dt.loc[(linea.linea_id, fecha_str, turno), "downtime_horas_turno"])
                dt = float(np.clip(dt, 0.0, 8.0))

                dev_idx = 0.0
                if idx_dv is not None and (linea.linea_id, fecha_str, turno) in idx_dv.index:
                    dev_idx = float(idx_dv.loc[(linea.linea_id, fecha_str, turno), "indice_desvio_turno"])
                dev_idx = float(np.clip(dev_idx, 0.0, 4.0))

                probs = np.array([0.36, 0.39, 0.25])
                probs = probs * np.array([
                    1.0 + 0.35 * max(0.0, ctx.mix_complejidad_indice - 1.0),
                    1.0,
                    1.0 - 0.22 * max(0.0, ctx.mix_complejidad_indice - 1.0),
                ])
                probs = probs / probs.sum()
                prod_idx = int(rng.choice(np.arange(len(fam_mix)), p=probs))
                producto, ton_per_unit, speed_base = fam_mix[prod_idx]

                horas_plan = 8.0
                horas_op = float(np.clip(horas_plan - dt, 0.0, horas_plan))
                util = (
                    (horas_op / horas_plan)
                    * (0.75 + 0.30 * ctx.demanda_operativa_indice)
                    * (0.95 - 0.06 * max(0.0, ctx.mix_complejidad_indice - 1.0))
                    * rng.uniform(0.92, 1.07)
                )
                util = float(np.clip(util, 0.05, 1.05))

                shift_penalty = 1.0 if turno != "C" else 0.94
                performance = (
                    (linea.oee_objetivo / 100.0)
                    * shift_penalty
                    * (1.0 - 0.05 * dev_idx)
                    * (1.0 - 0.08 * (dt / 8.0))
                    * rng.uniform(0.93, 1.05)
                )
                performance = float(np.clip(performance, 0.28, 1.02))

                ton_gross = linea.capacidad_nominal_hora * horas_op * performance * (0.90 + 0.14 * util)
                scrap = (
                    base_scrap[linea.familia_proceso]
                    + 1.1 * dev_idx
                    + 1.35 * (dt / 8.0)
                    + (0.55 if turno == "C" else 0.0)
                    + rng.normal(0, 0.45)
                )
                repro = (
                    base_repro[linea.familia_proceso]
                    + 0.65 * dev_idx
                    + 0.62 * (dt / 8.0)
                    + rng.normal(0, 0.25)
                )
                scrap = float(np.clip(scrap, 0.6, 22.0))
                repro = float(np.clip(repro, 0.2, 14.0))

                ton_neta = float(max(0.0, ton_gross * (1 - scrap / 100.0)))
                unidades = int(max(0, round(ton_neta / ton_per_unit)))
                vel = speed_base * (0.86 + 0.22 * util) * (1 - 0.05 * dev_idx) * (1 - 0.04 * (dt / 8.0))
                vel = float(np.clip(vel, 12.0, 175.0))

                rows.append(
                    {
                        "fecha": fecha_str,
                        "turno": turno,
                        "planta_id": linea.planta_id,
                        "linea_id": linea.linea_id,
                        "producto_familia": producto,
                        "unidades_producidas": unidades,
                        "toneladas_producidas_proxy": round(ton_neta, 4),
                        "horas_planificadas": round(horas_plan, 3),
                        "horas_operativas": round(horas_op, 3),
                        "scrap_pct": round(scrap, 4),
                        "reproceso_pct": round(repro, 4),
                        "velocidad_media_proceso": round(vel, 4),
                        "utilization_rate": round(util, 5),
                    }
                )

    return pd.DataFrame(rows)


def generate_mantenimientos(
    paradas: pd.DataFrame,
    equipos: pd.DataFrame,
    start_ts: str,
    end_ts: str,
    rng: np.random.Generator,
) -> pd.DataFrame:
    eq_info = equipos.set_index("equipo_id")
    rows: list[dict[str, object]] = []
    m_id = 1

    for parada in paradas.itertuples(index=False):
        programado = int(parada.planificada_flag)
        do_record = bool(programado) or (rng.random() < 0.84)
        if not do_record:
            continue

        correctivo = int(not bool(programado))
        if programado:
            tipo = rng.choice(["Preventivo", "Predictivo", "OverhaulParcial"])
        else:
            tipo = rng.choice(["CorrectivoMecanico", "CorrectivoInstrumentacion", "CorrectivoTermico"])

        horas = float(np.clip(parada.duracion_horas * rng.uniform(0.72, 1.28), 0.5, 18.0))
        criticidad = int(eq_info.loc[parada.equipo_id, "criticidad_equipo"])
        costo_hora = 380.0 + 95.0 * criticidad
        coste = float(round(horas * costo_hora * rng.uniform(0.92, 1.24), 2))

        if correctivo:
            resultado = rng.choice(["Estabilizado", "Recuperado", "ParcialConSeguimiento"], p=[0.54, 0.24, 0.22])
        else:
            resultado = rng.choice(["Exitoso", "ExitosoConAjustes", "PendienteOptimizar"], p=[0.66, 0.27, 0.07])

        fin = pd.Timestamp(parada.fecha_inicio) + pd.to_timedelta(horas, unit="h")
        rows.append(
            {
                "mantenimiento_id": f"MAN_{m_id:08d}",
                "fecha_inicio": parada.fecha_inicio,
                "fecha_fin": fin.isoformat(),
                "planta_id": parada.planta_id,
                "linea_id": parada.linea_id,
                "equipo_id": parada.equipo_id,
                "tipo_mantenimiento": tipo,
                "programado_flag": programado,
                "correctivo_flag": correctivo,
                "horas_mantenimiento": round(horas, 3),
                "coste_mantenimiento_proxy": coste,
                "resultado_mantenimiento": resultado,
            }
        )
        m_id += 1

    # Programa predictivo regular para equipos críticos.
    start = pd.Timestamp(start_ts)
    end = pd.Timestamp(end_ts)
    for eq in equipos.itertuples(index=False):
        if eq.criticidad_equipo < 4:
            continue
        next_date = start + pd.to_timedelta(int(rng.integers(8, 32)), unit="D")
        while next_date < end:
            horas = float(np.clip(rng.normal(2.4, 0.75), 1.0, 5.5))
            coste = float(round((320.0 + 70.0 * eq.criticidad_equipo) * horas * rng.uniform(0.9, 1.15), 2))
            rows.append(
                {
                    "mantenimiento_id": f"MAN_{m_id:08d}",
                    "fecha_inicio": next_date.isoformat(),
                    "fecha_fin": (next_date + pd.to_timedelta(horas, unit="h")).isoformat(),
                    "planta_id": eq.linea_id.split("_")[0] + "_" + eq.linea_id.split("_")[1],
                    "linea_id": eq.linea_id,
                    "equipo_id": eq.equipo_id,
                    "tipo_mantenimiento": "PredictivoRutinario",
                    "programado_flag": 1,
                    "correctivo_flag": 0,
                    "horas_mantenimiento": round(horas, 3),
                    "coste_mantenimiento_proxy": coste,
                    "resultado_mantenimiento": rng.choice(["Exitoso", "ExitosoConAjustes", "PendienteOptimizar"], p=[0.7, 0.25, 0.05]),
                }
            )
            m_id += 1
            next_date += pd.to_timedelta(int(rng.integers(48, 75)), unit="D")

    out = pd.DataFrame(rows)
    out["fecha_inicio"] = pd.to_datetime(out["fecha_inicio"], format="mixed").dt.strftime("%Y-%m-%dT%H:%M:%S")
    out["fecha_fin"] = pd.to_datetime(out["fecha_fin"], format="mixed").dt.strftime("%Y-%m-%dT%H:%M:%S")
    return out
