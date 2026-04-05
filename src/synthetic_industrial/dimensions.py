from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pandas as pd


def generate_plantas(rng: np.random.Generator) -> pd.DataFrame:
    rows = [
        {
            "planta_id": "PLT_BIZ",
            "nombre_planta": "Planta Bizkaia Norte",
            "tipo_planta": "Tubos sin soldadura",
            "region": "Pais Vasco",
            "intensidad_energetica_base": 612.0,
            "criticidad_operativa": 5,
            "estrategia_mejora_actual": "Eficiencia termica y estabilidad de proceso",
        },
        {
            "planta_id": "PLT_ALA",
            "nombre_planta": "Planta Alava Precision",
            "tipo_planta": "Acabado y tratamiento",
            "region": "Pais Vasco",
            "intensidad_energetica_base": 534.0,
            "criticidad_operativa": 4,
            "estrategia_mejora_actual": "Confiabilidad de equipos criticos",
        },
        {
            "planta_id": "PLT_CAN",
            "nombre_planta": "Planta Cantabria Energy",
            "tipo_planta": "Laminacion y conformado",
            "region": "Cantabria",
            "intensidad_energetica_base": 578.0,
            "criticidad_operativa": 4,
            "estrategia_mejora_actual": "Reduccion de consumo especifico",
        },
    ]
    df = pd.DataFrame(rows)
    # Variacion leve para evitar datos perfectamente estaticos.
    df["intensidad_energetica_base"] = df["intensidad_energetica_base"] * rng.uniform(0.97, 1.03, len(df))
    df["intensidad_energetica_base"] = df["intensidad_energetica_base"].round(2)
    return df


def generate_lineas_proceso(plantas: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    familias = [
        ("Laminacion", 9.6, 84.0, 590.0),
        ("TratamientoTermico", 7.2, 82.0, 730.0),
        ("Acabado", 8.8, 86.0, 470.0),
        ("InspeccionFinal", 10.5, 89.0, 315.0),
    ]

    rows: list[dict[str, object]] = []
    for planta in plantas.itertuples(index=False):
        for idx, (familia, base_cap, oee_obj, ie_obj) in enumerate(familias, start=1):
            rows.append(
                {
                    "linea_id": f"{planta.planta_id}_L{idx:02d}",
                    "planta_id": planta.planta_id,
                    "nombre_linea": f"Linea_{familia}_{idx}",
                    "familia_proceso": familia,
                    "capacidad_nominal_hora": round(base_cap * rng.uniform(0.85, 1.2), 2),
                    "oee_objetivo": round(oee_obj * rng.uniform(0.94, 1.03), 2),
                    "intensidad_energetica_objetivo": round(ie_obj * rng.uniform(0.92, 1.08), 2),
                    "criticidad_linea": int(rng.integers(2, 6)),
                }
            )

    return pd.DataFrame(rows)


def generate_equipos(lineas: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    template = [
        ("HornoProceso", "Termico", 1900, 0.91, 0.86),
        ("LaminadorPrincipal", "Mecanico", 1450, 0.93, 0.89),
        ("CompresorAire", "Utilidades", 680, 0.94, 0.88),
        ("BombaRecirculacion", "Hidraulico", 520, 0.95, 0.90),
        ("SistemaControl", "Automatizacion", 260, 0.97, 0.94),
        ("ManipuladorCarga", "MovimientoMateriales", 390, 0.94, 0.87),
    ]

    rows: list[dict[str, object]] = []
    for linea in lineas.itertuples(index=False):
        for idx, (tipo, subsistema, kw, disp, eff) in enumerate(template, start=1):
            anios_antiguedad = int(rng.integers(2, 18))
            fecha_inst = datetime(2026, 1, 1) - timedelta(days=365 * anios_antiguedad + int(rng.integers(0, 365)))
            rows.append(
                {
                    "equipo_id": f"{linea.linea_id}_EQ{idx:02d}",
                    "linea_id": linea.linea_id,
                    "tipo_equipo": tipo,
                    "subsistema": subsistema,
                    "fecha_instalacion": fecha_inst.date().isoformat(),
                    "potencia_nominal_kw": round(kw * rng.uniform(0.78, 1.22), 2),
                    "disponibilidad_objetivo": round(min(0.99, max(0.86, disp * rng.uniform(0.97, 1.02))), 4),
                    "criticidad_equipo": int(rng.integers(1, 6)),
                    "eficiencia_nominal": round(min(0.98, max(0.78, eff * rng.uniform(0.95, 1.03))), 4),
                }
            )

    return pd.DataFrame(rows)
