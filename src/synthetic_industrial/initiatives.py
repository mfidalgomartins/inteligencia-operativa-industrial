from __future__ import annotations

import numpy as np
import pandas as pd


def generate_iniciativas_mejora(
    lineas: pd.DataFrame,
    equipos: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    eq_by_line = {line_id: grp for line_id, grp in equipos.groupby("linea_id")}

    templates = [
        {
            "categoria_iniciativa": "EficienciaEnergetica",
            "tipo_iniciativa": "QuickWinOperativo",
            "descripcion_resumida": "Ajuste fino de setpoints y secuencia de arranque/parada",
            "impacto_esperado_energia_pct": (1.5, 5.5),
            "impacto_esperado_oee_pct": (0.4, 1.8),
            "impacto_esperado_emisiones_pct": (1.2, 4.2),
            "dificultad": (1, 2),
            "quick_win_flag": 1,
        },
        {
            "categoria_iniciativa": "Confiabilidad",
            "tipo_iniciativa": "MantenimientoPredictivo",
            "descripcion_resumida": "Analitica predictiva de fallo en equipos criticos",
            "impacto_esperado_energia_pct": (0.8, 2.6),
            "impacto_esperado_oee_pct": (1.5, 4.8),
            "impacto_esperado_emisiones_pct": (0.7, 2.2),
            "dificultad": (2, 4),
            "quick_win_flag": 0,
        },
        {
            "categoria_iniciativa": "OptimizacionProceso",
            "tipo_iniciativa": "ControlAvanzado",
            "descripcion_resumida": "Control multivariable para reducir variabilidad de proceso",
            "impacto_esperado_energia_pct": (2.5, 7.5),
            "impacto_esperado_oee_pct": (2.0, 5.6),
            "impacto_esperado_emisiones_pct": (2.2, 6.5),
            "dificultad": (3, 4),
            "quick_win_flag": 0,
        },
        {
            "categoria_iniciativa": "TransformacionIndustrial",
            "tipo_iniciativa": "CAPEXTransformacional",
            "descripcion_resumida": "Revamping de equipos termicos y recuperacion de calor",
            "impacto_esperado_energia_pct": (7.0, 18.0),
            "impacto_esperado_oee_pct": (2.8, 8.5),
            "impacto_esperado_emisiones_pct": (8.0, 20.0),
            "dificultad": (4, 5),
            "quick_win_flag": 0,
        },
    ]

    rows: list[dict[str, object]] = []
    ini_id = 1

    for linea in lineas.itertuples(index=False):
        eqs = eq_by_line[linea.linea_id]
        weights = eqs["criticidad_equipo"].values.astype(float)
        weights = weights / weights.sum()

        for template in templates:
            equipo = eqs.sample(1, weights=weights, random_state=int(rng.integers(0, 10_000_000))).iloc[0]

            # Algunas iniciativas son de linea, no de equipo particular.
            equipo_id = equipo.equipo_id if rng.random() < 0.82 else ""

            rows.append(
                {
                    "iniciativa_id": f"INI_{ini_id:07d}",
                    "planta_id": linea.planta_id,
                    "linea_id": linea.linea_id,
                    "equipo_id": equipo_id,
                    "categoria_iniciativa": template["categoria_iniciativa"],
                    "tipo_iniciativa": template["tipo_iniciativa"],
                    "descripcion_resumida": template["descripcion_resumida"],
                    "impacto_esperado_energia_pct": round(float(rng.uniform(*template["impacto_esperado_energia_pct"])), 3),
                    "impacto_esperado_oee_pct": round(float(rng.uniform(*template["impacto_esperado_oee_pct"])), 3),
                    "impacto_esperado_emisiones_pct": round(float(rng.uniform(*template["impacto_esperado_emisiones_pct"])), 3),
                    "dificultad_implementacion": int(rng.integers(template["dificultad"][0], template["dificultad"][1] + 1)),
                    "quick_win_flag": int(template["quick_win_flag"]),
                }
            )
            ini_id += 1

    return pd.DataFrame(rows)


def generate_capex_proyectos(
    iniciativas_mejora: pd.DataFrame,
    lineas_proceso: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    line_idx = lineas_proceso.set_index("linea_id")
    rows: list[dict[str, object]] = []

    project_id = 1
    for ini in iniciativas_mejora.itertuples(index=False):
        line = line_idx.loc[ini.linea_id]
        cap_base = {
            "QuickWinOperativo": (12000, 45000),
            "MantenimientoPredictivo": (90000, 260000),
            "ControlAvanzado": (180000, 460000),
            "CAPEXTransformacional": (850000, 2400000),
        }[ini.tipo_iniciativa]

        capex = float(rng.uniform(*cap_base))
        if ini.quick_win_flag == 1:
            capex *= rng.uniform(0.8, 1.1)

        # Estimador proxy de ahorro anual en base a capacidad, energía objetivo e impacto esperado.
        energia_base_cost = line.capacidad_nominal_hora * line.intensidad_energetica_objetivo * 24 * 330 * 0.11
        ahorro_energia = energia_base_cost * (ini.impacto_esperado_energia_pct / 100) * rng.uniform(0.72, 1.1)
        ahorro_oee = line.capacidad_nominal_hora * 24 * 330 * 120 * (ini.impacto_esperado_oee_pct / 100) * rng.uniform(0.52, 0.9)
        ahorro_emi = 90000 * (ini.impacto_esperado_emisiones_pct / 100) * rng.uniform(0.65, 1.05)
        ahorro_anual = float(max(3500.0, ahorro_energia + ahorro_oee + ahorro_emi))

        opex_impacto = float(rng.normal(loc=12000 if ini.quick_win_flag == 0 else 4500, scale=6000))
        opex_impacto = float(np.clip(opex_impacto, -25000, 85000))

        neto_anual = max(1500.0, ahorro_anual - max(0.0, opex_impacto))
        payback = float(np.clip(capex / (neto_anual / 12.0), 2.5, 180.0))

        criticidad_estrategica = int(
            np.clip(
                np.round(
                    0.42 * line.criticidad_linea
                    + 0.25 * ini.dificultad_implementacion
                    + 0.33 * (5 if ini.tipo_iniciativa == "CAPEXTransformacional" else 3)
                ),
                1,
                5,
            )
        )

        if payback <= 12:
            horizonte = "0-12 meses"
        elif payback <= 24:
            horizonte = "12-24 meses"
        elif payback <= 48:
            horizonte = "24-48 meses"
        else:
            horizonte = "48+ meses"

        if ini.tipo_iniciativa == "CAPEXTransformacional":
            riesgo = rng.choice(["Medio", "Alto"], p=[0.35, 0.65])
        elif ini.quick_win_flag == 1:
            riesgo = rng.choice(["Bajo", "Medio"], p=[0.82, 0.18])
        else:
            riesgo = rng.choice(["Bajo", "Medio", "Alto"], p=[0.28, 0.56, 0.16])

        rows.append(
            {
                "proyecto_id": f"PRY_{project_id:07d}",
                "iniciativa_id": ini.iniciativa_id,
                "capex_estimado": round(capex, 2),
                "opex_impacto_proxy": round(opex_impacto, 2),
                "ahorro_anual_estimado": round(ahorro_anual, 2),
                "payback_estimado_meses": round(payback, 2),
                "criticidad_estrategica": criticidad_estrategica,
                "horizonte_implantacion": horizonte,
                "riesgo_implantacion": riesgo,
            }
        )
        project_id += 1

    return pd.DataFrame(rows)
