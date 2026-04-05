from __future__ import annotations

from typing import Any

import pandas as pd


MANDATORY_TABLES = [
    "plantas",
    "lineas_proceso",
    "equipos",
    "consumos_energia_horarios",
    "produccion_turno",
    "desvios_proceso",
    "paradas",
    "mantenimientos",
    "emisiones_estimadas",
    "iniciativas_mejora",
    "capex_proyectos",
    "contexto_operativo",
    "benchmark_externo_proxy",
    "escenarios_mejora",
]


def run_plausibility_checks(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    checks: list[dict[str, Any]] = []

    def add(name: str, passed: bool, value: Any, threshold: str, detail: str) -> None:
        checks.append(
            {
                "check_name": name,
                "status": "OK" if passed else "ERROR",
                "observed_value": value,
                "threshold": threshold,
                "detail": detail,
            }
        )

    for table in MANDATORY_TABLES:
        add(
            name=f"tabla_presente_{table}",
            passed=table in tables and not tables[table].empty,
            value=0 if table not in tables else len(tables[table]),
            threshold=">0 filas",
            detail=f"Validacion de existencia y no vacio para {table}",
        )

    contexto = tables["contexto_operativo"].copy()
    min_fecha = pd.to_datetime(contexto["fecha"]).min()
    max_fecha = pd.to_datetime(contexto["fecha"]).max()
    months = (max_fecha.to_period("M") - min_fecha.to_period("M")).n + 1
    add("historico_minimo", months >= 18, months, ">=18 meses", "Horizonte temporal sintetico")

    add("multiples_plantas", tables["plantas"]["planta_id"].nunique() >= 3, tables["plantas"]["planta_id"].nunique(), ">=3", "Cobertura multipla de plantas")
    add("multiples_lineas", tables["lineas_proceso"]["linea_id"].nunique() >= 8, tables["lineas_proceso"]["linea_id"].nunique(), ">=8", "Cobertura de lineas")
    add("multiples_equipos", tables["equipos"]["equipo_id"].nunique() >= 40, tables["equipos"]["equipo_id"].nunique(), ">=40", "Cobertura de activos")

    prod = tables["produccion_turno"]
    add("scrap_rango", prod["scrap_pct"].between(0, 30).all(), bool(prod["scrap_pct"].between(0, 30).all()), "[0,30]", "Rango fisico scrap")
    add("utilization_rango", prod["utilization_rate"].between(0, 1.1).all(), bool(prod["utilization_rate"].between(0, 1.1).all()), "[0,1.1]", "Rango utilization")
    add("horas_operativas_validas", prod["horas_operativas"].between(0, 8).all(), bool(prod["horas_operativas"].between(0, 8).all()), "[0,8]", "Horas operativas por turno")

    ene = tables["consumos_energia_horarios"]
    add("consumo_positivo", (ene["consumo_kwh"] > 0).all(), bool((ene["consumo_kwh"] > 0).all()), ">0", "Consumo energetico por equipo-hora")
    add("factor_carga_rango", ene["factor_carga"].between(0, 1.2).all(), bool(ene["factor_carga"].between(0, 1.2).all()), "[0,1.2]", "Factor de carga plausible")

    paradas = tables["paradas"]
    add("paradas_planificadas_presentes", (paradas["planificada_flag"] == 1).any(), bool((paradas["planificada_flag"] == 1).any()), "al menos 1", "Debe haber paradas planificadas")
    add("paradas_no_planificadas_presentes", (paradas["planificada_flag"] == 0).any(), bool((paradas["planificada_flag"] == 0).any()), "al menos 1", "Debe haber paradas no planificadas")

    ini = tables["iniciativas_mejora"]
    add("quickwins_presentes", (ini["quick_win_flag"] == 1).any(), bool((ini["quick_win_flag"] == 1).any()), "al menos 1", "Quick wins necesarios")
    add(
        "capex_presentes",
        (ini["tipo_iniciativa"] == "CAPEXTransformacional").any(),
        bool((ini["tipo_iniciativa"] == "CAPEXTransformacional").any()),
        "al menos 1",
        "Iniciativas transformacionales necesarias",
    )

    capex = tables["capex_proyectos"]
    add("payback_corto_existente", (capex["payback_estimado_meses"] <= 12).any(), bool((capex["payback_estimado_meses"] <= 12).any()), "al menos 1", "Debe existir algun caso de alto retorno")
    add("payback_largo_existente", (capex["payback_estimado_meses"] >= 36).any(), bool((capex["payback_estimado_meses"] >= 36).any()), "al menos 1", "Debe existir algun caso de retorno largo")

    return pd.DataFrame(checks)


def build_cardinality_summary(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    for name, df in tables.items():
        row: dict[str, Any] = {
            "tabla": name,
            "filas": len(df),
            "columnas": len(df.columns),
        }

        if "planta_id" in df.columns:
            row["n_plantas"] = int(df["planta_id"].nunique())
        if "linea_id" in df.columns:
            row["n_lineas"] = int(df["linea_id"].nunique())
        if "equipo_id" in df.columns:
            row["n_equipos"] = int(df["equipo_id"].replace("", pd.NA).dropna().nunique())

        if "timestamp" in df.columns:
            ts = pd.to_datetime(df["timestamp"])
            row["periodo_inicio"] = ts.min().strftime("%Y-%m-%d %H:%M:%S")
            row["periodo_fin"] = ts.max().strftime("%Y-%m-%d %H:%M:%S")
        elif "fecha" in df.columns:
            f = pd.to_datetime(df["fecha"])
            row["periodo_inicio"] = f.min().strftime("%Y-%m-%d")
            row["periodo_fin"] = f.max().strftime("%Y-%m-%d")
        elif "periodo" in df.columns:
            row["periodo_inicio"] = str(df["periodo"].min())
            row["periodo_fin"] = str(df["periodo"].max())
        else:
            row["periodo_inicio"] = ""
            row["periodo_fin"] = ""

        rows.append(row)

    return pd.DataFrame(rows).sort_values("tabla").reset_index(drop=True)


def build_logic_summary_text() -> str:
    return """# Resumen de Lógica Sintética

## Principios de generación
- Horizonte temporal de 24 meses con granularidad horaria y por turno.
- Diferencias estructurales entre plantas, líneas y equipos.
- Comportamiento no estacionario: estacionalidad, shocks de energía y variación de mix.
- Relación causal proxy entre desvíos de proceso, scrap, consumo específico y paradas.
- Coexistencia de iniciativas quick win y CAPEX transformacional con perfiles de retorno distintos.

## Mecánicas simuladas
1. Contexto operativo: demanda, complejidad de mix, temperatura y precio de energía por planta/turno.
2. Paradas: planificadas y no planificadas con severidad distinta por familia de proceso.
3. Desvíos: probabilidad dependiente de tensión operativa y turno, con severidad y duración.
4. Producción por turno: afectada por downtime, desvíos, demanda y complejidad.
5. Consumo energético horario: sensible a carga, degradación de activo, pérdidas térmicas y sobreconsumo.
6. Emisiones estimadas: derivadas del consumo y del factor de emisión por fuente energética.
7. Iniciativas y CAPEX: impacto esperado heterogéneo, dificultad, riesgo y payback.

## Reproducibilidad
- Generación determinista bajo semilla fija.
- Salida estandarizada a CSV en `data/raw/`.
- Incluye reportes de plausibilidad y cardinalidades para auditoría.
"""
