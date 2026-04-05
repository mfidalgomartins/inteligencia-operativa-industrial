from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from .config import GenerationConfig
from .context import generate_benchmark_externo_proxy, generate_contexto_operativo, generate_escenarios_mejora
from .dimensions import generate_equipos, generate_lineas_proceso, generate_plantas
from .energy import generate_consumos_energia_horarios, generate_emisiones_estimadas
from .initiatives import generate_capex_proyectos, generate_iniciativas_mejora
from .operations import generate_desvios_proceso, generate_mantenimientos, generate_paradas, generate_produccion_turno
from .validation import build_cardinality_summary, build_logic_summary_text, run_plausibility_checks


TABLE_COLUMNS = {
    "plantas": [
        "planta_id",
        "nombre_planta",
        "tipo_planta",
        "region",
        "intensidad_energetica_base",
        "criticidad_operativa",
        "estrategia_mejora_actual",
    ],
    "lineas_proceso": [
        "linea_id",
        "planta_id",
        "nombre_linea",
        "familia_proceso",
        "capacidad_nominal_hora",
        "oee_objetivo",
        "intensidad_energetica_objetivo",
        "criticidad_linea",
    ],
    "equipos": [
        "equipo_id",
        "linea_id",
        "tipo_equipo",
        "subsistema",
        "fecha_instalacion",
        "potencia_nominal_kw",
        "disponibilidad_objetivo",
        "criticidad_equipo",
        "eficiencia_nominal",
    ],
    "consumos_energia_horarios": [
        "timestamp",
        "planta_id",
        "linea_id",
        "equipo_id",
        "fuente_energia",
        "consumo_kwh",
        "demanda_kw",
        "factor_carga",
        "energia_reactiva_proxy",
        "coste_energia_proxy",
        "precio_energia_proxy",
    ],
    "produccion_turno": [
        "fecha",
        "turno",
        "planta_id",
        "linea_id",
        "producto_familia",
        "unidades_producidas",
        "toneladas_producidas_proxy",
        "horas_planificadas",
        "horas_operativas",
        "scrap_pct",
        "reproceso_pct",
        "velocidad_media_proceso",
        "utilization_rate",
    ],
    "desvios_proceso": [
        "desvio_id",
        "timestamp",
        "planta_id",
        "linea_id",
        "equipo_id",
        "variable_proceso",
        "valor_observado",
        "valor_objetivo",
        "desviacion_pct",
        "severidad_desvio",
        "duracion_min",
        "causa_probable_proxy",
        "impacto_en_calidad_proxy",
    ],
    "paradas": [
        "parada_id",
        "fecha_inicio",
        "fecha_fin",
        "planta_id",
        "linea_id",
        "equipo_id",
        "tipo_parada",
        "planificada_flag",
        "causa_parada",
        "duracion_horas",
        "impacto_produccion_proxy",
        "impacto_energia_proxy",
    ],
    "mantenimientos": [
        "mantenimiento_id",
        "fecha_inicio",
        "fecha_fin",
        "planta_id",
        "linea_id",
        "equipo_id",
        "tipo_mantenimiento",
        "programado_flag",
        "correctivo_flag",
        "horas_mantenimiento",
        "coste_mantenimiento_proxy",
        "resultado_mantenimiento",
    ],
    "emisiones_estimadas": [
        "timestamp",
        "planta_id",
        "linea_id",
        "fuente_energia",
        "consumo_kwh",
        "factor_emision_proxy",
        "emisiones_co2e_kg",
        "intensidad_emisiones_por_unidad",
    ],
    "iniciativas_mejora": [
        "iniciativa_id",
        "planta_id",
        "linea_id",
        "equipo_id",
        "categoria_iniciativa",
        "tipo_iniciativa",
        "descripcion_resumida",
        "impacto_esperado_energia_pct",
        "impacto_esperado_oee_pct",
        "impacto_esperado_emisiones_pct",
        "dificultad_implementacion",
        "quick_win_flag",
    ],
    "capex_proyectos": [
        "proyecto_id",
        "iniciativa_id",
        "capex_estimado",
        "opex_impacto_proxy",
        "ahorro_anual_estimado",
        "payback_estimado_meses",
        "criticidad_estrategica",
        "horizonte_implantacion",
        "riesgo_implantacion",
    ],
    "contexto_operativo": [
        "fecha",
        "turno",
        "planta_id",
        "demanda_operativa_indice",
        "mix_complejidad_indice",
        "temperatura_ambiente",
        "precio_energia_mercado_proxy",
        "tension_operativa_proxy",
    ],
    "benchmark_externo_proxy": [
        "periodo",
        "familia_proceso",
        "sec_referencia",
        "oee_referencia",
        "emisiones_referencia",
        "disponibilidad_referencia",
    ],
    "escenarios_mejora": [
        "fecha",
        "escenario",
        "precio_energia_shock",
        "presion_demanda_indice",
        "disponibilidad_personal_indice",
        "restriccion_operativa_indice",
    ],
}


def _ensure_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _save_tables(tables: dict[str, pd.DataFrame], output_dir: Path) -> None:
    for name, df in tables.items():
        ordered = df[TABLE_COLUMNS[name]].copy()
        ordered.to_csv(output_dir / f"{name}.csv", index=False)


def run_generation_pipeline(config: GenerationConfig | None = None) -> dict[str, pd.DataFrame]:
    config = config or GenerationConfig()
    _ensure_output_dir(config.output_dir)

    rng = np.random.default_rng(config.seed)

    plantas = generate_plantas(rng)
    lineas = generate_lineas_proceso(plantas, rng)
    equipos = generate_equipos(lineas, rng)

    contexto = generate_contexto_operativo(plantas, config.start_ts, config.end_ts, rng)
    benchmark = generate_benchmark_externo_proxy(lineas, config.start_ts, config.end_ts, rng)
    escenarios = generate_escenarios_mejora(config.start_ts, config.end_ts, rng)

    paradas = generate_paradas(lineas, equipos, contexto, config.start_ts, config.end_ts, rng)
    desvios = generate_desvios_proceso(lineas, equipos, contexto, config.start_ts, config.end_ts, rng)
    produccion = generate_produccion_turno(lineas, contexto, paradas, desvios, config.start_ts, config.end_ts, rng)

    consumos = generate_consumos_energia_horarios(
        plantas,
        lineas,
        equipos,
        contexto,
        paradas,
        desvios,
        config.start_ts,
        config.end_ts,
        rng,
    )
    emisiones = generate_emisiones_estimadas(consumos, produccion, rng)

    mantenimientos = generate_mantenimientos(paradas, equipos, config.start_ts, config.end_ts, rng)

    iniciativas = generate_iniciativas_mejora(lineas, equipos, rng)
    capex = generate_capex_proyectos(iniciativas, lineas, rng)

    tables = {
        "plantas": plantas,
        "lineas_proceso": lineas,
        "equipos": equipos,
        "consumos_energia_horarios": consumos,
        "produccion_turno": produccion,
        "desvios_proceso": desvios,
        "paradas": paradas,
        "mantenimientos": mantenimientos,
        "emisiones_estimadas": emisiones,
        "iniciativas_mejora": iniciativas,
        "capex_proyectos": capex,
        "contexto_operativo": contexto,
        "benchmark_externo_proxy": benchmark,
        "escenarios_mejora": escenarios,
    }

    _save_tables(tables, config.output_dir)

    validations = run_plausibility_checks(tables)
    validations.to_csv(config.output_dir / "validaciones_plausibilidad.csv", index=False)

    cardinality = build_cardinality_summary(tables)
    cardinality.to_csv(config.output_dir / "resumen_dimensiones_periodos_cardinalidades.csv", index=False)

    logic_summary = build_logic_summary_text()
    (config.output_dir / "resumen_logica_generador.md").write_text(logic_summary, encoding="utf-8")

    return tables
