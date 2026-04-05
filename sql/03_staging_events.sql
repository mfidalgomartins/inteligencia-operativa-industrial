-- Dialecto: DuckDB SQL
-- Objetivo: tipado de eventos operativos, mantenimiento e iniciativas.

CREATE OR REPLACE VIEW stg_process_deviations AS
SELECT
    desvio_id,
    CAST(timestamp AS TIMESTAMP) AS timestamp,
    planta_id,
    linea_id,
    equipo_id,
    variable_proceso,
    CAST(valor_observado AS DOUBLE) AS valor_observado,
    CAST(valor_objetivo AS DOUBLE) AS valor_objetivo,
    CAST(desviacion_pct AS DOUBLE) AS desviacion_pct,
    severidad_desvio,
    CAST(duracion_min AS DOUBLE) AS duracion_min,
    causa_probable_proxy,
    CAST(impacto_en_calidad_proxy AS DOUBLE) AS impacto_en_calidad_proxy
FROM read_csv_auto('{raw_dir}/desvios_proceso.csv', HEADER=TRUE);

CREATE OR REPLACE VIEW stg_stops AS
SELECT
    parada_id,
    CAST(fecha_inicio AS TIMESTAMP) AS fecha_inicio,
    CAST(fecha_fin AS TIMESTAMP) AS fecha_fin,
    planta_id,
    linea_id,
    equipo_id,
    tipo_parada,
    CAST(planificada_flag AS INTEGER) AS planificada_flag,
    causa_parada,
    CAST(duracion_horas AS DOUBLE) AS duracion_horas,
    CAST(impacto_produccion_proxy AS DOUBLE) AS impacto_produccion_proxy,
    CAST(impacto_energia_proxy AS DOUBLE) AS impacto_energia_proxy
FROM read_csv_auto('{raw_dir}/paradas.csv', HEADER=TRUE);

CREATE OR REPLACE VIEW stg_maintenance AS
SELECT
    mantenimiento_id,
    CAST(fecha_inicio AS TIMESTAMP) AS fecha_inicio,
    CAST(fecha_fin AS TIMESTAMP) AS fecha_fin,
    planta_id,
    linea_id,
    equipo_id,
    tipo_mantenimiento,
    CAST(programado_flag AS INTEGER) AS programado_flag,
    CAST(correctivo_flag AS INTEGER) AS correctivo_flag,
    CAST(horas_mantenimiento AS DOUBLE) AS horas_mantenimiento,
    CAST(coste_mantenimiento_proxy AS DOUBLE) AS coste_mantenimiento_proxy,
    resultado_mantenimiento
FROM read_csv_auto('{raw_dir}/mantenimientos.csv', HEADER=TRUE);

CREATE OR REPLACE VIEW stg_initiatives AS
SELECT
    iniciativa_id,
    planta_id,
    linea_id,
    NULLIF(equipo_id, '') AS equipo_id,
    categoria_iniciativa,
    tipo_iniciativa,
    descripcion_resumida,
    CAST(impacto_esperado_energia_pct AS DOUBLE) AS impacto_esperado_energia_pct,
    CAST(impacto_esperado_oee_pct AS DOUBLE) AS impacto_esperado_oee_pct,
    CAST(impacto_esperado_emisiones_pct AS DOUBLE) AS impacto_esperado_emisiones_pct,
    CAST(dificultad_implementacion AS INTEGER) AS dificultad_implementacion,
    CAST(quick_win_flag AS INTEGER) AS quick_win_flag
FROM read_csv_auto('{raw_dir}/iniciativas_mejora.csv', HEADER=TRUE);

CREATE OR REPLACE VIEW stg_capex AS
SELECT
    proyecto_id,
    iniciativa_id,
    CAST(capex_estimado AS DOUBLE) AS capex_estimado,
    CAST(opex_impacto_proxy AS DOUBLE) AS opex_impacto_proxy,
    CAST(ahorro_anual_estimado AS DOUBLE) AS ahorro_anual_estimado,
    CAST(payback_estimado_meses AS DOUBLE) AS payback_estimado_meses,
    CAST(criticidad_estrategica AS INTEGER) AS criticidad_estrategica,
    horizonte_implantacion,
    riesgo_implantacion
FROM read_csv_auto('{raw_dir}/capex_proyectos.csv', HEADER=TRUE);

CREATE OR REPLACE VIEW stg_scenarios AS
SELECT
    CAST(fecha AS DATE) AS fecha,
    escenario,
    CAST(precio_energia_shock AS DOUBLE) AS precio_energia_shock,
    CAST(presion_demanda_indice AS DOUBLE) AS presion_demanda_indice,
    CAST(disponibilidad_personal_indice AS DOUBLE) AS disponibilidad_personal_indice,
    CAST(restriccion_operativa_indice AS DOUBLE) AS restriccion_operativa_indice
FROM read_csv_auto('{raw_dir}/escenarios_mejora.csv', HEADER=TRUE);
