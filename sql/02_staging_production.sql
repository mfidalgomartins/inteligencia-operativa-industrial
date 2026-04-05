-- Dialecto: DuckDB SQL
-- Objetivo: tipado de catálogos productivos y producción por turno.

CREATE OR REPLACE VIEW stg_plants AS
SELECT
    planta_id,
    nombre_planta,
    tipo_planta,
    region,
    CAST(intensidad_energetica_base AS DOUBLE) AS intensidad_energetica_base,
    CAST(criticidad_operativa AS INTEGER) AS criticidad_operativa,
    estrategia_mejora_actual
FROM read_csv_auto('{raw_dir}/plantas.csv', HEADER=TRUE);

CREATE OR REPLACE VIEW stg_lines AS
SELECT
    linea_id,
    planta_id,
    nombre_linea,
    familia_proceso,
    CAST(capacidad_nominal_hora AS DOUBLE) AS capacidad_nominal_hora,
    CAST(oee_objetivo AS DOUBLE) AS oee_objetivo,
    CAST(intensidad_energetica_objetivo AS DOUBLE) AS intensidad_energetica_objetivo,
    CAST(criticidad_linea AS INTEGER) AS criticidad_linea
FROM read_csv_auto('{raw_dir}/lineas_proceso.csv', HEADER=TRUE);

CREATE OR REPLACE VIEW stg_equipment AS
SELECT
    equipo_id,
    linea_id,
    tipo_equipo,
    subsistema,
    CAST(fecha_instalacion AS DATE) AS fecha_instalacion,
    CAST(potencia_nominal_kw AS DOUBLE) AS potencia_nominal_kw,
    CAST(disponibilidad_objetivo AS DOUBLE) AS disponibilidad_objetivo,
    CAST(criticidad_equipo AS INTEGER) AS criticidad_equipo,
    CAST(eficiencia_nominal AS DOUBLE) AS eficiencia_nominal
FROM read_csv_auto('{raw_dir}/equipos.csv', HEADER=TRUE);

CREATE OR REPLACE VIEW stg_production_shift AS
SELECT
    CAST(fecha AS DATE) AS fecha,
    turno,
    planta_id,
    linea_id,
    producto_familia,
    CAST(unidades_producidas AS BIGINT) AS unidades_producidas,
    CAST(toneladas_producidas_proxy AS DOUBLE) AS toneladas_producidas_proxy,
    CAST(horas_planificadas AS DOUBLE) AS horas_planificadas,
    CAST(horas_operativas AS DOUBLE) AS horas_operativas,
    CAST(scrap_pct AS DOUBLE) AS scrap_pct,
    CAST(reproceso_pct AS DOUBLE) AS reproceso_pct,
    CAST(velocidad_media_proceso AS DOUBLE) AS velocidad_media_proceso,
    CAST(utilization_rate AS DOUBLE) AS utilization_rate
FROM read_csv_auto('{raw_dir}/produccion_turno.csv', HEADER=TRUE);

CREATE OR REPLACE VIEW stg_benchmark_external AS
SELECT
    periodo,
    familia_proceso,
    CAST(sec_referencia AS DOUBLE) AS sec_referencia,
    CAST(oee_referencia AS DOUBLE) AS oee_referencia,
    CAST(emisiones_referencia AS DOUBLE) AS emisiones_referencia,
    CAST(disponibilidad_referencia AS DOUBLE) AS disponibilidad_referencia
FROM read_csv_auto('{raw_dir}/benchmark_externo_proxy.csv', HEADER=TRUE);
