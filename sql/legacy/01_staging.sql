CREATE OR REPLACE TABLE stg_dim_planta AS
SELECT
    planta_id,
    nombre_planta,
    region,
    CAST(factor_emision_tco2_mwh AS DOUBLE) AS factor_emision_tco2_mwh,
    CAST(coste_energia_base_eur_mwh AS DOUBLE) AS coste_energia_base_eur_mwh
FROM read_csv_auto('{raw_dir}/dim_planta.csv', HEADER = TRUE);

CREATE OR REPLACE TABLE stg_dim_linea AS
SELECT
    linea_id,
    planta_id,
    proceso,
    CAST(anio_instalacion AS INTEGER) AS anio_instalacion,
    CAST(capacidad_nominal_ton_h AS DOUBLE) AS capacidad_nominal_ton_h,
    CAST(intensidad_energetica_base_mwh_ton AS DOUBLE) AS intensidad_energetica_base_mwh_ton,
    CAST(indice_mantenibilidad AS DOUBLE) AS indice_mantenibilidad,
    CAST(criticidad_linea AS INTEGER) AS criticidad_linea
FROM read_csv_auto('{raw_dir}/dim_linea.csv', HEADER = TRUE);

CREATE OR REPLACE TABLE stg_dim_equipo AS
SELECT
    equipo_id,
    linea_id,
    planta_id,
    tipo_equipo,
    CAST(factor_consumo_energetico AS DOUBLE) AS factor_consumo_energetico,
    CAST(factor_parada AS DOUBLE) AS factor_parada,
    CAST(indice_criticidad AS INTEGER) AS indice_criticidad
FROM read_csv_auto('{raw_dir}/dim_equipo.csv', HEADER = TRUE);

CREATE OR REPLACE TABLE stg_dim_turno AS
SELECT
    turno,
    descripcion,
    CAST(hora_inicio AS INTEGER) AS hora_inicio,
    CAST(hora_fin AS INTEGER) AS hora_fin
FROM read_csv_auto('{raw_dir}/dim_turno.csv', HEADER = TRUE);

CREATE OR REPLACE TABLE stg_fact_linea_hora AS
SELECT
    CAST(timestamp AS TIMESTAMP) AS timestamp,
    planta_id,
    linea_id,
    proceso,
    turno,
    CAST(planned_runtime_min AS DOUBLE) AS planned_runtime_min,
    CAST(actual_runtime_min AS DOUBLE) AS actual_runtime_min,
    CAST(downtime_min AS DOUBLE) AS downtime_min,
    CAST(produccion_obj_ton AS DOUBLE) AS produccion_obj_ton,
    CAST(produccion_bruta_ton AS DOUBLE) AS produccion_bruta_ton,
    CAST(produccion_neta_ton AS DOUBLE) AS produccion_neta_ton,
    CAST(scrap_ton AS DOUBLE) AS scrap_ton,
    CAST(energia_mwh AS DOUBLE) AS energia_mwh,
    CAST(intensidad_energetica_mwh_ton AS DOUBLE) AS intensidad_energetica_mwh_ton,
    CAST(coste_energia_eur AS DOUBLE) AS coste_energia_eur,
    CAST(co2_ton AS DOUBLE) AS co2_ton,
    CAST(temperatura_ambiente_c AS DOUBLE) AS temperatura_ambiente_c,
    CAST(temperatura_proceso_c AS DOUBLE) AS temperatura_proceso_c,
    CAST(presion_bar AS DOUBLE) AS presion_bar,
    CAST(vibracion_global_mm_s AS DOUBLE) AS vibracion_global_mm_s,
    CAST(oee AS DOUBLE) AS oee
FROM read_csv_auto('{raw_dir}/fact_linea_hora.csv', HEADER = TRUE);

CREATE OR REPLACE TABLE stg_fact_equipo_hora AS
SELECT
    CAST(timestamp AS TIMESTAMP) AS timestamp,
    planta_id,
    linea_id,
    equipo_id,
    tipo_equipo,
    turno,
    CAST(energia_mwh AS DOUBLE) AS energia_mwh,
    CAST(downtime_min AS DOUBLE) AS downtime_min,
    CAST(carga_pct AS DOUBLE) AS carga_pct,
    CAST(temperatura_equipo_c AS DOUBLE) AS temperatura_equipo_c,
    CAST(vibracion_equipo_mm_s AS DOUBLE) AS vibracion_equipo_mm_s,
    CAST(indice_salud_equipo AS DOUBLE) AS indice_salud_equipo
FROM read_csv_auto('{raw_dir}/fact_equipo_hora.csv', HEADER = TRUE);

CREATE OR REPLACE TABLE stg_fact_eventos_parada AS
SELECT
    evento_id,
    planta_id,
    linea_id,
    equipo_id,
    categoria_parada,
    causa_raiz,
    CAST(timestamp_inicio AS TIMESTAMP) AS timestamp_inicio,
    CAST(timestamp_fin AS TIMESTAMP) AS timestamp_fin,
    CAST(duracion_min AS DOUBLE) AS duracion_min,
    CAST(criticidad_evento AS INTEGER) AS criticidad_evento,
    CAST(impacto_seguridad AS INTEGER) AS impacto_seguridad
FROM read_csv_auto('{raw_dir}/fact_eventos_parada.csv', HEADER = TRUE);

CREATE OR REPLACE TABLE stg_fact_proceso_hora AS
SELECT
    CAST(timestamp AS TIMESTAMP) AS timestamp,
    planta_id,
    linea_id,
    turno,
    CAST(temperatura_proceso_c AS DOUBLE) AS temperatura_proceso_c,
    CAST(presion_bar AS DOUBLE) AS presion_bar,
    CAST(vibracion_global_mm_s AS DOUBLE) AS vibracion_global_mm_s,
    CAST(indice_desviacion_proceso AS DOUBLE) AS indice_desviacion_proceso,
    CAST(alarma_proceso AS INTEGER) AS alarma_proceso
FROM read_csv_auto('{raw_dir}/fact_proceso_hora.csv', HEADER = TRUE);
