CREATE OR REPLACE TABLE mart_linea_turno_dia AS
WITH base AS (
    SELECT
        DATE_TRUNC('day', f.timestamp) AS fecha,
        f.planta_id,
        f.linea_id,
        f.proceso,
        f.turno,
        SUM(f.planned_runtime_min) AS runtime_plan_min,
        SUM(f.actual_runtime_min) AS runtime_real_min,
        SUM(f.downtime_min) AS downtime_min,
        SUM(f.produccion_obj_ton) AS produccion_obj_ton,
        SUM(f.produccion_bruta_ton) AS produccion_bruta_ton,
        SUM(f.produccion_neta_ton) AS produccion_neta_ton,
        SUM(f.scrap_ton) AS scrap_ton,
        SUM(f.energia_mwh) AS energia_mwh,
        SUM(f.coste_energia_eur) AS coste_energia_eur,
        SUM(f.co2_ton) AS co2_ton,
        AVG(f.temperatura_ambiente_c) AS temperatura_ambiente_c,
        AVG(f.temperatura_proceso_c) AS temperatura_proceso_c,
        AVG(f.presion_bar) AS presion_bar,
        AVG(f.vibracion_global_mm_s) AS vibracion_global_mm_s,
        AVG(f.oee) AS oee_promedio
    FROM stg_fact_linea_hora f
    GROUP BY 1,2,3,4,5
)
SELECT
    b.*,
    ROUND(100.0 * b.runtime_real_min / NULLIF(b.runtime_plan_min, 0), 3) AS disponibilidad_pct,
    ROUND(
        100.0 * b.produccion_bruta_ton /
        NULLIF((d.capacidad_nominal_ton_h * b.runtime_real_min / 60.0), 0),
        3
    ) AS performance_pct,
    ROUND(100.0 * b.produccion_neta_ton / NULLIF(b.produccion_bruta_ton, 0), 3) AS calidad_pct,
    ROUND(
        (
            (100.0 * b.runtime_real_min / NULLIF(b.runtime_plan_min, 0))
            * (100.0 * b.produccion_bruta_ton / NULLIF((d.capacidad_nominal_ton_h * b.runtime_real_min / 60.0), 0))
            * (100.0 * b.produccion_neta_ton / NULLIF(b.produccion_bruta_ton, 0))
        ) / 10000.0,
        3
    ) AS oee_pct,
    ROUND(1000.0 * b.energia_mwh / NULLIF(b.produccion_neta_ton, 0), 3) AS intensidad_energetica_kwh_ton,
    ROUND(100.0 * b.scrap_ton / NULLIF(b.produccion_bruta_ton, 0), 3) AS scrap_pct,
    ROUND(1000.0 * b.co2_ton / NULLIF(b.produccion_neta_ton, 0), 3) AS intensidad_emisiones_kgco2_ton
FROM base b
LEFT JOIN stg_dim_linea d USING(linea_id);

CREATE OR REPLACE TABLE mart_equipo_confiabilidad AS
WITH eventos AS (
    SELECT
        equipo_id,
        COUNT(*) AS total_eventos,
        AVG(duracion_min) / 60.0 AS mttr_horas,
        SUM(CASE WHEN impacto_seguridad = 1 THEN 1 ELSE 0 END) AS eventos_con_impacto_seguridad
    FROM stg_fact_eventos_parada
    GROUP BY 1
),
horas_equipo AS (
    SELECT
        e.equipo_id,
        e.linea_id,
        e.planta_id,
        e.tipo_equipo,
        SUM(60.0 - f.downtime_min) / 60.0 AS horas_operativas,
        SUM(f.downtime_min) / 60.0 AS horas_parada,
        AVG(f.indice_salud_equipo) AS indice_salud_promedio,
        AVG(f.vibracion_equipo_mm_s) AS vibracion_promedio_mm_s,
        AVG(f.temperatura_equipo_c) AS temperatura_promedio_c
    FROM stg_fact_equipo_hora f
    JOIN stg_dim_equipo e USING(equipo_id)
    GROUP BY 1,2,3,4
)
SELECT
    h.*,
    COALESCE(ev.total_eventos, 0) AS total_eventos,
    COALESCE(ev.mttr_horas, 0) AS mttr_horas,
    ROUND(h.horas_operativas / NULLIF(COALESCE(ev.total_eventos, 0), 0), 3) AS mtbf_horas,
    ROUND(100.0 * h.horas_operativas / NULLIF(h.horas_operativas + h.horas_parada, 0), 3) AS disponibilidad_equipo_pct,
    COALESCE(ev.eventos_con_impacto_seguridad, 0) AS eventos_con_impacto_seguridad
FROM horas_equipo h
LEFT JOIN eventos ev USING(equipo_id);

CREATE OR REPLACE TABLE mart_benchmark_energia AS
WITH base AS (
    SELECT
        m.planta_id,
        m.linea_id,
        m.proceso,
        AVG(m.intensidad_energetica_kwh_ton) AS intensidad_energia_media_kwh_ton,
        AVG(m.intensidad_emisiones_kgco2_ton) AS intensidad_emisiones_media_kgco2_ton,
        AVG(m.oee_pct) AS oee_medio_pct,
        SUM(m.produccion_neta_ton) AS produccion_neta_total_ton,
        SUM(m.coste_energia_eur) AS coste_energia_total_eur
    FROM mart_linea_turno_dia m
    GROUP BY 1,2,3
),
bench AS (
    SELECT
        b.*,
        QUANTILE_CONT(b.intensidad_energia_media_kwh_ton, 0.25)
            OVER(PARTITION BY b.proceso) AS benchmark_energia_kwh_ton,
        QUANTILE_CONT(b.intensidad_emisiones_media_kgco2_ton, 0.25)
            OVER(PARTITION BY b.proceso) AS benchmark_emisiones_kgco2_ton
    FROM base b
)
SELECT
    *,
    ROUND(intensidad_energia_media_kwh_ton - benchmark_energia_kwh_ton, 3) AS brecha_energia_kwh_ton,
    ROUND(intensidad_emisiones_media_kgco2_ton - benchmark_emisiones_kgco2_ton, 3) AS brecha_emisiones_kgco2_ton,
    ROUND(PERCENT_RANK() OVER(PARTITION BY proceso ORDER BY intensidad_energia_media_kwh_ton) * 100, 2) AS percentil_energia_proceso
FROM bench;

CREATE OR REPLACE TABLE mart_perdidas_economicas_linea AS
SELECT
    f.planta_id,
    f.linea_id,
    d.proceso,
    ROUND(SUM(GREATEST(f.produccion_obj_ton - f.produccion_neta_ton, 0) * 480.0), 2) AS perdida_no_producida_eur,
    ROUND(SUM(f.scrap_ton * 210.0), 2) AS perdida_scrap_eur,
    ROUND((SUM(f.downtime_min) / 60.0) * AVG(d.capacidad_nominal_ton_h) * 480.0, 2) AS perdida_paradas_eur,
    ROUND(SUM(f.co2_ton * 85.0), 2) AS coste_emisiones_proxy_eur,
    ROUND(SUM(f.coste_energia_eur), 2) AS coste_energia_eur,
    ROUND(
        SUM(GREATEST(f.produccion_obj_ton - f.produccion_neta_ton, 0) * 480.0)
        + SUM(f.scrap_ton * 210.0)
        + (SUM(f.downtime_min) / 60.0) * AVG(d.capacidad_nominal_ton_h) * 480.0
        + SUM(f.co2_ton * 85.0),
        2
    ) AS perdida_total_eur
FROM stg_fact_linea_hora f
JOIN stg_dim_linea d USING(linea_id)
GROUP BY 1,2,3;
