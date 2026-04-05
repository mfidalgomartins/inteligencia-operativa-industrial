-- Dialecto: DuckDB SQL
-- Objetivo: integrar consumo, paradas, mantenimiento y desvíos por equipo-día.

CREATE OR REPLACE VIEW int_equipment_day AS
WITH energy_equipment_day AS (
    SELECT
        CAST(eh.timestamp AS DATE) AS fecha,
        eh.planta_id,
        eh.linea_id,
        eh.equipo_id,
        eq.tipo_equipo,
        eq.subsistema,
        eq.criticidad_equipo,
        eq.eficiencia_nominal,
        SUM(eh.consumo_kwh) AS consumo_kwh,
        AVG(eh.factor_carga) AS factor_carga_prom,
        SUM(eh.coste_energia_proxy) AS coste_energia_eur,
        AVG(eh.demanda_kw) AS demanda_kw_media
    FROM stg_energy_hourly eh
    JOIN stg_equipment eq
      ON eh.equipo_id = eq.equipo_id
    GROUP BY 1,2,3,4,5,6,7,8
),
stops_equipment_day AS (
    SELECT
        CAST(fecha_inicio AS DATE) AS fecha,
        planta_id,
        linea_id,
        equipo_id,
        COUNT(*) AS n_paradas,
        SUM(duracion_horas) AS horas_parada,
        SUM(CASE WHEN planificada_flag = 0 THEN 1 ELSE 0 END) AS n_paradas_no_plan
    FROM stg_stops
    GROUP BY 1,2,3,4
),
maint_equipment_day AS (
    SELECT
        CAST(fecha_inicio AS DATE) AS fecha,
        planta_id,
        linea_id,
        equipo_id,
        COUNT(*) AS n_mantenimientos,
        SUM(horas_mantenimiento) AS horas_mantenimiento,
        SUM(coste_mantenimiento_proxy) AS coste_mantenimiento_eur
    FROM stg_maintenance
    GROUP BY 1,2,3,4
),
dev_equipment_day AS (
    SELECT
        CAST(timestamp AS DATE) AS fecha,
        planta_id,
        linea_id,
        equipo_id,
        COUNT(*) AS n_desvios,
        AVG(ABS(desviacion_pct)) AS desviacion_abs_media,
        SUM(impacto_en_calidad_proxy) AS impacto_calidad_sum
    FROM stg_process_deviations
    GROUP BY 1,2,3,4
)
SELECT
    eed.fecha,
    eed.planta_id,
    eed.linea_id,
    eed.equipo_id,
    eed.tipo_equipo,
    eed.subsistema,
    eed.criticidad_equipo,
    eed.eficiencia_nominal,
    eed.consumo_kwh,
    eed.factor_carga_prom,
    eed.coste_energia_eur,
    eed.demanda_kw_media,
    COALESCE(se.n_paradas, 0) AS n_paradas,
    COALESCE(se.horas_parada, 0) AS horas_parada,
    COALESCE(se.n_paradas_no_plan, 0) AS n_paradas_no_plan,
    COALESCE(me.n_mantenimientos, 0) AS n_mantenimientos,
    COALESCE(me.horas_mantenimiento, 0) AS horas_mantenimiento,
    COALESCE(me.coste_mantenimiento_eur, 0) AS coste_mantenimiento_eur,
    COALESCE(de.n_desvios, 0) AS n_desvios,
    COALESCE(de.desviacion_abs_media, 0) AS desviacion_abs_media,
    COALESCE(de.impacto_calidad_sum, 0) AS impacto_calidad_sum,
    CASE WHEN 24 > 0 THEN 1.0 - COALESCE(se.horas_parada, 0) / 24.0 ELSE NULL END AS disponibilidad_proxy,
    CASE
        WHEN AVG(eed.consumo_kwh) OVER (PARTITION BY eed.tipo_equipo) > 0
        THEN eed.consumo_kwh / AVG(eed.consumo_kwh) OVER (PARTITION BY eed.tipo_equipo)
        ELSE NULL
    END AS consumo_relativo_tipo,
    (
        0.35 * COALESCE(se.horas_parada, 0)
        + 0.25 * COALESCE(de.n_desvios, 0)
        + 0.20 * (1.0 - eed.eficiencia_nominal) * 10
        + 0.20 * eed.criticidad_equipo
    ) AS riesgo_equipo_proxy
FROM energy_equipment_day eed
LEFT JOIN stops_equipment_day se
  ON eed.fecha = se.fecha AND eed.equipo_id = se.equipo_id
LEFT JOIN maint_equipment_day me
  ON eed.fecha = me.fecha AND eed.equipo_id = me.equipo_id
LEFT JOIN dev_equipment_day de
  ON eed.fecha = de.fecha AND eed.equipo_id = de.equipo_id;
