-- Dialecto: DuckDB SQL
-- Objetivo: integrar energía, emisiones, producción, desvíos y paradas a nivel línea-hora.

CREATE OR REPLACE VIEW int_line_hour AS
WITH energy_line_hour AS (
    SELECT
        DATE_TRUNC('hour', eh.timestamp) AS timestamp_hour,
        eh.planta_id,
        eh.linea_id,
        SUM(eh.consumo_kwh) AS consumo_kwh,
        SUM(eh.coste_energia_proxy) AS coste_energia_eur,
        AVG(eh.factor_carga) AS factor_carga_prom,
        AVG(eh.precio_energia_proxy) AS precio_energia_prom
    FROM stg_energy_hourly eh
    GROUP BY 1,2,3
),
emissions_line_hour AS (
    SELECT
        DATE_TRUNC('hour', em.timestamp) AS timestamp_hour,
        em.planta_id,
        em.linea_id,
        SUM(em.emisiones_co2e_kg) AS emisiones_co2e_kg,
        AVG(em.factor_emision_proxy) AS factor_emision_prom
    FROM stg_emissions_hourly em
    GROUP BY 1,2,3
),
deviations_line_hour AS (
    SELECT
        DATE_TRUNC('hour', d.timestamp) AS timestamp_hour,
        d.planta_id,
        d.linea_id,
        COUNT(*) AS n_desvios,
        AVG(ABS(d.desviacion_pct)) AS desv_abs_media_pct,
        SUM(d.impacto_en_calidad_proxy) AS impacto_calidad_sum
    FROM stg_process_deviations d
    GROUP BY 1,2,3
),
stops_line_hour AS (
    SELECT
        DATE_TRUNC('hour', s.fecha_inicio) AS timestamp_hour,
        s.planta_id,
        s.linea_id,
        COUNT(*) AS n_paradas,
        SUM(s.duracion_horas) AS horas_parada,
        SUM(CASE WHEN s.planificada_flag = 0 THEN s.duracion_horas ELSE 0 END) AS horas_parada_no_plan
    FROM stg_stops s
    GROUP BY 1,2,3
),
base AS (
    SELECT
        e.timestamp_hour,
        e.planta_id,
        e.linea_id,
        e.consumo_kwh,
        e.coste_energia_eur,
        e.factor_carga_prom,
        e.precio_energia_prom,
        COALESCE(em.emisiones_co2e_kg, 0) AS emisiones_co2e_kg,
        COALESCE(em.factor_emision_prom, 0) AS factor_emision_prom,
        COALESCE(d.n_desvios, 0) AS n_desvios,
        COALESCE(d.desv_abs_media_pct, 0) AS desv_abs_media_pct,
        COALESCE(d.impacto_calidad_sum, 0) AS impacto_calidad_sum,
        COALESCE(s.n_paradas, 0) AS n_paradas,
        COALESCE(s.horas_parada, 0) AS horas_parada,
        COALESCE(s.horas_parada_no_plan, 0) AS horas_parada_no_plan
    FROM energy_line_hour e
    LEFT JOIN emissions_line_hour em
        ON e.timestamp_hour = em.timestamp_hour
       AND e.planta_id = em.planta_id
       AND e.linea_id = em.linea_id
    LEFT JOIN deviations_line_hour d
        ON e.timestamp_hour = d.timestamp_hour
       AND e.planta_id = d.planta_id
       AND e.linea_id = d.linea_id
    LEFT JOIN stops_line_hour s
        ON e.timestamp_hour = s.timestamp_hour
       AND e.planta_id = s.planta_id
       AND e.linea_id = s.linea_id
),
base_with_shift AS (
    SELECT
        b.*,
        CAST(b.timestamp_hour AS DATE) AS fecha,
        CASE
            WHEN EXTRACT(HOUR FROM b.timestamp_hour) BETWEEN 6 AND 13 THEN 'A'
            WHEN EXTRACT(HOUR FROM b.timestamp_hour) BETWEEN 14 AND 21 THEN 'B'
            ELSE 'C'
        END AS turno
    FROM base b
)
SELECT
    bws.timestamp_hour,
    bws.fecha,
    bws.turno,
    bws.planta_id,
    bws.linea_id,
    l.familia_proceso,
    bws.consumo_kwh,
    bws.coste_energia_eur,
    bws.factor_carga_prom,
    bws.precio_energia_prom,
    bws.emisiones_co2e_kg,
    bws.factor_emision_prom,
    bws.n_desvios,
    bws.desv_abs_media_pct,
    bws.impacto_calidad_sum,
    bws.n_paradas,
    bws.horas_parada,
    bws.horas_parada_no_plan,
    p.toneladas_producidas_proxy / 8.0 AS toneladas_equivalentes_hora,
    p.unidades_producidas / 8.0 AS unidades_equivalentes_hora,
    p.utilization_rate,
    p.scrap_pct,
    p.reproceso_pct,
    p.horas_operativas,
    p.horas_planificadas,
    CASE
        -- Se evita SEC explosivo cuando hay producción residual en el turno.
        WHEN (p.toneladas_producidas_proxy / 8.0) >= 0.50 THEN bws.consumo_kwh / (p.toneladas_producidas_proxy / 8.0)
        ELSE NULL
    END AS sec_kwh_ton,
    CASE
        WHEN p.unidades_producidas > 0 THEN bws.coste_energia_eur / (p.unidades_producidas / 8.0)
        ELSE NULL
    END AS coste_energia_por_unidad,
    (
        COALESCE(bws.coste_energia_eur, 0)
        + COALESCE((p.toneladas_producidas_proxy / 8.0) * (p.scrap_pct / 100.0) * 210.0, 0)
        + COALESCE(bws.horas_parada * l.capacidad_nominal_hora * 480.0, 0)
        + COALESCE((bws.emisiones_co2e_kg / 1000.0) * 85.0, 0)
    ) AS perdida_total_proxy_eur
FROM base_with_shift bws
LEFT JOIN stg_lines l
    ON bws.linea_id = l.linea_id
LEFT JOIN stg_production_shift p
    ON bws.fecha = p.fecha
   AND bws.turno = p.turno
   AND bws.linea_id = p.linea_id;
