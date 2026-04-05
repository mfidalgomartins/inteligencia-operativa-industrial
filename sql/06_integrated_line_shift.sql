-- Dialecto: DuckDB SQL
-- Objetivo: integración línea-turno para análisis de varianza operacional.

CREATE OR REPLACE VIEW int_line_shift AS
WITH energy_shift AS (
    SELECT
        fecha,
        turno,
        planta_id,
        linea_id,
        SUM(consumo_kwh) AS consumo_kwh_shift,
        SUM(coste_energia_eur) AS coste_energia_shift_eur,
        SUM(emisiones_co2e_kg) AS emisiones_shift_kg,
        AVG(sec_kwh_ton) AS sec_shift_kwh_ton,
        SUM(n_desvios) AS n_desvios_shift,
        SUM(n_paradas) AS n_paradas_shift,
        SUM(horas_parada) AS horas_parada_shift,
        AVG(perdida_total_proxy_eur) AS perdida_proxy_media_hora
    FROM int_line_hour
    GROUP BY 1,2,3,4
)
SELECT
    p.fecha,
    p.turno,
    p.planta_id,
    p.linea_id,
    l.familia_proceso,
    p.producto_familia,
    p.unidades_producidas,
    p.toneladas_producidas_proxy,
    p.horas_planificadas,
    p.horas_operativas,
    p.scrap_pct,
    p.reproceso_pct,
    p.velocidad_media_proceso,
    p.utilization_rate,
    COALESCE(e.consumo_kwh_shift, 0) AS consumo_kwh_shift,
    COALESCE(e.coste_energia_shift_eur, 0) AS coste_energia_shift_eur,
    COALESCE(e.emisiones_shift_kg, 0) AS emisiones_shift_kg,
    COALESCE(e.sec_shift_kwh_ton, 0) AS sec_shift_kwh_ton,
    COALESCE(e.n_desvios_shift, 0) AS n_desvios_shift,
    COALESCE(e.n_paradas_shift, 0) AS n_paradas_shift,
    COALESCE(e.horas_parada_shift, 0) AS horas_parada_shift,
    COALESCE(e.perdida_proxy_media_hora * 8.0, 0) AS perdida_proxy_shift_eur,
    CASE WHEN p.horas_planificadas > 0 THEN p.horas_operativas / p.horas_planificadas ELSE NULL END AS availability_shift,
    (
        0.45 * COALESCE(e.sec_shift_kwh_ton, 0)
        + 0.25 * p.scrap_pct
        + 0.20 * COALESCE(e.horas_parada_shift, 0)
        + 0.10 * COALESCE(e.n_desvios_shift, 0)
    ) AS shift_variance_proxy
FROM stg_production_shift p
LEFT JOIN energy_shift e
  ON p.fecha = e.fecha AND p.turno = e.turno AND p.linea_id = e.linea_id
LEFT JOIN stg_lines l
  ON p.linea_id = l.linea_id;
