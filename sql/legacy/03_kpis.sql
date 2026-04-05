CREATE OR REPLACE TABLE kpi_resumen AS
SELECT 'OEE Global (%)' AS indicador, ROUND(AVG(oee_pct), 2) AS valor
FROM mart_linea_turno_dia
UNION ALL
SELECT 'Disponibilidad Global (%)', ROUND(AVG(disponibilidad_pct), 2)
FROM mart_linea_turno_dia
UNION ALL
SELECT 'Intensidad Energetica (kWh/ton)', ROUND(AVG(intensidad_energetica_kwh_ton), 2)
FROM mart_linea_turno_dia
UNION ALL
SELECT 'Intensidad Emisiones (kgCO2/ton)', ROUND(AVG(intensidad_emisiones_kgco2_ton), 2)
FROM mart_linea_turno_dia
UNION ALL
SELECT 'Scrap Global (%)', ROUND(AVG(scrap_pct), 2)
FROM mart_linea_turno_dia
UNION ALL
SELECT 'Perdida Economica Total (EUR)', ROUND(SUM(perdida_total_eur), 2)
FROM mart_perdidas_economicas_linea;

CREATE OR REPLACE TABLE kpi_ranking_lineas AS
SELECT
    b.planta_id,
    b.linea_id,
    b.proceso,
    ROUND(b.intensidad_energia_media_kwh_ton, 2) AS intensidad_energia_media_kwh_ton,
    ROUND(b.oee_medio_pct, 2) AS oee_medio_pct,
    ROUND(p.perdida_total_eur, 2) AS perdida_total_eur,
    ROUND(
        0.45 * (100 - b.oee_medio_pct)
        + 0.35 * (b.percentil_energia_proceso)
        + 0.20 * (CASE WHEN p.perdida_total_eur > 0
            THEN 100 * p.perdida_total_eur / MAX(p.perdida_total_eur) OVER ()
            ELSE 0 END),
        2
    ) AS score_prioridad_mejora
FROM mart_benchmark_energia b
JOIN mart_perdidas_economicas_linea p USING(planta_id, linea_id, proceso)
ORDER BY score_prioridad_mejora DESC;

CREATE OR REPLACE TABLE kpi_turno AS
SELECT
    turno,
    ROUND(AVG(oee_pct), 2) AS oee_promedio_pct,
    ROUND(AVG(intensidad_energetica_kwh_ton), 2) AS intensidad_energetica_kwh_ton,
    ROUND(AVG(disponibilidad_pct), 2) AS disponibilidad_pct,
    ROUND(AVG(scrap_pct), 2) AS scrap_pct,
    ROUND(SUM(coste_energia_eur), 2) AS coste_energia_total_eur
FROM mart_linea_turno_dia
GROUP BY 1
ORDER BY 1;
