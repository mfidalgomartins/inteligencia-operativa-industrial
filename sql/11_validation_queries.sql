-- Dialecto: DuckDB SQL
-- Objetivo: validaciones SQL para robustez de capa analítica.

CREATE OR REPLACE TABLE validation_sql_checks AS
WITH checks AS (
    SELECT 'row_count_int_line_hour' AS check_name,
           CASE WHEN COUNT(*) > 0 THEN 'OK' ELSE 'ERROR' END AS status,
           CAST(COUNT(*) AS VARCHAR) AS observed,
           '>0 filas' AS expected,
           'Cobertura de integración línea-hora' AS detail
    FROM int_line_hour

    UNION ALL

    SELECT 'sec_rango_mart_line_day',
           CASE WHEN COUNT(*) FILTER (WHERE sec_kwh_ton < 0 OR sec_kwh_ton > 5000) = 0 THEN 'OK' ELSE 'ERROR' END,
           CAST(COUNT(*) FILTER (WHERE sec_kwh_ton < 0 OR sec_kwh_ton > 5000) AS VARCHAR),
           '0 fuera de rango',
           'SEC dentro de rango razonable'
    FROM mart_line_day

    UNION ALL

    SELECT 'oee_rango_mart_line_day',
           CASE WHEN COUNT(*) FILTER (WHERE oee_synthetic < 0 OR oee_synthetic > 1.1) = 0 THEN 'OK' ELSE 'ERROR' END,
           CAST(COUNT(*) FILTER (WHERE oee_synthetic < 0 OR oee_synthetic > 1.1) AS VARCHAR),
           '0 fuera de rango',
           'OEE sintético dentro de rango'
    FROM mart_line_day

    UNION ALL

    SELECT 'energy_cost_consistency',
           CASE WHEN COUNT(*) FILTER (WHERE consumo_kwh < 0 OR coste_energia_eur < 0) = 0 THEN 'OK' ELSE 'ERROR' END,
           CAST(COUNT(*) FILTER (WHERE consumo_kwh < 0 OR coste_energia_eur < 0) AS VARCHAR),
           '0 negativos',
           'Consumo y coste no negativos en equipo-día'
    FROM mart_equipment_day

    UNION ALL

    SELECT 'capex_payback_positive',
           CASE WHEN COUNT(*) FILTER (WHERE payback_estimado_meses <= 0) = 0 THEN 'OK' ELSE 'ERROR' END,
           CAST(COUNT(*) FILTER (WHERE payback_estimado_meses <= 0) AS VARCHAR),
           '0 no positivos',
           'Payback válido en value pool'
    FROM vw_improvement_value_pool

    UNION ALL

    SELECT 'null_candidate_keys_int_line_hour',
           CASE WHEN COUNT(*) FILTER (WHERE fecha IS NULL OR linea_id IS NULL) = 0 THEN 'OK' ELSE 'ERROR' END,
           CAST(COUNT(*) FILTER (WHERE fecha IS NULL OR linea_id IS NULL) AS VARCHAR),
           '0 nulls',
           'Claves obligatorias no nulas en integración'
    FROM int_line_hour

    UNION ALL

    SELECT 'emissions_consistency',
           CASE WHEN COUNT(*) FILTER (WHERE emissions_kg < 0) = 0 THEN 'OK' ELSE 'ERROR' END,
           CAST(COUNT(*) FILTER (WHERE emissions_kg < 0) AS VARCHAR),
           '0 negativos',
           'Emisiones no negativas en mart semanal'
    FROM mart_plant_week
)
SELECT
    check_name,
    status,
    observed,
    expected,
    detail
FROM checks;
