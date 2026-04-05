CREATE OR REPLACE TABLE validation_checks AS
WITH checks AS (
    SELECT
        'linea_hora_sin_null_claves' AS check_name,
        COUNT(*) FILTER (WHERE planta_id IS NULL OR linea_id IS NULL OR timestamp IS NULL) AS error_count,
        'Claves obligatorias en fact_linea_hora' AS descripcion
    FROM stg_fact_linea_hora

    UNION ALL

    SELECT
        'runtime_no_supera_plan',
        COUNT(*) FILTER (WHERE actual_runtime_min > planned_runtime_min OR actual_runtime_min < 0),
        'Runtime real entre 0 y runtime planificado'
    FROM stg_fact_linea_hora

    UNION ALL

    SELECT
        'produccion_no_negativa',
        COUNT(*) FILTER (WHERE produccion_neta_ton < 0 OR produccion_bruta_ton < 0 OR produccion_obj_ton < 0),
        'Producciones no negativas'
    FROM stg_fact_linea_hora

    UNION ALL

    SELECT
        'energia_positiva',
        COUNT(*) FILTER (WHERE energia_mwh <= 0),
        'Energia positiva por observacion'
    FROM stg_fact_linea_hora

    UNION ALL

    SELECT
        'integridad_referencial_linea',
        COUNT(*) FILTER (WHERE d.linea_id IS NULL),
        'Cada linea en hechos existe en dim_linea'
    FROM stg_fact_linea_hora f
    LEFT JOIN stg_dim_linea d ON f.linea_id = d.linea_id

    UNION ALL

    SELECT
        'indice_salud_en_rango',
        COUNT(*) FILTER (WHERE indice_salud_equipo < 0 OR indice_salud_equipo > 100),
        'Indice de salud de equipo en [0,100]'
    FROM stg_fact_equipo_hora

    UNION ALL

    SELECT
        'duracion_evento_positiva',
        COUNT(*) FILTER (WHERE duracion_min <= 0),
        'Duracion de evento de parada positiva'
    FROM stg_fact_eventos_parada
)
SELECT
    check_name,
    descripcion,
    error_count,
    CASE WHEN error_count = 0 THEN 'OK' ELSE 'ERROR' END AS status
FROM checks
ORDER BY check_name;
