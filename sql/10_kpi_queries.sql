-- Dialecto: DuckDB SQL
-- Objetivo: vistas de causa raíz, value pool e indicadores priorizados.

CREATE OR REPLACE VIEW vw_loss_root_cause_summary AS
WITH cause_base AS (
    SELECT
        DATE_TRUNC('month', s.fecha_inicio) AS periodo,
        s.planta_id,
        s.linea_id,
        s.causa_parada,
        COUNT(*) AS n_eventos,
        SUM(s.duracion_horas) AS horas_parada,
        SUM(s.impacto_produccion_proxy) AS impacto_produccion_proxy,
        SUM(s.impacto_energia_proxy) AS impacto_energia_proxy
    FROM stg_stops s
    GROUP BY 1,2,3,4
),
line_emissions AS (
    SELECT
        DATE_TRUNC('month', CAST(timestamp AS TIMESTAMP)) AS periodo,
        planta_id,
        linea_id,
        SUM(emisiones_co2e_kg) AS emisiones_kg
    FROM stg_emissions_hourly
    GROUP BY 1,2,3
)
SELECT
    cb.periodo,
    cb.planta_id,
    cb.linea_id,
    cb.causa_parada,
    cb.n_eventos,
    cb.horas_parada,
    cb.impacto_produccion_proxy,
    cb.impacto_energia_proxy,
    COALESCE(le.emisiones_kg, 0) AS emisiones_asociadas_kg,
    (
        0.50 * cb.impacto_produccion_proxy
        + 0.30 * cb.impacto_energia_proxy
        + 0.20 * cb.horas_parada * 100
    ) AS loss_root_cause_score,
    ROW_NUMBER() OVER (PARTITION BY cb.periodo, cb.planta_id ORDER BY (
        0.50 * cb.impacto_produccion_proxy
        + 0.30 * cb.impacto_energia_proxy
        + 0.20 * cb.horas_parada * 100
    ) DESC) AS ranking_causa
FROM cause_base cb
LEFT JOIN line_emissions le
  ON cb.periodo = le.periodo
 AND cb.planta_id = le.planta_id
 AND cb.linea_id = le.linea_id;

CREATE OR REPLACE VIEW vw_improvement_value_pool AS
SELECT
    i.iniciativa_id,
    i.planta_id,
    i.linea_id,
    i.equipo_id,
    i.categoria_iniciativa,
    i.tipo_iniciativa,
    i.descripcion_resumida,
    i.impacto_esperado_energia_pct,
    i.impacto_esperado_oee_pct,
    i.impacto_esperado_emisiones_pct,
    i.dificultad_implementacion,
    i.quick_win_flag,
    c.capex_estimado,
    c.opex_impacto_proxy,
    c.ahorro_anual_estimado,
    c.payback_estimado_meses,
    c.criticidad_estrategica,
    c.horizonte_implantacion,
    c.riesgo_implantacion,
    (
        0.30 * i.impacto_esperado_energia_pct
        + 0.25 * i.impacto_esperado_oee_pct
        + 0.20 * i.impacto_esperado_emisiones_pct
        + 0.15 * (CASE WHEN c.payback_estimado_meses > 0 THEN 100 / c.payback_estimado_meses ELSE 0 END)
        + 0.10 * (CASE WHEN i.quick_win_flag = 1 THEN 10 ELSE 5 END)
    ) AS prioridad_compuesta
FROM stg_initiatives i
LEFT JOIN stg_capex c
  ON i.iniciativa_id = c.iniciativa_id;

-- KPI: top líneas por pérdidas.
CREATE OR REPLACE VIEW kpi_top_lineas_perdidas AS
SELECT
    planta_id,
    linea_id,
    SUM(loss_value_proxy_eur) AS perdida_total_eur
FROM mart_line_day
GROUP BY 1,2
ORDER BY perdida_total_eur DESC
LIMIT 15;

-- KPI: top equipos por sobreconsumo.
CREATE OR REPLACE VIEW kpi_top_equipos_sobreconsumo AS
SELECT
    planta_id,
    linea_id,
    equipo_id,
    AVG(consumo_relativo_tipo) AS consumo_relativo_prom,
    SUM(coste_energia_eur) AS coste_energia_total
FROM mart_equipment_day
GROUP BY 1,2,3
ORDER BY consumo_relativo_prom DESC, coste_energia_total DESC
LIMIT 15;

-- KPI: top turnos por ineficiencia.
CREATE OR REPLACE VIEW kpi_top_turnos_ineficiencia AS
SELECT
    planta_id,
    turno,
    AVG(sec_shift_kwh_ton) AS sec_promedio,
    AVG(scrap_pct) AS scrap_promedio,
    AVG(shift_variance_proxy) AS varianza_operativa
FROM int_line_shift
GROUP BY 1,2
ORDER BY varianza_operativa DESC
LIMIT 10;

-- KPI: top causas raíz.
CREATE OR REPLACE VIEW kpi_top_causas_raiz AS
SELECT
    periodo,
    planta_id,
    causa_parada,
    SUM(loss_root_cause_score) AS score_total
FROM vw_loss_root_cause_summary
GROUP BY 1,2,3
ORDER BY score_total DESC
LIMIT 20;

-- KPI: líneas con peor OEE.
CREATE OR REPLACE VIEW kpi_lineas_peor_oee AS
SELECT
    planta_id,
    linea_id,
    AVG(oee_synthetic) AS oee_promedio
FROM mart_line_day
GROUP BY 1,2
ORDER BY oee_promedio ASC
LIMIT 15;

-- KPI: líneas con peor SEC.
CREATE OR REPLACE VIEW kpi_lineas_peor_sec AS
SELECT
    planta_id,
    linea_id,
    AVG(sec_kwh_ton) AS sec_promedio
FROM mart_line_day
GROUP BY 1,2
ORDER BY sec_promedio DESC
LIMIT 15;

-- KPI: iniciativas con mayor valor.
CREATE OR REPLACE VIEW kpi_iniciativas_mayor_valor AS
SELECT
    iniciativa_id,
    planta_id,
    linea_id,
    tipo_iniciativa,
    prioridad_compuesta,
    ahorro_anual_estimado,
    payback_estimado_meses
FROM vw_improvement_value_pool
ORDER BY prioridad_compuesta DESC
LIMIT 20;

-- KPI: CAPEX con mejor payback.
CREATE OR REPLACE VIEW kpi_capex_mejor_payback AS
SELECT
    iniciativa_id,
    planta_id,
    linea_id,
    capex_estimado,
    ahorro_anual_estimado,
    payback_estimado_meses
FROM vw_improvement_value_pool
WHERE tipo_iniciativa = 'CAPEXTransformacional'
ORDER BY payback_estimado_meses ASC
LIMIT 15;

-- KPI: emisiones evitables proxy.
CREATE OR REPLACE VIEW kpi_emisiones_evitables_proxy AS
SELECT
    planta_id,
    linea_id,
    SUM(emissions_kg) AS emisiones_actuales_kg,
    SUM(emissions_kg) * 0.12 AS emisiones_evitables_proxy_kg
FROM mart_line_day
GROUP BY 1,2
ORDER BY emisiones_evitables_proxy_kg DESC;

-- KPI: criticidad global planta/línea.
CREATE OR REPLACE VIEW kpi_criticidad_global AS
SELECT
    mld.planta_id,
    mld.linea_id,
    AVG(mld.loss_value_proxy_eur) AS loss_promedio,
    AVG(mld.sec_kwh_ton) AS sec_promedio,
    AVG(mld.oee_synthetic) AS oee_promedio,
    (
        0.45 * AVG(mld.loss_value_proxy_eur) / NULLIF(MAX(AVG(mld.loss_value_proxy_eur)) OVER (), 0)
        + 0.30 * AVG(mld.sec_kwh_ton) / NULLIF(MAX(AVG(mld.sec_kwh_ton)) OVER (), 0)
        + 0.25 * (1 - AVG(mld.oee_synthetic))
    ) AS criticidad_global_score
FROM mart_line_day mld
GROUP BY 1,2
ORDER BY criticidad_global_score DESC;
