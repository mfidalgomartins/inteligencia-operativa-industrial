-- Dialecto: DuckDB SQL
-- Objetivo: mart línea-día y vista integrada de performance energética-operativa.

CREATE OR REPLACE TABLE mart_line_day AS
SELECT
    ilh.fecha,
    ilh.planta_id,
    ilh.linea_id,
    ilh.familia_proceso,
    SUM(COALESCE(ilh.toneladas_equivalentes_hora, 0)) AS production_volume_ton,
    AVG(ilh.utilization_rate) AS utilization_rate,
    AVG(ilh.sec_kwh_ton) AS sec_kwh_ton,
    AVG(l.intensidad_energetica_objetivo) AS sec_target_kwh_ton,
    AVG(ilh.coste_energia_por_unidad) AS cost_energy_per_unit,
    SUM(ilh.coste_energia_eur) AS cost_energy_total,
    SUM(ilh.emisiones_co2e_kg) AS emissions_kg,
    AVG(CASE WHEN ilh.horas_planificadas > 0 THEN ilh.horas_operativas / ilh.horas_planificadas ELSE NULL END) AS availability_rate,
    AVG(1 - ilh.scrap_pct / 100.0) AS quality_rate_proxy,
    AVG((1 - ilh.scrap_pct / 100.0) * COALESCE(ilh.utilization_rate, 0) * CASE WHEN ilh.horas_planificadas > 0 THEN ilh.horas_operativas / ilh.horas_planificadas ELSE 0 END) AS oee_synthetic,
    SUM(ilh.n_desvios) AS n_desvios,
    SUM(ilh.n_paradas) AS n_paradas,
    SUM(ilh.horas_parada) AS horas_parada,
    SUM(ilh.perdida_total_proxy_eur) AS loss_value_proxy_eur
FROM int_line_hour ilh
JOIN stg_lines l ON ilh.linea_id = l.linea_id
GROUP BY 1,2,3,4;

CREATE OR REPLACE VIEW vw_line_energy_performance AS
SELECT
    mld.fecha,
    mld.planta_id,
    mld.linea_id,
    mld.familia_proceso,
    mld.production_volume_ton,
    mld.utilization_rate,
    mld.sec_kwh_ton,
    mld.sec_target_kwh_ton,
    mld.sec_kwh_ton - mld.sec_target_kwh_ton AS sec_gap,
    mld.cost_energy_total,
    mld.cost_energy_per_unit,
    mld.emissions_kg,
    mld.availability_rate,
    mld.quality_rate_proxy,
    mld.oee_synthetic,
    mld.n_desvios,
    mld.n_paradas,
    mld.horas_parada,
    mld.loss_value_proxy_eur,
    CASE WHEN mld.production_volume_ton > 0 THEN mld.loss_value_proxy_eur / mld.production_volume_ton ELSE NULL END AS loss_value_per_ton
FROM mart_line_day mld;
