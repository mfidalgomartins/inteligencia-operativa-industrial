-- Dialecto: DuckDB SQL
-- Objetivo: mart planta-semana y vista de variabilidad operacional por turno.

CREATE OR REPLACE TABLE mart_plant_week AS
SELECT
    DATE_TRUNC('week', fecha) AS semana,
    planta_id,
    SUM(production_volume_ton) AS production_volume_ton,
    AVG(sec_kwh_ton) AS sec_kwh_ton,
    AVG(oee_synthetic) AS oee_synthetic,
    AVG(availability_rate) AS availability_rate,
    SUM(cost_energy_total) AS cost_energy_total,
    SUM(emissions_kg) AS emissions_kg,
    SUM(loss_value_proxy_eur) AS loss_value_proxy_eur,
    SUM(n_paradas) AS n_paradas,
    SUM(n_desvios) AS n_desvios
FROM mart_line_day
GROUP BY 1,2;

CREATE OR REPLACE VIEW vw_shift_operational_variance AS
SELECT
    ils.fecha,
    ils.turno,
    ils.planta_id,
    ils.linea_id,
    ils.familia_proceso,
    ils.toneladas_producidas_proxy AS throughput_ton,
    ils.sec_shift_kwh_ton,
    ils.scrap_pct,
    ils.reproceso_pct,
    ils.n_desvios_shift,
    ils.n_paradas_shift,
    ils.horas_parada_shift,
    ils.perdida_proxy_shift_eur,
    ils.shift_variance_proxy,
    ils.availability_shift,
    ils.utilization_rate,
    AVG(ils.sec_shift_kwh_ton) OVER (PARTITION BY ils.linea_id, ils.turno) AS sec_promedio_turno_linea,
    AVG(ils.scrap_pct) OVER (PARTITION BY ils.linea_id, ils.turno) AS scrap_promedio_turno_linea
FROM int_line_shift ils;
