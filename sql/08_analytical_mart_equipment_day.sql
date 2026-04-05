-- Dialecto: DuckDB SQL
-- Objetivo: mart equipo-día y vista de eficiencia/riesgo de activos.

CREATE OR REPLACE TABLE mart_equipment_day AS
SELECT
    ied.fecha,
    ied.planta_id,
    ied.linea_id,
    ied.equipo_id,
    ied.tipo_equipo,
    ied.subsistema,
    ied.criticidad_equipo,
    ied.eficiencia_nominal,
    ied.consumo_kwh,
    ied.coste_energia_eur,
    ied.factor_carga_prom,
    ied.demanda_kw_media,
    ied.n_paradas,
    ied.n_paradas_no_plan,
    ied.horas_parada,
    ied.n_mantenimientos,
    ied.horas_mantenimiento,
    ied.coste_mantenimiento_eur,
    ied.n_desvios,
    ied.desviacion_abs_media,
    ied.impacto_calidad_sum,
    ied.disponibilidad_proxy,
    ied.consumo_relativo_tipo,
    ied.riesgo_equipo_proxy,
    (
        0.35 * COALESCE(ied.consumo_relativo_tipo, 0)
        + 0.25 * COALESCE(ied.horas_parada, 0)
        + 0.20 * COALESCE(ied.n_desvios, 0)
        + 0.20 * ied.criticidad_equipo
    ) AS equipment_efficiency_risk_index
FROM int_equipment_day ied;

CREATE OR REPLACE VIEW vw_equipment_efficiency_risk AS
SELECT
    med.*,
    CASE
        WHEN med.equipment_efficiency_risk_index >= 8 THEN 'MuyAlto'
        WHEN med.equipment_efficiency_risk_index >= 5 THEN 'Alto'
        WHEN med.equipment_efficiency_risk_index >= 3 THEN 'Medio'
        ELSE 'Bajo'
    END AS risk_band,
    AVG(med.equipment_efficiency_risk_index) OVER (
        PARTITION BY med.equipo_id
        ORDER BY med.fecha
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS trend_deterioro_7d
FROM mart_equipment_day med;
