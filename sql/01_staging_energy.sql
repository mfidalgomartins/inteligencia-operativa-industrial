-- Dialecto: DuckDB SQL
-- Objetivo: normalizar fuentes energéticas, emisiones y contexto operativo.

CREATE OR REPLACE VIEW stg_energy_hourly AS
SELECT
    CAST(timestamp AS TIMESTAMP) AS timestamp,
    planta_id,
    linea_id,
    equipo_id,
    fuente_energia,
    CAST(consumo_kwh AS DOUBLE) AS consumo_kwh,
    CAST(demanda_kw AS DOUBLE) AS demanda_kw,
    CAST(factor_carga AS DOUBLE) AS factor_carga,
    CAST(energia_reactiva_proxy AS DOUBLE) AS energia_reactiva_proxy,
    CAST(coste_energia_proxy AS DOUBLE) AS coste_energia_proxy,
    CAST(precio_energia_proxy AS DOUBLE) AS precio_energia_proxy
FROM read_csv_auto('{raw_dir}/consumos_energia_horarios.csv', HEADER=TRUE);

CREATE OR REPLACE VIEW stg_emissions_hourly AS
SELECT
    CAST(timestamp AS TIMESTAMP) AS timestamp,
    planta_id,
    linea_id,
    fuente_energia,
    CAST(consumo_kwh AS DOUBLE) AS consumo_kwh,
    CAST(factor_emision_proxy AS DOUBLE) AS factor_emision_proxy,
    CAST(emisiones_co2e_kg AS DOUBLE) AS emisiones_co2e_kg,
    CAST(intensidad_emisiones_por_unidad AS DOUBLE) AS intensidad_emisiones_por_unidad
FROM read_csv_auto('{raw_dir}/emisiones_estimadas.csv', HEADER=TRUE);

CREATE OR REPLACE VIEW stg_context_shift AS
SELECT
    CAST(fecha AS DATE) AS fecha,
    turno,
    planta_id,
    CAST(demanda_operativa_indice AS DOUBLE) AS demanda_operativa_indice,
    CAST(mix_complejidad_indice AS DOUBLE) AS mix_complejidad_indice,
    CAST(temperatura_ambiente AS DOUBLE) AS temperatura_ambiente,
    CAST(precio_energia_mercado_proxy AS DOUBLE) AS precio_energia_mercado_proxy,
    CAST(tension_operativa_proxy AS DOUBLE) AS tension_operativa_proxy
FROM read_csv_auto('{raw_dir}/contexto_operativo.csv', HEADER=TRUE);
