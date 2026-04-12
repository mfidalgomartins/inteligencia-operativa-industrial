# Arquitectura SQL

## Dialecto elegido
- **DuckDB SQL**.
- Razón: portabilidad local, ejecución analítica columnar rápida y lectura directa de CSV para portfolio reproducible.

## Niveles de modelado
1. **Staging**: tipado y normalización de fuentes crudas.
2. **Integration**: integración granular por línea-hora, equipo-día y línea-turno.
3. **Analytical marts**: vistas y tablas de consumo, eficiencia, riesgo y pérdida.
4. **KPI queries**: rankings ejecutivos de pérdidas, sobreconsumo, SEC/OEE y value pool.
5. **Validation queries**: controles de rango, consistencia y calidad analítica.

## Scripts y propósito
1. `01_staging_energy.sql`: staging de energía, emisiones y contexto operativo.
2. `02_staging_production.sql`: staging de plantas, líneas, equipos, producción y benchmark externo.
3. `03_staging_events.sql`: staging de desvíos, paradas, mantenimiento, iniciativas, CAPEX y escenarios.
4. `04_integrated_line_hour.sql`: integración línea-hora (energía, producción, emisiones, desvíos, paradas, pérdidas).
5. `05_integrated_equipment_day.sql`: integración equipo-día (consumo, disponibilidad, mantenimiento, riesgo).
6. `06_integrated_line_shift.sql`: integración línea-turno para varianza operacional.
7. `07_analytical_mart_line_day.sql`: mart línea-día y `vw_line_energy_performance`.
8. `08_analytical_mart_equipment_day.sql`: mart equipo-día y `vw_equipment_efficiency_risk`.
9. `09_analytical_mart_plant_week.sql`: mart planta-semana y `vw_shift_operational_variance`.
10. `10_kpi_queries.sql`: `vw_loss_root_cause_summary`, `vw_improvement_value_pool` y KPIs top.
11. `11_validation_queries.sql`: `validation_sql_checks` para controles de robustez.

## Orden de ejecución oficial
- Ejecutar secuencialmente del `01` al `11`.
- Runner: `src/sql_pipeline.py`.
- Log de ejecución exportado en `data/processed/sql_execution_log.csv`.
- Scripts históricos removidos para mantener SQL operativo curado.

## Objetos analíticos clave
- `vw_line_energy_performance`
- `vw_equipment_efficiency_risk`
- `vw_shift_operational_variance`
- `vw_loss_root_cause_summary`
- `vw_improvement_value_pool`

## Reglas de calidad SQL
- Sin `SELECT *` en queries productivas de consumo final.
- CTEs explícitos por dominio.
- Joins por claves de negocio defendibles.
- Validaciones de rango y no negatividad en capa final.
