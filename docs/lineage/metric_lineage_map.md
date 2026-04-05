# Metric Lineage Map

## Cadena de trazabilidad
`raw -> stg -> int -> mart -> features -> scores -> decision -> executive`

## KPIs críticos
- `oee_synthetic`: `mart_line_day` -> `line_day_features_scored` -> dashboard/report.
- `sec`: `mart_line_day.sec_kwh_ton` -> `line_day_features_scored.sec` -> scores/benchmark.
- `loss_value_proxy`: `int_line_hour` -> `mart_line_day.loss_value_proxy_eur` -> priorización.
- `annual_saving_proxy`: `scenario_table` -> `investment_prioritization_final` -> dashboard.
- `portfolio_npv_risk_adjusted`: `scenario_table` + restricciones -> `portfolio_recommendation`.

## Reglas
- Ninguna métrica ejecutiva debe saltarse `mart` o `decision` para cálculo ad hoc.
- Todo output final incluye `run_id` y `metric_version_set`.
- Toda recomendación final debe existir en `data/processed/recommendation_trace_table.csv`.
- Trazabilidad ejecutable y acíclica en `data/processed/lineage_registry.csv` + `data/processed/lineage_edges.csv`.
