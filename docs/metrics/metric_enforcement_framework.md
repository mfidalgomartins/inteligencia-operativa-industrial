# Metric Enforcement Framework

## Objetivo
Convertir la capa métrica de “documentada” a “obligatoria”, evitando deriva entre SQL, Python, dashboard y reporting.

## Reglas de enforcement
1. Toda métrica headline debe nacer en `data/processed/executive_kpi_snapshot.csv` o `data/processed/executive_decision_snapshot.csv`.
2. `cross_output_consistency.csv` es control obligatorio, con tiers:
   - `BLOCKER`: no puede haber `ERROR`.
   - `CRITICAL`: no puede haber `ERROR` ni `WARN` abiertos para release limpio.
   - `HIGH`: puede quedar `WARN` con caveat explícito.
3. Dashboard no recalcula KPIs críticos; consume `dashboard_canonical_dataset.json`.
4. Reporting ejecutivo no recalcula headline KPIs fuera de snapshots canónicos.

## Checks mínimos exigidos
- `sec_sql_vs_python`
- `oee_sql_vs_python`
- `snapshot_production_vs_line_day`
- `snapshot_loss_vs_line_day`
- `snapshot_saving_vs_invest`
- `decision_top_line_vs_scores`
- `decision_top_equipment_vs_scores`
- `snapshot_vs_decision_run_id`
- `snapshot_vs_decision_metric_version_set`

## Política de fallo
- Si un `BLOCKER` falla: release inválido.
- Si hay discrepancia CRITICAL entre snapshot/decision/sources: no publicar como material de comité.
- Si hay drift HIGH: publicar solo con caveat explícito.

## Evidencia y trazabilidad
- `data/processed/cross_output_consistency.csv`
- `data/processed/metric_contract_validation.csv`
- `data/processed/validation_readiness_summary.csv`
- `outputs/reports/validation_report.md`
