# Dashboard Build And Governance

## Artefactos oficiales
- HTML oficial: `outputs/dashboard/industrial-operating-command-center.html`
- Dataset oficial: `data/processed/dashboard_canonical_dataset.json`
- Auditoría de serving: `data/processed/dashboard_serving_audit.csv`
- Generador oficial: `src/dashboard.py` (`build_dashboard`)

## Ruta canónica de build
1. Ejecutar `python -m src`.
2. El pipeline llama `build_dashboard` dos veces desde `src/run_pipeline.py`:
   - pre-validación (construcción inicial),
   - post-validación (alineación final de `validation_state` y stamping).
3. `build_dashboard`:
   - compone dataset canónico desde outputs gobernados,
   - renderiza el HTML oficial,
   - elimina artefactos legacy de dashboard,
   - emite auditoría de serving.

## Datasets de entrada permitidos
- `data/processed/executive_kpi_snapshot.csv`
- `data/processed/executive_decision_snapshot.csv`
- `data/processed/validation_readiness_summary.csv`
- tablas analíticas gobernadas (`line_day_features_scored`, `line_criticality_scores`, `equipment_energy_anomaly_scores`, `loss_root_cause_score`, `investment_prioritization_final`, `scenario_table`, `portfolio_summary`, `score_comparability_registry`)

## Lógica prohibida en frontend
- Recalcular KPIs headline.
- Recalcular scoring o priorización.
- Aplicar fórmulas de negocio que ya existen en backend.

## Version stamping obligatorio
El HTML y el dataset deben incluir:
- `run_id`
- `snapshot_id`
- `metric_version_set`
- `decision_model_version`
- `validation_state`
- `dashboard_mode=canonical_single_html`

## Checks de gobernanza obligatorios
- Un único HTML oficial presente.
- Ausencia de HTMLs legacy (`dashboard_executive_light`, `dashboard_full_offline`, `dashboard_board_pack`).
- Ausencia de datasets legacy (`dashboard_exec_dataset`, `dashboard_full_dataset`).
- `canonical_under_threshold_flag=1`.
- `no_competing_html_flag=1`.
- Consistencia de stamping con snapshot ejecutivo.

## Política de tamaño
- Umbral máximo del HTML oficial: `4_000_000` bytes.
- Si se supera el umbral: abrir issue de performance y no promover release como limpio.

## Mantenimiento
- Cualquier cambio en visuales o estructura debe:
  - actualizar tests de `tests/dashboard_consistency/`,
  - mantener consistencia con `executive_kpi_snapshot.csv`,
  - mantener secciones ejecutivas obligatorias.
