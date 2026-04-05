# Recommendation Lineage Framework

## Objetivo
Hacer auditable cada recomendación final (`recommendation_id`) con evidencia técnica, métrica, decisional y de release.

## Niveles de lineage implementados
1. **dataset lineage**
   - `data/processed/dataset_lineage.csv`
2. **metric lineage**
   - `data/processed/metric_lineage.csv`
3. **score lineage**
   - `data/processed/score_lineage.csv`
4. **decision lineage**
   - `data/processed/decision_lineage.csv`
5. **executive output lineage**
   - `data/processed/executive_output_lineage.csv`

## Artefactos principales
- `data/processed/lineage_registry.csv`
- `data/processed/lineage_edges.csv`
- `data/processed/metric_to_score_map.csv`
- `data/processed/score_to_decision_map.csv`
- `data/processed/recommendation_trace_table.csv`
- `data/processed/executive_output_trace_table.csv`
- `data/processed/version_dependency_map.csv`

## Explicación auditable por recomendación
Campos clave en `recommendation_trace_table.csv`:
- `recommendation_id`
- `selected_flag`
- `objective_contribution`
- `dominant_drivers`
- `metrics_involved`
- `scores_involved`
- `active_constraints`
- `scenario_used`
- `trade_offs`
- `confidence_score`, `confidence_band`
- `caveat_codes`
- `metric_version_set`, `metric_registry_version`, `data_contract_version`
- `readiness_level`, `validation_evidence_grade`

## Reglas de enforcement
- No se considera recomendación auditable si no existe su fila en `recommendation_trace_table.csv`.
- `caveat_codes` debe reflejar issues abiertos de validación.
- `version_dependency_map.csv` debe coincidir con `run_id` activo.
- `lineage_edges.csv` debe ser DAG acíclico.

## Límite explícito
La trazabilidad no sustituye la robustez metodológica: si validación marca `not-committee-grade`, la recomendación es trazable pero no committee-grade.
