# Score Comparability Policy

## Taxonomía de comparabilidad
- `absolute_comparable_inter_release`
  - Comparable entre releases si se mantiene `scale_version`.
  - Permite seguimiento temporal directo.
- `contextual_comparable_inter_release`
  - Comparable con cautela y contexto operativo equivalente.
  - Requiere revisar mix de proceso, restricciones y shocks.
- `non_comparable_cross_window`
  - No debe compararse entre ventanas como KPI de tendencia.

## Reglas de uso ejecutivo
1. Nunca presentar score sin `scaling_method`, `scale_version` y `comparability_tag`.
2. Si `scale_version` cambia, reportar ruptura de serie.
3. `contextual` exige nota de interpretación en dashboard/reporte.
4. `non_comparable` solo para diagnóstico puntual.

## Enforcement en el proyecto
- Registro oficial: `data/processed/score_comparability_registry.csv`
- Validación: check `score_comparability_tags_valid` en `validation_checks_hardened.csv`
- Capa dashboard: callout de comparabilidad para score de priorización.

## Scores y política activa
- Absolute: `equipment_energy_anomaly_score`, `process_deviation_risk_score`, componentes base de priorización.
- Contextual: `line_criticality_score`, `opportunity_priority_score`, `improvement_priority_index`, `portfolio_objective_score`.

## Interpretaciones incorrectas a evitar
- Tratar un score `contextual` como benchmark absoluto universal.
- Afirmar mejora real por cambio de score cuando cambió `scale_version`.
- Usar score como sustituto de validación económica formal.
