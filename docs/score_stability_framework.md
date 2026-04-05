# Score Stability Framework

## Objetivo
Reducir sensibilidad artificial por normalización local y mejorar comparabilidad inter-release sin perder interpretabilidad.

## Problema detectado
- Uso de escalado `min-max` por corrida en varios scores.
- Riesgo de inversión de ranking por cambios de distribución (no por cambio real operativo).
- Dificultad para defender evolución temporal en comité.

## Auditoría de fragilidad detectada
- `src/diagnostics.py`
  - `equipment_energy_anomaly_score`: antes local min-max.
  - `process_deviation_risk_score`: antes local min-max.
  - `line_criticality_score`: antes local min-max.
  - `opportunity_priority_score`: antes local min-max.
- `src/investment_prioritization.py`
  - Componentes de priorización (`energy_saving_score`, `payback_score`, etc.): antes local min-max.
  - `portfolio_objective_score`: antes mixto con min-max local.
- `src/feature_engineering.py`
  - `energy_risk_proxy`, `shift_variance_score`, `implementation_complexity_score`, `operational_urgency_score`: antes con escalado relativo por corrida.

## Hardening implementado
1. Escalado anclado por bandas de referencia (`anchored_reference_linear`).
2. Versionado explícito de escala: `anchored_reference_v1`.
3. Bandas por familia de proceso en `shift_variance_score`.
4. Tags de comparabilidad por score (`absolute` vs `contextual`).
5. Artefactos de robustez before/after:
   - `data/processed/score_stability_before_after.csv`
   - `data/processed/score_ranking_robustness_before_after.csv`

## Scores endurecidos
- `equipment_energy_anomaly_score`
- `process_deviation_risk_score`
- `line_criticality_score`
- `opportunity_priority_score`
- `energy_saving_score`
- `operational_impact_score`
- `emissions_reduction_score`
- `implementation_feasibility_score`
- `payback_score`
- `strategic_priority_score`
- `financial_resilience_score`
- `uncertainty_index`
- `improvement_priority_index`
- `portfolio_objective_score`

## Evidencia mínima de estabilidad
- Delta mediano ante outlier (anchored vs legacy local min-max).
- Correlación de ranking (`spearman_anchored_vs_legacy`).
- Solidez top-10 bajo shock de outlier para índice de priorización.

## Caveats
- `contextual_comparable_inter_release` requiere contexto operativo similar.
- Cambios de `scale_version` invalidan comparación directa de score histórico.
- No usar score aislado para claims causales.
