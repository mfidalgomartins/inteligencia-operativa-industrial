# Framework de Scoring y Priorización

## 1) Capa diagnóstica
- `sec_benchmark_gap`: brecha energética por línea.
- `equipment_energy_anomaly_score`: severidad de sobreconsumo por equipo.
- `process_deviation_risk_score`: exposición a desvíos de proceso.
- `line_criticality_score`: prioridad de intervención línea.

## 2) Capa de oportunidad
- `opportunity_priority_score`: score de oportunidad técnico-operativa por iniciativa.
- `main_loss_driver`: clasifica driver principal (paradas, desvío proceso, ineficiencia térmica).
- `recommended_action_initial`: primera acción recomendada por driver.

## 3) Capa de inversión
- `energy_saving_score`
- `operational_impact_score`
- `emissions_reduction_score`
- `implementation_feasibility_score`
- `payback_score`
- `strategic_priority_score`
- `financial_resilience_score`
- `uncertainty_index`

Índice final:
- `improvement_priority_index` = combinación ponderada de ocho dimensiones.

Gobernanza de estabilidad:
- Escalado: `anchored_reference_linear`
- Versión de escala: `anchored_reference_v1`
- Comparabilidad declarada por score en `score_comparability_registry.csv`

## 4) Decisión de cartera
Reglas de salida:
- `ejecutar ahora`
- `ejecutar en siguiente ola`
- `analizar técnicamente`
- `mantener en pipeline`
- `no prioritaria por ahora`

## 5) Segmentación
- `initiative_tier` (Tier 1-4)
- `recommended_sequence` (OLA_1, OLA_2, ...)
- `main_business_case` (ahorro rápido, confiabilidad, descarbonización, CAPEX estructural, optimización incremental)
