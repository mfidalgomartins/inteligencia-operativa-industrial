# Investment Case Methodology

## Objetivo
Definir un marco financiero operativo para priorizar iniciativas industriales sin confundir screening con evaluación final de comité.

## 1. Baseline económico
Cada iniciativa se evalúa sobre tres exposiciones de línea:
- `baseline_energy_exposure`
- `baseline_non_energy_exposure`
- `baseline_emission_cost`

Estas exposiciones se ajustan por sensibilidad de escenario (`precio energía`, `carbono`, `margen operativo`).

## 2. Puente de valor
1. `gross_technical_value_energy`
2. `avoided_loss`
3. `gross_technical_value_emissions`
4. `gross_technical_value = 1 + 2 + 3`
5. `net_operational_value = gross_technical_value - implementation_opex`
6. `net_captured_value = gross_technical_value * expected_capture_rate - implementation_opex`
7. `downside_adjusted_annual = gross_technical_value * downside_capture_rate - implementation_opex`

Control clave: `avoided_loss <= baseline_non_energy_exposure`.

## 3. Modelado temporal
Se modela captura con:
- `start_delay_months`
- `ramp_up_months`
- horizonte `ANALYSIS_HORIZON_YEARS`

Con ello se calculan:
- `discounted_value`
- `downside_adjusted_value`
- `payback_meses` (descontado)
- `cost_of_delay_12m`

## 4. Riesgo financiero
- `value_at_risk_95`: gap anual esperado vs downside.
- `screening_var_95_npv`: gap NPV esperado vs downside.
- `robust_risk_metric_candidate_npv`: métrica conservadora ampliada.

## 5. Métricas por nivel de madurez
### Screening financiero
- `annual_saving_proxy`
- `screening_irr_pct`
- `value_at_risk_95`

### Pre-feasibility
- `discounted_value`
- `downside_adjusted_value`
- `cost_of_delay_12m`
- `capital_efficiency`

### Business-case candidate (no final)
- `formal_irr_candidate_pct`
- `formal_irr_candidate_flag`

No se considera business case final hasta calibración con datos reales y supuestos corporativos auditados.

## 6. Sensibilidad obligatoria
Se aplica sobre:
- precio energía
- coste carbono
- margen operativo
- retraso implantación
- sobrecoste CAPEX
- capture shortfall

## 7. Reglas de interpretación
- `screening_irr_pct` **no** es IRR final de comité.
- `formal_irr_candidate_pct` es señal pre-feasibility, no aprobación financiera.
- `financial_maturity_stage` define el lenguaje permitido en reporting.

## 8. Before/After de lógica financiera
Disponible en:
- `data/processed/financial_metrics_before_after.csv`
- `data/processed/financial_logic_audit_table.csv`

## 9. Estado de credibilidad actual
- Defendible: screening + pre-feasibility + priorización multicriterio.
- No defendible aún: cierre committee-grade de inversión.
