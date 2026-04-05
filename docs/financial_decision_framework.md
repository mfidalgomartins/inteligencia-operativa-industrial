# Financial Decision Framework

## 1) Alcance real de madurez
- **Screening financiero**: comparación temprana entre iniciativas con proxies explícitos.
- **Pre-feasibility**: discounted value, downside, cost of delay y sensibilidad estructurada.
- **Business case candidate (no final)**: iniciativas con señal financiera robusta para pasar a fase de caso corporativo.
- **No committee-grade final**: este marco no sustituye modelización financiera auditada con datos reales ERP/controlling.

## 2) Taxonomía financiera operativa
- `gross_technical_value`: valor técnico anual bruto (energía + avoided loss + emisiones).
- `avoided_loss`: pérdidas operativas evitadas (sin doble conteo con energía).
- `net_captured_value`: valor anual neto capturado (captura esperada - OPEX impl.).
- `discounted_value`: valor descontado esperado (NPV screening/pre-feasibility).
- `downside_adjusted_value`: NPV bajo downside.
- `cost_of_delay_12m`: erosión de valor por retrasar 12 meses.
- `capital_efficiency`: downside-adjusted value / CAPEX efectivo.
- `screening_irr_pct`: ratio de retorno para priorización temprana.
- `formal_irr_candidate_pct`: candidato aproximado de pre-feasibility (no IRR final de comité).
- `screening_var_95_npv`: gap entre valor esperado y downside en NPV.
- `robust_risk_metric_candidate_npv`: ampliación conservadora del riesgo NPV.

## 3) Principios de diseño
- Separación explícita entre baseline exposure y valor capturado.
- Prevención explícita de doble conteo en ahorro operativo.
- Distinción entre métricas de priorización y métricas candidatas para comité.
- Sensibilidad obligatoria sobre energía, carbono, margen, delay, CAPEX overrun y capture shortfall.
- Caveat obligatorio: ninguna métrica se presenta como cierre financiero corporativo final.

## 4) Reglas de uso por audiencia
- Operaciones/Energía: usar `gross_technical_value`, `net_captured_value`, `cost_of_delay_12m`.
- PMO/Transformación: usar `portfolio_objective_score`, `capital_efficiency`, `implementation_burden`.
- Finanzas industriales: usar `discounted_value`, `downside_adjusted_value`, `screening_var_95_npv` con caveat.
- Comité inversión: usar solo como pre-screening para decidir qué iniciativas pasan a business case formal.

## 5) Claims defendibles
- La priorización financiera está endurecida para screening/pre-feasibility.
- Se evita doble conteo básico en el puente de valor técnico.
- El escenario financiero incorpora sensibilidad explícita y trazable.

## 6) Claims no permitidos
- "IRR final de comité" usando `screening_irr_pct` o `formal_irr_candidate_pct`.
- "Portfolio óptimo financiero final" sin cierre OR/finanzas corporativas.
- "Caso de inversión aprobado" sin datos reales auditados.
