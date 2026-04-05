# Metric Contracts Overview

## Registro oficial
Archivo: `contracts/metrics/metric_registry.json`

## KPI críticos gobernados
- `oee_synthetic`
- `sec`
- `loss_value_proxy`
- `annual_saving_proxy`
- `npv_risk_adjusted`
- `improvement_priority_index`

## Reglas de agregación
- Ratios energéticos: razón de sumas, no promedio simple.
- OEE: recomputar desde componentes o ponderar por horas.
- NPV y ahorro de portfolio: sumar solo iniciativas seleccionadas.

## Anti-patterns prohibidos
- Recalcular KPI en frontend.
- Duplicar fórmulas de métricas en notebooks ad hoc.
- Mezclar score analítico con regla de decisión sin capa intermedia.
