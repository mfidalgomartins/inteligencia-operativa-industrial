# Ownership y Release Model

## Ownership (RACI simplificado)
- Operaciones: `production`, `oee`, `utilization`, `line_criticality`.
- Energía: `sec`, `energy_cost`, `energy_anomaly`.
- Mantenimiento: `availability`, `downtime`, `reliability`.
- Finanzas industriales: `annual_saving`, `npv`, `payback`, `portfolio_value`.
- Analytics Engineering: contratos, semantic marts, validación y release gates.

## Reglas de aprobación
1. Cambio de definición KPI: aprobación dual negocio + técnico.
2. Cambio de score/priorización: aprobación triple (negocio + técnico + estrategia industrial).
3. Publicación dashboard/reports: requiere `validation_report` sin fallos críticos.

## Disciplina de release
- `dev`: exploratorio, no ejecutivo.
- `candidate`: validaciones completas ejecutadas.
- `prod`: publicación ejecutiva con manifiesto de build y evidencia de reconciliación.

## Gates de publicación
1. `schema + grain`: obligatorio.
2. `metric contracts`: obligatorio.
3. `cross-output consistency`: obligatorio.
4. `scenario/portfolio constraints`: obligatorio.
