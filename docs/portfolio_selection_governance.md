# Portfolio Selection Governance

## Propósito
Evitar deriva metodológica y sobreclaims en selección de cartera, garantizando trazabilidad desde inputs financieros-operativos hasta recomendación ejecutiva.

## Reglas de gobierno
1. No usar ranking simple como selección final oficial.
2. La selección oficial solo proviene de `run_investment_prioritization`.
3. No afirmar “cartera óptima” sin `optimization_status=solved_exact`.
4. Si `time_limited_best_feasible`, etiquetar resultado como aproximación semi-formal.
5. Si estado `infeasible_*`, bloquear claim de recomendación ejecutable.

## Roles y ownership
- Owner técnico: Analytics Engineering (formulación y ejecución).
- Owner económico: Industrial Finance Analytics (supuestos CAPEX/OPEX/riesgo).
- Owner de decisión: Operaciones + Energía + PMO (aprobación por ola).
- QA owner: Validation/Release (gates y publish decision).

## Artefactos mínimos para release
- `investment_prioritization_final.csv`
- `portfolio_summary.csv`
- `portfolio_optimization_solution.csv`
- `portfolio_optimization_metadata.csv`
- `portfolio_selected_vs_excluded.csv`
- `portfolio_constraint_pressure_summary.csv`
- `portfolio_unlocked_value_analysis.csv`
- `portfolio_wave_logic_explained.csv`
- `validation_report.md`

## Gates de publicación
- `publish-blocked` si:
  - incumple CAPEX/OPEX/capacidad/dependencias,
  - metadata de optimización inconsistente,
  - faltan artefactos obligatorios.
- `publish with caveat` si:
  - resultado `time_limited_best_feasible`,
  - madurez financiera sigue en screening/pre-feasibility.
- `publish` solo cuando no hay blockers y readiness lo permite.

## Política de comunicación ejecutiva
- Mostrar siempre:
  - estado del optimizador,
  - grado de optimalidad,
  - restricciones activas,
  - valor desbloqueable bajo relajaciones.
- Nunca ocultar caveats de madurez financiera.
- Separar claramente:
  - priorización por score,
  - selección de cartera,
  - secuenciación por olas.
