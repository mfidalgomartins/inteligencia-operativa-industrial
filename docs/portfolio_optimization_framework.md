# Portfolio Optimization Framework

## Objetivo
Elevar la selección de cartera desde ranking heurístico a una formulación binaria auditable, manteniendo interpretabilidad para operación, energía y finanzas industriales.

## Formulación implementada
- Tipo: selección binaria con branch-and-bound.
- Variable de decisión: `x_i ∈ {0,1}` por iniciativa.
- Objetivo (EUR):
  - maximizar `downside_adjusted_value`
  - + `0.30 * cost_of_delay_12m`
  - + `0.10 * max(npv_risk_adjusted, 0)`
  - - `0.15 * screening_var_95_npv`
- Restricciones hard:
  - presupuesto CAPEX,
  - presupuesto OPEX de implantación,
  - capacidad máxima de iniciativas,
  - dependencias técnicas (`i -> dep(i)`),
  - exclusiones mutuas (`i ⟂ j`),
  - inclusión obligatoria `mandatory/compliance`,
  - cobertura mínima de iniciativas `reliability-protection`.

## Estado de optimalidad (claim controlado)
- `solved_exact`: solución exacta para la formulación activa.
- `time_limited_best_feasible`: mejor factible conocida en límite de tiempo.
- `infeasible_forced_constraints` / `infeasible_no_feasible_solution`: no factible bajo restricciones.
- **Regla**: solo puede afirmarse “óptimo” cuando `status=solved_exact`.

## Outputs de decisión obligatorios
- `portfolio_optimization_solution.csv`: vector `x_i` por iniciativa.
- `portfolio_optimization_metadata.csv`: estado, grado de optimalidad, runtime y nodos.
- `portfolio_selected_vs_excluded.csv`: seleccionadas/excluidas con motivo.
- `portfolio_constraint_pressure_summary.csv`: presión por restricción activa.
- `portfolio_unlocked_value_analysis.csv`: valor desbloqueado por relajación de restricciones.
- `portfolio_wave_logic_explained.csv`: secuenciación por olas con regla aplicada.
- `portfolio_selection_before_after.csv`: puente de gobernanza heurística -> optimización.

## Límites explícitos
- No sustituye un MILP corporativo con costes estocásticos completos.
- Dependencias y conflictos se modelan como sets discretos (sin calendarios finos).
- No incluye precios sombra duales formales de solver LP/MIP comercial.

## Uso recomendado
- Comité operativo/energía: selección y secuencia por valor capturable ajustado por riesgo.
- CFO/PMO: screening robusto previo a business case formal.
- Validación: consumir siempre `portfolio_optimization_metadata.csv` para controlar claims.
