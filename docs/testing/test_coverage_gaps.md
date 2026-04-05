# Test Coverage Gaps

## Before/After útil (no cosmético)

## Before (estado previo)
- 12 tests centrados en:
  - existencia de outputs,
  - checks básicos de contrato,
  - constraints de presupuesto simples.
- Regresiones que podían pasar desapercibidas:
  - inversión de ranking por ruido/normalización,
  - incoherencias gross/net/risk en escenarios,
  - inconsistencias summary ↔ detalle en cartera,
  - mismatch dashboard ↔ snapshot ejecutivo,
  - drift de rowcounts/temporalidad entre releases.

## After (estado actual)
- 95 tests activos.
- Nuevas regresiones cubiertas:
  - **score math drift**: fórmula compuesta y monotonicidad.
  - **ranking fragility**: sensibilidad y perturbación.
  - **financial regressions**: invariantes de savings/NPV/VaR/payback.
  - **portfolio regressions**: restricciones, dependencias, olas y consistencia de estado/optimalidad del optimizador.
  - **executive consistency**: snapshot vs dashboard y checks de comité.
  - **reproducibility drift**: firmas de rowcount y cobertura temporal.
  - **sector consistency**: cobertura de mapping por dominio y divergencia real Tubacex vs Petronor.
  - **packaging forensics**: lockfile sync, fingerprints, packs de release y reducción material de payload.

## Riesgos ahora cubiertos explícitamente
1. Drift de fórmula en `improvement_priority_index`.
2. Pérdida de robustez top-N por pequeños cambios de distribución.
3. Savings inconsistentes entre escenario y priorización.
4. Selección de cartera incoherente con restricciones técnicas/económicas.
5. KPIs ejecutivos desalineados entre datasets y dashboard.
6. Artefactos incompletos en release y ausencia de manifest válido.
7. Deriva silenciosa de volumen temporal de datos base.

## Gaps que siguen expuestos (honesto)
1. La optimización de cartera ya es formal para su formulación binaria (branch-and-bound), pero no hay aún MILP con duales/precios sombra de solver OR corporativo.
2. Estabilidad inter-release de scores en serie histórica de múltiples corridas (hoy hay perturbation local + sensibilidad, no benchmark histórico multi-release).
3. Pruebas de performance del dashboard (payload/latencia) no automatizadas.
4. Golden snapshots de visuales (image snapshot testing) no implementados.
5. Tests de compatibilidad con datos reales de historian/MES/ERP/CMMS no aplican todavía (dataset sintético).

## Próximos tests de alta prioridad
1. Test de optimización formal con constraints duales y shadow prices (cuando exista motor OR).
2. Regression pack de estabilidad inter-release (N últimos manifests).
3. Tests de reconciliación financiera DCF/NPV/IRR con escenarios de retraso y overrun CAPEX.
4. Tests de budget stress (variación ±20% CAPEX/OPEX) con expectativas de cambio de cartera.
5. Dashboard performance gates (peso HTML, render time y memoria).
