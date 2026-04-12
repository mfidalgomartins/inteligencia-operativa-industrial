# Testing Strategy Enterprise

## 1) Objetivo
Elevar la suite de tests desde validación funcional de pipeline a control de **confiabilidad de decisión**: estabilidad de scores, reconciliación financiera, robustez de escenarios/cartera, consistencia ejecutiva y disciplina de release.

## 2) Auditoría de partida (before)
- Cobertura previa: **12 tests**.
- Cobertura fuerte en:
  - existencia de outputs,
  - rangos básicos,
  - checks contractuales mínimos.
- Huecos críticos detectados:
  - sin tests de monotonicidad ni perturbación de scoring,
  - sin reconciliación financiera robusta en scenario/portfolio,
  - sin pruebas de estabilidad de ranking multi-escenario,
  - sin verificación fuerte de consistencia dashboard ↔ snapshot,
  - sin firma reproducible de rowcount/temporalidad por release,
  - sin taxonomía explícita blocker/critical/high/warning para gates.

## 3) Estado actual (after)
- Cobertura actual: **101 tests**.
- Distribución por suite:
  - `tests/unit`: 3
  - `tests/data_quality`: 5
  - `tests/metric_contracts`: 7
  - `tests/features`: 4
  - `tests/scores`: 8
  - `tests/scenario_engine`: 11
  - `tests/portfolio`: 11
  - `tests/dashboard_consistency`: 9
  - `tests/cross_output`: 5
  - `tests/sector`: 6
  - `tests/release_artifacts`: 3
  - `tests/reproducibility`: 8
  - `tests/traceability`: 6
  - `tests/validation_hardening`: 5
  - `tests` (legacy pipeline): 5
- Marcado por severidad:
  - `blocker`: 22+
  - `critical`: 45+
  - `high`: 28+
  - `warning`: 1

## 4) Matriz test-by-layer
| Layer | Riesgo evitado | Tipos de test implementados |
|---|---|---|
| Unit logic | deriva de fórmula en score compuesto | monotonicity, boundedness, weight sanity |
| Data quality | datos imposibles que sesgan KPIs | grain uniqueness, null-rate, rangos físicos, coherencia emisiones |
| Metric contracts | drift semántico y métricas no gobernadas | registry integrity, contratos runtime, reconciliación SQL-Python |
| Features | señales corruptas o no interpretables | finitud numérica, invariantes de negocio, flags binarios |
| Scores | ranking inestable por ruido | formula reconciliation, perturbation robustness, sensitivity overlap |
| Scenario engine | claims financieros rotos | invariants gross/net/risk, downside/VaR sanity, sensitivity regression |
| Portfolio | selección incoherente | budget/capacity constraints, dependency consistency, wave assignment |
| Dashboard consistency | headline mismatch ejecutivo | snapshot-token reconciliation, governance stamp, top-line consistency |
| Visual safety dashboard | solapes, clipping y charts ilegibles | density caps, layout primitives, overflow-safe KPI, chart readability checks |
| Release artifacts | release incompleto o no auditable | manifest integrity, artifact completeness, committee minimum checks |
| Reproducibility | drift silencioso inter-release | rowcount signatures, temporal coverage signatures |

## 5) Tests clave añadidos por tipo solicitado
- `monotonicity`: `tests/unit/test_scoring_monotonicity.py`
- `invariants`: `tests/data_quality/*`, `tests/scenario_engine/test_scenario_financial_invariants.py`
- `perturbation`: `tests/scores/test_score_stability_and_robustness.py::test_priority_ranking_perturbation_robustness`
- `ranking stability`: sensitivity overlap top-10 entre escenarios
- `sensitivity regression`: shock energético vs base
- `financial reconciliation`: portfolio summary ↔ selected initiatives, gross/net/risk logic
- `property-based logic`: generación aleatoria determinista en monotonicity tests
- `snapshot consistency`: dashboard KPI cards ↔ executive snapshot
- `build artifact completeness`: manifest + artefactos de release
- `committee-readiness consistency`: checks bloqueantes y P1 sin ERROR

## 6) Severidades y release gates
- `blocker`: fallo bloquea merge y release.
- `critical`: fallo bloquea release candidate.
- `high`: no bloquea merge, sí requiere issue + owner + ETA antes de publish ejecutivo.
- `warning`: no bloquea, se registra en backlog QA.

### Reglas de bloqueo operativo
1. **PR gate**:
   - `pytest -m "quick and (blocker or critical)" -q`
2. **Release candidate gate**:
   - `pytest -m "full and (blocker or critical)" -q`
3. **Full assurance (comité)**:
   - `pytest -m full -q`

## 7) Ownership y frecuencia de ejecución
- Data contracts + metric governance: **Analytics Engineering**.
- Financial/scenario/portfolio: **Decision Science + Industrial Finance Analytics**.
- Dashboard consistency: **BI Lead**.
- Release artifacts/reproducibility: **QA Owner + Release Engineering**.

Frecuencia recomendada:
- PR: quick blockers/criticals.
- Nightly: full blockers/criticals/high.
- Release candidate: full suite completa.

## 8) Quick suite vs full suite
- **Quick suite**: tests marcados `quick` (63+ tests). Objetivo: feedback rápido de regresiones críticas.
- **Full suite**: `101 tests` (`full`) para hardening antes de publicación de outputs ejecutivos.

## 9) Política de mantenimiento
- Cada bug en producción analítica debe crear:
  - 1 test de regresión mínimo,
  - etiqueta de severidad,
  - owner de mantenimiento.
- Cualquier cambio en fórmulas de score/KPI requiere actualización de tests de reconciliación y estabilidad antes de merge.
