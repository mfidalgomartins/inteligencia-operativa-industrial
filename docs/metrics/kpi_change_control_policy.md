# KPI Change Control Policy

## Objetivo
Evitar cambios silenciosos de fórmula/grain que rompan comparabilidad inter-release y credibilidad ejecutiva.

## Clasificación de cambios
- **Patch**: ajustes de documentación, sin cambiar fórmula, grano o unidad.
- **Minor**: cambios compatibles en cobertura/filtros permitidos, manteniendo interpretación.
- **Major**: cambios en fórmula, denominador, baseline o interpretación de decisión.

## Reglas obligatorias
1. Todo cambio KPI debe actualizar `contracts/metrics/metric_registry.json`.
2. Todo cambio KPI debe actualizar `docs/metrics/diccionario_metricas.md` y `docs/sql_metric_definitions.md` cuando aplique.
3. Todo cambio KPI debe recalcular snapshots (`executive_kpi_snapshot`, `executive_decision_snapshot`) y reconciliaciones cross-output.
4. Está prohibido introducir una segunda fórmula activa para el mismo KPI certified.
5. Frontend no puede introducir lógica de KPI fuera del dataset canónico.

## Gating de release por cambios KPI
- Cambio `Major` requiere:
  - suite completa de tests
  - validación cross-output en estado sin blockers
  - actualización explícita de caveats en memo/README si cambia interpretación.
- Cambio `Minor` requiere reconciliación SQL vs Python limpia para KPI afectado.

## Evidencia mínima de cierre
- `data/processed/cross_output_consistency.csv`
- `data/processed/metric_contract_validation.csv`
- `outputs/reports/validation_report.md`
- `manifests/build_manifest_latest.json`
