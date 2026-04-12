# Dashboard Serving Architecture

## Objetivo
Mantener capacidad decisional ejecutiva con payload ligero, datasets gobernados y menor fragilidad operativa.

## Modo de serving oficial
1. `canonical_single_html`
- Archivo oficial: `outputs/dashboard/industrial-operating-command-center.html`.
- Uso: comité, dirección industrial, operaciones y energía.
- Principio: KPI + trade-offs + decisión en <3 minutos, con drill-down interactivo controlado.
- Policy de tamaño: <= 4 MB.

## Datasets canónicos
- `data/processed/dashboard_canonical_dataset.json`
- `data/processed/dashboard_serving_audit.csv`

## Reglas de construcción
- No KPI headline calculado en frontend.
- Visuales alimentadas por datasets pre-agregados.
- `run_id`, `snapshot_id`, `metric_version_set`, `decision_model_version`, `validation_state` y `dashboard_mode` obligatorios en HTML.
- Dashboard canónico de release = `industrial-operating-command-center.html`.
- Outputs legacy de dashboard deben eliminarse en build.

## Before/After (refactor)
- Before: varios HTML y datasets de dashboard competían entre sí.
- After: un solo HTML, un solo dataset, una sola ruta de build y controles explícitos de no-competencia.
