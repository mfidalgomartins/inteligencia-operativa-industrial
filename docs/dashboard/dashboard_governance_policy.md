# Dashboard Governance Policy

## 1) Scope
Esta política aplica a todos los dashboards ejecutivos publicados por el pipeline del proyecto.

## 2) Single source of truth
- KPI headline únicamente desde `executive_kpi_snapshot.csv`.
- Dataset de dashboard únicamente desde `dashboard_canonical_dataset.json`.
- Prohibido recalcular KPI críticos en JavaScript.

## 3) Version stamping obligatorio
El dashboard oficial debe incluir:
- `run_id`
- `snapshot_id`
- `metric_version_set`
- `decision_model_version`
- `validation_state`
- `dashboard_mode`

## 4) Payload policy
- Dashboard oficial canónico: máximo 4 MB.
- Si supera threshold: release con `publish-blocked` o `publish with caveat` según severidad.

## 5) Release checks mínimos
- Existencia de un único HTML oficial (`industrial-operating-command-center.html`).
- Ausencia de HTMLs legacy activos (`dashboard_executive_light.html`, `dashboard_full_offline.html`, `dashboard_board_pack.html`).
- Ausencia de datasets legacy activos (`dashboard_exec_dataset.json`, `dashboard_full_dataset.json`).
- Alineación de stamping con snapshot.
- Checks de validación: `dashboard_payload_thresholds`, `dashboard_version_stamping`, `dashboard_serving_audit_consistency`.

## 6) Cambios permitidos
- Añadir visuales solo si cambian decisión.
- Eliminar visuales redundantes preferido sobre incrementar payload.
- Cambios de estructura requieren actualizar tests y `dashboard_serving_audit.csv`.
