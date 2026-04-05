# Arquitectura del Dashboard (Canónica)

## Objetivo
Tener un único dashboard oficial, gobernado y consistente con métricas certificadas y snapshots ejecutivos.

## Implementación oficial
- Generador oficial único: `src/dashboard.py` (`build_dashboard`).
- HTML oficial único: `outputs/dashboard/dashboard_inteligencia_operativa.html`.
- Dataset oficial único: `data/processed/dashboard_canonical_dataset.json`.
- Auditoría de serving: `data/processed/dashboard_serving_audit.csv`.
- Reporte de refactor de serving: `outputs/reports/dashboard_serving_refactor.md`.

## Secciones obligatorias
1. Header ejecutivo con stamping (`run_id`, `snapshot_id`, `metric_version_set`, `decision_model_version`, `validation_state`, `dashboard_mode`).
2. KPI cards canónicas desde snapshot.
3. Vista de eficiencia operativa.
4. Vista energética y coste.
5. Vista de proceso y causas raíz.
6. Vista de cartera y riesgo.
7. Tabla final interactiva de priorización.
8. Decisión ejecutiva final.

## Serving policy
- Tamaño máximo HTML canónico: `<= 4 MB`.
- Prohibido recalcular KPIs headline en frontend.
- Visuales alimentadas por dataset canónico pre-agregado.
- Eliminación automática de artefactos legacy (`executive_light`, `full_offline`, `board_pack`).

## Uso
1. Ejecutar `python -m src`.
2. Abrir `outputs/dashboard/dashboard_inteligencia_operativa.html`.
3. Validar flags de serving en `data/processed/dashboard_serving_audit.csv`.
