# Traceability Operating Model

## Flujo operativo
1. Pipeline genera datasets y scores.
2. Validación endurecida fija `readiness_level` y `caveat_codes`.
3. `build_traceability_layer` genera traza recomendación-céntrica.
4. Release manifest se enriquece con estado de trazabilidad y dependencias de versión.

## Controles operativos
- **Trace completeness**: toda iniciativa en priorización debe aparecer en `recommendation_trace_table`.
- **Version consistency**: `run_id` y `metric_version_set` consistentes entre snapshot, trace y manifest.
- **DAG acyclicity**: `lineage_edges` sin ciclos.
- **Metric contract match**: métricas en lineage incluidas en registry.
- **Score traceability**: scores de decisión mapeados a métricas.
- **Executive traceability**: outputs ejecutivos mapeados a datasets/métricas/scores.

## Responsabilidades
- Analytics Engineering: mantenimiento de DAG y mapas metric-score.
- KPI Governance Lead: integridad de contratos métricos.
- BI Lead: trazabilidad de outputs ejecutivos.
- QA/Release: bloqueo de publicación si trazabilidad está incompleta.

## Política de publicación
- Si falla traceability crítica: `publish-blocked`.
- Si traceability está completa pero con caveats metodológicos: `publish with caveat`.
- Solo `committee-candidate`/`committee-grade` con traza completa y sin blockers.

## Artefacto de evidencia before/after
- `outputs/reports/recommendation_trace_before_after.md`
