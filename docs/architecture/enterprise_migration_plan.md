# Plan de Migración a Plataforma Enterprise

## Estado actual reforzado
- Pipeline con `run_id` y manifest por ejecución.
- Scenario engine con incertidumbre, downside y VaR.
- Priorización bajo restricciones de cartera.
- Contratos de datos/métricas y checks cruzados.

## Próximos hitos
1. Externalizar contratos en YAML versionado con changelog formal.
2. Integrar CI con gates de release (`candidate`/`prod`).
3. Añadir conectores a fuentes reales (Historian/MES/ERP/CMMS).
4. Migrar SQL layer a estructura dbt-like manteniendo semantic layer.

## Criterios de éxito
- Trazabilidad completa desde raw hasta decisión ejecutiva.
- Sin discrepancias KPI entre SQL, scoring, dashboard y reportes.
- Portfolio recomendado reproducible bajo escenarios definidos.
