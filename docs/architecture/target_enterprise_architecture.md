# Arquitectura Target Enterprise

## Capas
1. `raw` (inmutable por lote)
2. `staging` (conformance/tipado)
3. `integration` (granos analíticos intermedios)
4. `semantic_marts` (single source of truth KPI)
5. `features`
6. `scores`
7. `decision`
8. `executive_serving`

## Boundaries
- `raw`: sin lógica de negocio.
- `semantic_marts`: única capa autorizada para KPI oficiales.
- `scores`: cuantifica señales, no decide ejecución.
- `decision`: aplica reglas, restricciones y trade-offs.
- `executive_serving`: solo consume salidas gobernadas.

## Principios operativos
- Contract-first para tablas y métricas.
- Grain explícito y testeado.
- Reconciliación entre SQL y Python obligatoria.
- Release con gates de calidad.
- Trazabilidad desde dato crudo hasta recomendación.

## Evolución futura
- Compatible con estructura `dbt-like` por capas.
- Preparado para conectores Historian/MES/ERP/CMMS.
