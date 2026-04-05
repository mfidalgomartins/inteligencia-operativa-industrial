# Metric Governance Policy

## Principios
- Single source of truth en semantic marts.
- Una fórmula oficial por KPI.
- Agregaciones válidas explícitas por métrica.
- Cálculos prohibidos en frontend.

## Convenciones
- Unidad explícita en nombre o diccionario (`_kwh`, `_eur`, `_kgco2e`).
- Grain declarado y testeado.
- Todo KPI ejecutivo debe registrar `metric_version`.

## Controles obligatorios
- Reconciliación SQL vs Python para KPIs críticos.
- Reconciliación dashboard vs report.
- Tolerancia de discrepancia documentada por métrica.

## Cambios de versión
- Patch: ajuste documental sin cambio de fórmula.
- Minor: cambio compatible de cobertura o atributo auxiliar.
- Major: cambio de fórmula, denominador o interpretación.
