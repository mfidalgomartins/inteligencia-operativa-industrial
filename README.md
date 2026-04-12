# Sistema de Inteligencia Operativa Industrial

Plataforma analítica para identificar pérdidas de eficiencia y energía, explicar causas raíz y priorizar iniciativas con lógica económica y de riesgo.

## Problema de negocio
Las plantas industriales pierden valor por ineficiencia, consumo energético y paradas no explicadas. Sin un sistema integrado, se ejecutan mejoras sin foco ni secuencia óptima.

## Qué hace el sistema
- Integra producción, energía, paradas, desvíos y emisiones en una capa gobernada.
- Calcula KPIs y scores interpretables por línea, equipo y turno.
- Simula escenarios y prioriza iniciativas bajo restricciones reales.
- Entrega un cockpit ejecutivo con trazabilidad y límites explícitos de madurez.

## Decisiones que soporta
- Qué líneas y equipos intervenir primero.
- Qué causas raíz explican más pérdida y coste.
- Qué iniciativas ejecutar ahora vs en siguiente ola.
- Qué cartera cabe en presupuesto y capacidad.

## Arquitectura (alto nivel)
- Datos raw → SQL (staging/integration/marts)
- Semantic layer (SSOT KPIs)
- Features + scores
- Scenario & portfolio
- Dashboard ejecutivo

## Estructura del repositorio
- `src/` núcleo analítico y pipeline
- `sql/` capa SQL oficial
- `data/` raw, processed y samples
- `outputs/` dashboard, reports, charts
- `docs/` arquitectura, governance y metodología
- `tests/` QA y validaciones
- `configs/` perfiles sectoriales

## Outputs principales
- Dashboard: `outputs/dashboard/dashboard_inteligencia_operativa.html`
- Charts: `outputs/charts/`
- Reportes clave: `outputs/reports/validation_report.md`, `memo_ejecutivo.md`, `advanced_analysis_report.md`
- Datos procesados y manifests en `data/processed/` y `manifests/`

## Por qué este proyecto es fuerte
- End-to-end real: datos → KPIs → decisión → cockpit ejecutivo.
- Gobernanza y trazabilidad explícitas.
- Priorización con restricciones reales y supuestos visibles.

## Cómo ejecutar
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m src
pytest -q
```

## Limitaciones
- Datos sintéticos (no históricos reales).
- Métricas financieras en modo screening/pre-feasibility.
- Requiere calibración sectorial con datos reales para comité final.

## Herramientas
Python, SQL, DuckDB, pandas y Chart.js.
