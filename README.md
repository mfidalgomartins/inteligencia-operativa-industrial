# Sistema de Inteligencia Operativa Industrial

Este proyecto es un cockpit de decisión para planta industrial: combina datos de producción, energía, paradas y calidad para explicar pérdidas, estimar valor capturable y priorizar iniciativas con restricciones reales. Está pensado para hablar el lenguaje de dirección de operaciones, energía y mantenimiento sin perder rigor técnico.

## El problema que resuelve
Las plantas toman decisiones de mejora con información fragmentada: energía por un lado, OEE por otro, mantenimiento en otro sistema. El resultado es inversión dispersa y poca trazabilidad de valor. Este sistema ordena la señal, cuantifica el impacto y propone una secuencia ejecutable.

## Qué hace realmente
Integra producción, consumo y fiabilidad en una capa gobernada; calcula KPIs y scores interpretables por línea, equipo y turno; simula escenarios y prioriza iniciativas bajo restricciones de CAPEX/OPEX y capacidad; y entrega un dashboard ejecutivo con trazabilidad y límites explícitos de madurez.

## Decisiones que soporta
- Dónde se pierde más valor y por qué.
- Qué líneas y equipos intervenir primero.
- Qué iniciativas ejecutar ahora vs. siguiente ola.
- Qué cartera cabe en presupuesto y capacidad.

## Arquitectura, en una línea
Raw → SQL (staging/integration/marts) → semantic KPIs → features/scores → scenario & portfolio → dashboard.

## Estructura del repositorio (núcleo)
- `src/` pipeline y lógica analítica
- `sql/` capa SQL oficial
- `data/` raw, processed y samples
- `outputs/` dashboard, charts, reports
- `docs/` arquitectura, governance, validación
- `tests/` QA y consistencia
- `configs/` perfiles sectoriales

## Outputs que importan
- Dashboard: `outputs/dashboard/dashboard_inteligencia_operativa.html`
- Charts: `outputs/charts/`
- Reports clave: `outputs/reports/validation_report.md`, `memo_ejecutivo.md`, `advanced_analysis_report.md`
- Datos y manifests: `data/processed/`, `manifests/`

## Dashboard en vivo
GitHub Pages: https://mfidalgomartins.github.io/inteligencia-operativa-industrial/

## Por qué este proyecto es fuerte
No es solo visualización: hay gobernanza de métricas, trazabilidad de decisión y priorización con restricciones reales. La lógica financiera es explícita y marcada como screening/pre‑feasibility, evitando sobreclaims.

## Cómo ejecutar
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m src
pytest -q
```

## Limitaciones
- Dataset sintético.
- Métricas financieras en modo screening/pre‑feasibility.
- Requiere calibración sectorial con datos reales para comité final.

## Herramientas
Python, SQL, DuckDB, pandas y Chart.js.
