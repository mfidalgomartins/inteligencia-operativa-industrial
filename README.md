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
- Dashboard: `outputs/dashboard/industrial-operating-command-center.html`
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

<!-- AUTO:KEY_FINDINGS:START -->
- Línea más crítica: **PLT_BIZ_L04** (`line_criticality_score = 27.29`).
- Equipo más anómalo: **PLT_BIZ_L02_EQ06** (`equipment_energy_anomaly_score = 61.62`).
- Ahorro anual risk-adjusted **proxy** total: **28,041,632.26 EUR/año**.
- NPV cartera ajustada por riesgo (**screening**): **50,763,712.71 EUR**.
- Valor downside-adjusted de cartera (**screening**): **55,883,365.82 EUR**.
- Readiness actual de validación: **screening-grade-only** (publish_decision: **publish with caveat**).
<!-- AUTO:KEY_FINDINGS:END -->

<!-- AUTO:EXEC_DECISION:START -->
- Línea a intervenir primero: **PLT_BIZ_L04**.
- Equipo a priorizar: **PLT_BIZ_L02_EQ06**.
- Ahorro potencial de actuar ahora (**estimación proxy top-5**): **2,745,345.66 EUR/año**.
- Riesgo si se retrasa: aumento de pérdida proxy acumulada y degradación de criticidad en nodos top.
- Cuándo quick win: captura de valor <12 meses con bajo burden.
- Cuándo CAPEX: brecha estructural SEC/OEE con criticidad alta persistente.
<!-- AUTO:EXEC_DECISION:END -->
