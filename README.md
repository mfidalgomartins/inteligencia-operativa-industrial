# Sistema de Inteligencia de Eficiencia Operativa, Consumo Energético, Riesgo de Proceso y Priorización de Iniciativas en Planta Industrial

Plataforma analítica industrial gobernada para **pre-screening, diagnóstico y priorización de iniciativas** en una compañía tipo **Tubacex**, con adaptación técnica parametrizada a contexto de refino como **Petronor**.

## Executive Overview
Este proyecto transforma datos operativos y energéticos en una **cartera priorizada de iniciativas** con lógica económica y de riesgo orientada a decisión.

Diferencial clave: no se queda en visualización o scoring; integra una capa gobernada end-to-end:
- semantic KPI layer gobernada,
- scenario engine multi-escenario con incertidumbre,
- priorización con restricciones reales de portfolio,
- dashboard ejecutivo orientado a comité (con límites explícitos de madurez).

## Positioning
- **What this is**: governed industrial decision platform para diagnóstico, pre-feasibility y secuenciación de iniciativas.
- **What this is not**: sustituto de un business case corporativo final, de ingeniería de detalle o de aprobación financiera formal standalone.

## Business Problem
¿Qué líneas/equipos concentran mayores pérdidas de eficiencia, coste, emisiones y riesgo operativo, qué causas las explican y qué iniciativas conviene ejecutar primero bajo restricciones de ejecución y presupuesto?

## What This Project Demonstrates
- Framing industrial orientado a decisión.
- Gobernanza de métricas y contratos de datos.
- SQL multicapa + Python analítico + capa ejecutiva consistente.
- Priorización robusta para screening (valor bruto/neto/risk-adjusted, downside y restricciones).

## Architecture (Governed)
Capas objetivo implementadas:
1. `raw`
2. `staging`
3. `integration`
4. `semantic_marts` (SSOT KPI)
5. `features`
6. `scores`
7. `decision` (scenario + prioritization)
8. `executive_serving` (dashboard/reportes)

Documentación:
- `docs/architecture/target_enterprise_architecture.md`
- `docs/governance/ownership_and_release_model.md`
- `docs/governance/metric_governance_policy.md`
- `docs/metrics/metric_enforcement_framework.md`
- `docs/metrics/kpi_change_control_policy.md`
- `docs/governance/validation_framework.md`
- `docs/release/reproducibility_forensics_framework.md`
- `docs/release/release_operating_model.md`
- `docs/release/repository_packaging_policy.md`

## Data + Contracts
- Datos sintéticos industriales: 14 tablas raw.
- Contratos:
  - `contracts/data/data_contracts.json`
  - `contracts/metrics/metric_registry.json`

Outputs de governance:
- `data/processed/semantic_metric_registry.csv`
- `data/processed/metric_contract_validation.csv`
- `data/processed/cross_output_consistency.csv`
- `data/processed/executive_kpi_snapshot.csv`

Parametrización sectorial gobernada:
- `configs/sectors/common_core.json`
- `configs/sectors/tubacex_profile.json`
- `configs/sectors/petronor_profile.json`
- `data/processed/sector_profile_registry.csv`
- `data/processed/sector_mapping_coverage.csv`
- `data/processed/sector/<profile_id>/...`

## SQL Layer (DuckDB)
Scripts oficiales:
- `01_staging_energy.sql`
- `02_staging_production.sql`
- `03_staging_events.sql`
- `04_integrated_line_hour.sql`
- `05_integrated_equipment_day.sql`
- `06_integrated_line_shift.sql`
- `07_analytical_mart_line_day.sql`
- `08_analytical_mart_equipment_day.sql`
- `09_analytical_mart_plant_week.sql`
- `10_kpi_queries.sql`
- `11_validation_queries.sql`

## Scenario Engine (Redesigned)
Ahora modela:
- baseline por línea,
- macro-escenarios (`Base`, `ShockEnergia`, `AltaDemanda`, `RestriccionOperativa`),
- ramp-up y start delay,
- probabilidad de éxito,
- dependencias/conflictos,
- valor bruto/neto/risk-adjusted,
- NPV/IRR/VaR **de screening**, payback descontado, downside y sensibilidad.

Outputs:
- `data/processed/scenario_table.csv`
- `data/processed/scenario_ranking.csv`
- `data/processed/scenario_dependencies.csv`
- `data/processed/scenario_conflicts.csv`
- `data/processed/scenario_waterfall_value.csv`

## Investment Prioritization (Redesigned)
Incluye:
- clasificación de iniciativas (`discretionary`, `mandatory`, `compliance-driven`, `reliability-protection`),
- índice multicriterio reforzado,
- optimización de cartera bajo límites CAPEX/OPEX/capacidad en la formulación actual,
- recomendación por olas (`OLA_1`, `OLA_2`, `OLA_3`, `BACKLOG`).

Outputs:
- `data/processed/investment_prioritization_final.csv`
- `data/processed/portfolio_recommendation.csv`
- `data/processed/portfolio_summary.csv`
- `data/processed/investment_prioritization_sensitivity.csv`

## Dashboard (Decision Cockpit)
Archivo final:
- `outputs/dashboard/dashboard_inteligencia_operativa.html`

Incluye:
- KPI ejecutivo + metadata (`run_id`, `metric_version_set`),
- vistas operativa/energética/fiabilidad/emisiones,
- robustez de valor y riesgo de inacción,
- encaje presupuestario de cartera,
- tabla interactiva de priorización con tiers y olas.
- advertencias explícitas de uso: soporte de comité, no aprobación final.

## Validation + Release Discipline
- Validación end-to-end en `src/validate_project.py`.
- Manifiesto de build y estado por fases:
  - `manifests/build_manifest_latest.json`
  - `outputs/reports/phase_transformation_status.md`
  - `outputs/reports/validation_report.md`

Narrativa y fronteras de claim:
- `docs/archive/claims/` (política de claims y posicionamiento histórico consolidado)

## Sector Adaptation (Technical, Not Only Narrative)
- Núcleo común estable + perfiles sectoriales versionados por configuración.
- Adaptación técnica de:
  - taxonomía de procesos/activos,
  - benchmark por familia,
  - factores de emisiones,
  - librería de iniciativas,
  - lógica de criticidad,
  - constraints y sensibilidades sectoriales.
- Documentación:
  - `docs/sector_parameterization_framework.md`
  - `docs/tubacex_operating_profile.md`
  - `docs/petronor_operating_profile.md`

## Key Findings (Sample Run on Synthetic Data)
<!-- AUTO:KEY_FINDINGS:START -->
- Línea más crítica: **PLT_BIZ_L04** (`line_criticality_score = 27.29`).
- Equipo más anómalo: **PLT_BIZ_L02_EQ06** (`equipment_energy_anomaly_score = 61.62`).
- Ahorro anual risk-adjusted **proxy** total: **28,041,632.26 EUR/año**.
- NPV cartera ajustada por riesgo (**screening**): **50,763,712.71 EUR**.
- Valor downside-adjusted de cartera (**screening**): **55,883,365.82 EUR**.
- Readiness actual de validación: **screening-grade-only** (publish_decision: **publish with caveat**).
<!-- AUTO:KEY_FINDINGS:END -->

## Decisión Ejecutiva Clara
<!-- AUTO:EXEC_DECISION:START -->
- Línea a intervenir primero: **PLT_BIZ_L04**.
- Equipo a priorizar: **PLT_BIZ_L02_EQ06**.
- Ahorro potencial de actuar ahora (**estimación proxy top-5**): **2,745,345.66 EUR/año**.
- Riesgo si se retrasa: aumento de pérdida proxy acumulada y degradación de criticidad en nodos top.
- Cuándo quick win: captura de valor <12 meses con bajo burden.
- Cuándo CAPEX: brecha estructural SEC/OEE con criticidad alta persistente.
<!-- AUTO:EXEC_DECISION:END -->

## Repository Structure
- `README.md`
- `requirements.txt` / `requirements.lock.txt`
- `.gitignore`
- `src/` (núcleo de pipeline y lógica analítica)
- `data/` (`raw/`, `processed/`, `samples/`)
- `sql/` (layer SQL oficial + `legacy/` histórico aislado)
- `docs/` (arquitectura, governance, validación, narrativa)
- `tests/` (QA por dominio y checks cross-output)
- `outputs/` (dashboard, reports, charts, release packs generados)
- `notebooks/` (exploración y narrativa analítica)
- `configs/` (perfiles sectoriales y release config)
- `scripts/` (entrypoints operativos standalone)
- `contracts/` (data/metric contracts y waivers)
- `manifests/` (trazabilidad de build y release discipline)

## How to Run
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m src
pytest -q
```

### Reproducibilidad estricta (forense)
```bash
pip install -r requirements.lock.txt
python -m src
pytest -q
```

### Modos de packaging
- `minimal_public`: pack ligero para portfolio público.
- `github_reviewer`: pack orientado a revisión técnica.
- `committee_pack`: pack orientado a comité industrial, con **gating automático por readiness** (puede quedar bloqueado con `README_BLOCKED.md`).

Artefactos:
- `outputs/release_packs/*`
- `manifests/environment_fingerprint.json`
- `manifests/artifact_fingerprints_latest.csv`

## Limitations
- Dataset sintético (plausible, no histórico real).
- IRR/VaR reportados como proxies de priorización.
- La selección usa optimización branch-and-bound sobre formulación binaria; si el estado es `time_limited_best_feasible`, la solución se comunica como aproximación semi-formal.
- Adaptabilidad sectorial implementada en capa de configuración (taxonomía/benchmark/emisiones/constraints), pendiente de calibración con datos reales por sector.

## Why This Is Relevant for Tubacex
- Prioriza líneas y equipos térmico-mecánicos críticos.
- Conecta SEC, disponibilidad y coste en una misma lógica de decisión.
- Permite secuenciar quick wins y CAPEX en cartera defendible.

## How to Reinterpret for Petronor
- Mantener arquitectura técnica y semantic layer.
- Activar `petronor_profile` para aplicar mapping, benchmark, emisiones, constraints y caveats sectoriales.
- Consumir datasets sectoriales en `data/processed/sector/petronor/` para reporting y cockpit ejecutivo.
