# Validation Hardening Framework

## Propósito
Evitar falsa seguridad en proyectos analíticos industriales separando explícitamente:
- calidad técnica,
- calidad metodológica,
- calidad de decisión,
- calidad de comité.

## Dominios obligatorios
1. structural validity
2. semantic validity
3. analytical validity
4. financial validity
5. decision validity
6. release validity
7. committee-readiness validity

## Estados de readiness
Estados implementados y válidos:
- `publish-blocked`
- `structurally-valid`
- `analytically-acceptable`
- `screening-grade-only`
- `decision-support-only`
- `not-committee-grade`
- `committee-candidate`
- `committee-grade`

## Regla anti-falsa-confianza
- `committee_readiness_domain_state` captura el resultado local del dominio committee-readiness.
- `committee_readiness_state` es el estado **efectivo** para publicación y puede degradarse respecto al estado local.
- Si `overall_readiness_level` está por debajo de comité (por ejemplo `screening-grade-only` o `decision-support-only`), el estado efectivo se fuerza a `not-committee-grade`.
- `committee_presentation_allowed=1` solo cuando:
  - `committee_readiness_state` es `committee-candidate` o `committee-grade`,
  - `publish_decision=publish`,
  - `unwaived_blockers=0`.

## Controles clave endurecidos
- Proxy risk financiero explícito.
- Overclaim risk en narrativa vs evidencia.
- Metric drift snapshot vs cálculo canónico.
- Score instability por sensibilidad de ranking.
- Optimization weakness por ausencia de evidencia OR/MILP.
- Dashboard/report inconsistency.
- Release incompleteness.
- Waiver expiry y scope enforcement.

## Artefactos generados
- `data/processed/validation_checks_hardened.csv`
- `data/processed/validation_checklist_final.csv`
- `data/processed/validation_issues_found.csv`
- `data/processed/validation_domain_assessment.csv`
- `data/processed/validation_readiness_summary.csv`
- `data/processed/validation_waivers_status.csv`
- `data/processed/validation_exceptions_active.csv`
- `data/processed/validation_top_open_risks.csv`
- `outputs/reports/validation_report.md`

## Decisión de publicación
Regla implementada:
- `no publish` si hay blockers no waivados.
- `publish` solo para `committee-candidate`/`committee-grade` sin críticos abiertos.
- `publish with caveat` para el resto de escenarios.

## Principio de honestidad metodológica
Si el proyecto mantiene proxy financiero o debilidad de optimización, el framework no permite etiquetarlo como `committee-grade`.
