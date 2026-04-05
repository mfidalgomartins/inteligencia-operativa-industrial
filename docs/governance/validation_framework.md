# Validation and Quality Framework (Updated)

Este documento queda como índice corto. La definición completa está en:
- `docs/validation/validation_hardening_framework.md`
- `docs/validation/release_readiness_levels.md`
- `docs/validation/exceptions_and_waivers_policy.md`

## Dominios activos
1. structural validity
2. semantic validity
3. analytical validity
4. financial validity
5. decision validity
6. release validity
7. committee-readiness validity

## Artefactos principales
- `data/processed/validation_checks_hardened.csv`
- `data/processed/validation_domain_assessment.csv`
- `data/processed/validation_readiness_summary.csv`
- `data/processed/validation_waivers_status.csv`
- `data/processed/validation_exceptions_active.csv`
- `outputs/reports/validation_report.md`

## Regla de publicación
- `no publish` con blockers no waivados.
- `publish with caveat` cuando no hay blockers pero el readiness es sub-comité.
- `publish` solo con readiness de comité y evidencia suficiente.
