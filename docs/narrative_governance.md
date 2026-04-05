# Narrative Governance

## Propósito
Evitar overclaim y asegurar que README, memo, dashboard y reportes usen el mismo nivel de madurez metodológica.

## Principios de narrativa
1. Cada claim técnico/financiero debe mapearse a evidencia trazable en artefactos.
2. Si una métrica es proxy, se etiqueta como proxy en todas las capas.
3. Si una capacidad depende de supuestos sintéticos, no se presenta como “cerrada”.
4. No usar lenguaje de aprobación final cuando el nivel sea screening/pre-feasibility.
5. Narrativa ejecutiva y narrativa técnica deben ser consistentes con `validation_readiness_summary`.

## Auditoría aplicada
- Fuentes auditadas:
  - `README.md`
  - `outputs/reports/memo_ejecutivo.md` (y su fuente `src/reporting.py`)
  - `outputs/dashboard/dashboard_inteligencia_operativa.html` (y su fuente `src/dashboard.py`)
- Matriz de claims:
  - `docs/claims_matrix.md`

## Cambios de hardening aplicados
- README reposicionado como:
  - governed industrial decision platform,
  - pre-screening and prioritization engine,
  - committee-support con límites explícitos.
- Memo ejecutivo:
  - incorpora alcance decisional explícito (`What this supports / What this does not replace`).
  - refuerza caveats financieros y de datos sintéticos.
- Dashboard:
  - wording de soporte a comité, no aprobación final.
  - métricas financieras señaladas como proxy/screening.

## Reglas de publicación narrativa
- No publicar frase “business case final” salvo evidencia externa auditada.
- No publicar “committee-grade” sin readiness correspondiente y sin issues críticos abiertos.
- Todo claim de ahorro debe indicar:
  - horizonte temporal,
  - si es proxy o auditado,
  - dependencia de supuestos.

## Gate narrativo mínimo antes de release
1. `docs/claims_matrix.md` actualizado.
2. README y memo sin claims clasificados como `too strong` o `should be removed`.
3. Dashboard con disclaimer de madurez.
4. Coherencia con `validation_report.md` y `validation_readiness_summary.csv`.
