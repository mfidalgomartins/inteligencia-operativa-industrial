# Release Readiness Levels

## Niveles y significado
- `publish-blocked`: falla estructural/release bloqueante; no publicar.
- `structurally-valid`: estructura y contratos mínimos correctos; sin calidad analítica suficiente.
- `analytically-acceptable`: análisis consistente, pero sin robustez de decisión suficiente.
- `screening-grade-only`: útil para pre-screening; no apto para business case de comité.
- `decision-support-only`: apto para orientar decisiones operativas, no para aprobación de inversión formal.
- `not-committee-grade`: explícitamente no apto para presentarse como material de comité.
- `committee-candidate`: apto para revisión preliminar de comité con caveats explícitos.
- `committee-grade`: apto para comité sin caveats metodológicos críticos abiertos.

## Reglas de asignación
- Dominios estructural/semántico/release con blocker abierto => `publish-blocked`.
- Debilidad financiera proxy abierta => mínimo `screening-grade-only`.
- Debilidad de optimización abierta => mínimo `decision-support-only`.
- Riesgo de sobreclaim abierto => `not-committee-grade`.

## Publicación
- `publish`: solo `committee-candidate` o `committee-grade`, sin blockers ni críticos abiertos.
- `publish with caveat`: niveles intermedios sin blockers.
- `no publish`: cualquier blocker no waivado.

## Claims permitidos por nivel
- `screening-grade-only`: “pre-screening”, “estimación preliminar”.
- `decision-support-only`: “apoyo a priorización operativa”.
- `not-committee-grade`: prohibido usar lenguaje de aprobación de inversión.
- `committee-candidate`: permitido “candidato a revisión de comité”, con limitaciones explícitas.
- `committee-grade`: permitido “material defendible de comité”.
