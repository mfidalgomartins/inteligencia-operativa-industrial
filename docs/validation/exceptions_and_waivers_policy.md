# Exceptions and Waivers Policy

## Objetivo
Permitir publicación controlada sin ocultar riesgos metodológicos.

## Contrato de waiver
Archivo: `contracts/waivers/waivers_active.csv`

Campos obligatorios:
- `waiver_id`
- `issue_id`
- `motivo`
- `owner`
- `fecha_caducidad`
- `nivel_maximo_permitido_uso`
- `plan_cierre`
- `estado`

## Reglas de validez
Un waiver solo aplica si:
1. `estado` es `activo`.
2. No está caducado (`fecha_caducidad >= fecha de validación`).
3. El nivel de uso solicitado no supera `nivel_maximo_permitido_uso`.

Estados de evaluación del waiver:
- `applied`
- `expired`
- `inactive`
- `invalid_expiry`
- `scope_exceeded`
- `no_waiver`

## Política de uso
- Waiver no elimina el issue: lo marca como `WAIVED` con trazabilidad.
- Waivers caducados se reportan como riesgo de release.
- Ningún waiver puede elevar automáticamente a `committee-grade`.
- Cada waiver debe tener plan de cierre y owner explícito.

## Disciplina operativa
- Revisar waivers en cada release candidate.
- Rechazar publicación si existe blocker no waivado.
- Documentar en el validation report todos los waivers activos.
