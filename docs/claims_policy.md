# Claims Policy

## Objetivo
Estandarizar qué se puede afirmar públicamente según la madurez real del sistema.

## Política por tipo de claim

### 1) Claims técnicos
- Permitido:
  - “capa SQL gobernada”
  - “semantic layer con contratos de métricas”
  - “trazabilidad recommendation-centric”
- Condicionado:
  - “enterprise-grade” solo con caveat de alcance (piloto interno / committee-support).

### 2) Claims financieros
- Permitido:
  - “screening financiero”
  - “pre-feasibility analytics”
  - “estimación proxy de ahorro/NPV ajustado por riesgo”
- Prohibido sin evidencia externa:
  - “valor financiero final aprobado”
  - “IRR corporativa final”
  - “investment case cerrado”

### 3) Claims de optimización
- Permitido:
  - “optimización formal para la formulación actual”
  - “selección sujeta a restricciones modeladas”
- Prohibido:
  - “cartera globalmente óptima en cualquier condición”
  - “frontera eficiente corporativa cerrada” sin solver/proceso formal validado externamente.

### 4) Claims de comité
- Permitido:
  - “committee-support material”
  - “committee-candidate” si readiness y validación lo soportan.
- Prohibido:
  - “committee-grade final” por defecto.

### 5) Claims sectoriales
- Permitido:
  - “adaptación técnica parametrizada por configuración”
- Condicionado:
  - siempre aclarar que la calibración final requiere datos reales del sector.

## Tabla de wording recomendado
| Evitar | Usar |
|---|---|
| “NPV final” | “NPV de screening (proxy)” |
| “optimización óptima definitiva” | “optimización formal para la formulación actual” |
| “listo para decisión de inversión final” | “soporte para pre-screening y priorización” |
| “adaptación completa a cualquier sector” | “adaptación parametrizada con calibración pendiente” |

## Enforcement
- Cada release debe revisar esta policy junto con:
  - `docs/claims_matrix.md`
  - `docs/narrative_governance.md`
  - `docs/committee_safe_positioning.md`
