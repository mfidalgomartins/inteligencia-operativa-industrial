# Memo Ejecutivo (Screening y Priorización Industrial)

## 1. Contexto
La planta opera bajo presión combinada de coste energético, disponibilidad y variabilidad de proceso.

## 2. Problema
La pérdida de eficiencia y confiabilidad se concentra en nodos concretos (línea-equipo-turno), afectando coste y riesgo operativo.

## 3. Enfoque metodológico
Plataforma analítica gobernada (SQL + features + scoring interpretable + escenarios + priorización) para pre-screening y secuenciación de iniciativas.

## 3.1 Alcance decisional explícito
- Este sistema soporta priorización operativa y pre-feasibility.
- Este sistema NO reemplaza un business case corporativo final ni la aprobación financiera formal de comité.

## 4. Hallazgos principales
|   prioridad | hallazgo                                                            | impacto                                                        |
|------------:|:--------------------------------------------------------------------|:---------------------------------------------------------------|
|           1 | La línea PLT_BIZ_L04 concentra la mayor criticidad integrada.       | Riesgo alto de pérdida económica y degradación de continuidad. |
|           2 | El equipo PLT_BIZ_L02_EQ06 lidera el riesgo de anomalía energética. | Penaliza coste energético y estabilidad operacional.           |
|           3 | La causa raíz dominante es InestabilidadTorque.                     | Explica la mayor fracción de pérdida operativa agregada.       |
|           4 | El turno C en PLT_BIZ_L02 muestra mayor varianza operativa.         | Necesita estandarización operativa y control de rutina.        |

## 5. Implicaciones operativas
Priorizar la línea **PLT_BIZ_L04** por su criticidad compuesta.
Intervenir el equipo **PLT_BIZ_L02_EQ06** para contener sobreconsumo y riesgo de fallo.

## 6. Implicaciones energéticas
La brecha SEC persistente sugiere pérdidas estructurales en régimen operativo y disciplina de proceso.

## 7. Implicaciones económicas
Actuar en iniciativas de ejecución inmediata captura un ahorro potencial **proxy** aproximado de **2,745,346 EUR/año**.
Madurez financiera del pipeline: screening=28, pre-feasibility=12, business-case-candidate=8.
NPV/IRR/VaR se usan con propósito de screening y priorización; no equivalen a una valoración financiera corporativa auditada.
El modelo es apto para screening/pre-feasibility; no reemplaza business case corporativo auditado.
Estado de release: readiness=screening-grade-only, committee_state=not-committee-grade, publish_decision=publish with caveat.

## 8. Implicaciones ambientales
La reducción de SEC y paradas reduce simultáneamente emisiones proxy por unidad producida.

## 9. Trade-offs principales
- Ahorro rápido (quick wins) vs transformación estructural (CAPEX).
- Payback corto vs criticidad alta.
- Viabilidad de ejecución vs impacto estratégico.

## 10. Prioridades de mejora
1. Ejecutar quick wins y mantenimiento en líneas críticas (ola 1).
2. Consolidar optimización de proceso en turnos con mayor varianza (ola 2).
3. Preparar CAPEX en iniciativas con impacto estructural y gating técnico-financiero (ola 3).

## 11. Limitaciones
- El modelo utiliza datos sintéticos y proxies económicos/ambientales.
- La optimización de cartera es formal para su formulación actual, pero sigue sujeta a supuestos de datos sintéticos y restricciones modeladas.
- La adaptación sectorial está parametrizada en taxonomía/benchmark/factores/constraints; la validación final requiere datos reales de planta/refino.
- Si `publish_decision` es `no publish` o `publish with caveat`, no debe presentarse como material final de comité.

## 12. Próximos pasos
Validar supuestos con operaciones, energía y mantenimiento; ejecutar pilotos controlados por línea crítica y cerrar business cases de iniciativas top antes de comité final.