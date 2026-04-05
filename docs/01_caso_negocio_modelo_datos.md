# Blueprint Maestro del Sistema

## 1. Contexto del negocio
Plantas industriales intensivas en energía (acero/tubos/refino) enfrentan presión simultánea en coste energético, disponibilidad de activos, estabilidad de proceso, emisiones y cumplimiento de objetivos productivos. El valor se captura al reducir pérdidas recurrentes en líneas y equipos críticos, no únicamente con mejoras puntuales.

## 2. Problema de negocio exacto
Identificar qué combinaciones de **planta-línea-equipo-turno-contexto operativo** concentran mayores pérdidas de eficiencia, sobreconsumo, emisiones y riesgo; explicar causas dominantes; y priorizar una cartera de iniciativas entre quick wins, mantenimiento, optimización y CAPEX.

## 3. Usuarios objetivo del sistema
- Dirección de planta.
- Dirección industrial corporativa.
- Operaciones de línea/turno.
- Energía y utilities.
- Mantenimiento y confiabilidad.
- Excelencia operativa.
- Transformación industrial/capital projects.

## 4. Decisiones que el sistema debe soportar
- Dónde intervenir primero por impacto económico-operativo.
- Qué activos requieren acción preventiva/correctiva.
- Qué causas raíz atacar para maximizar retorno.
- Qué iniciativas ejecutar ahora vs diferir.
- Cuándo justificar CAPEX frente a quick wins.

## 5. Preguntas analíticas prioritarias
- ¿Qué líneas tienen peor brecha SEC vs target y benchmark?
- ¿Qué equipos muestran anomalía energética persistente?
- ¿Qué turnos elevan la varianza operativa?
- ¿Qué causas raíz dominan pérdida de valor?
- ¿Cuál es el ahorro potencial por tipo de iniciativa y su payback?

## 6. Diseño conceptual del modelo de datos
- Capa de **dimensiones**: plantas, líneas, equipos.
- Capa de **hechos operativos**: energía horaria, producción por turno, desvíos, paradas, mantenimiento.
- Capa de **sostenibilidad/economía**: emisiones estimadas, costes proxy.
- Capa de **decisión**: iniciativas, CAPEX, escenarios.

## 7. Lista completa de tablas
`plantas`, `lineas_proceso`, `equipos`, `consumos_energia_horarios`, `produccion_turno`, `desvios_proceso`, `paradas`, `mantenimientos`, `emisiones_estimadas`, `iniciativas_mejora`, `capex_proyectos`, `contexto_operativo`, `benchmark_externo_proxy`, `escenarios_mejora`.

## 8. Grain de cada tabla
- `plantas`: 1 fila/planta.
- `lineas_proceso`: 1 fila/línea.
- `equipos`: 1 fila/equipo.
- `consumos_energia_horarios`: 1 fila/equipo-hora-fuente.
- `produccion_turno`: 1 fila/línea-turno-día.
- `desvios_proceso`: 1 fila/evento desvío.
- `paradas`: 1 fila/evento parada.
- `mantenimientos`: 1 fila/intervención.
- `emisiones_estimadas`: 1 fila/línea-hora-fuente.
- `iniciativas_mejora`: 1 fila/iniciativa.
- `capex_proyectos`: 1 fila/proyecto.
- `contexto_operativo`: 1 fila/planta-turno-día.
- `benchmark_externo_proxy`: 1 fila/periodo-familia.
- `escenarios_mejora`: 1 fila/periodo-escenario.

## 9. Relaciones entre tablas
- `lineas_proceso.planta_id -> plantas.planta_id`.
- `equipos.linea_id -> lineas_proceso.linea_id`.
- Hechos (`consumos`, `produccion`, `desvios`, `paradas`, `mantenimientos`) enlazan por `planta_id`, `linea_id`, `equipo_id` según grano.
- `capex_proyectos.iniciativa_id -> iniciativas_mejora.iniciativa_id`.

## 10. KPIs obligatorios
- SEC medio y gap SEC vs target/benchmark.
- OEE sintético, disponibilidad, utilización.
- Coste energético total y por unidad.
- Emisiones proxy totales e intensidad.
- Pérdida económica proxy por línea/causa.
- Riesgo línea/equipo.
- Ahorro anual potencial y payback.

## 11. Features analíticas obligatorias
- `line_day_features` (SEC, pérdidas, densidades, trends).
- `equipment_day_features` (sobrecarga, drift, riesgo).
- `shift_features` (varianza operativa por turno).
- `improvement_opportunity_features` (ahorro, complejidad, urgencia).

## 12. Scores y marcos de priorización
- `sec_benchmark_gap`, `equipment_energy_anomaly_score`, `process_deviation_risk_score`, `line_criticality_score`, `opportunity_priority_score`.
- Framework de inversión: `energy_saving_score`, `operational_impact_score`, `emissions_reduction_score`, `implementation_feasibility_score`, `payback_score`, `strategic_priority_score`, `improvement_priority_index`.

## 13. Lógica del scenario engine
Escenarios predefinidos (quick wins, mantenimiento, optimización, scrap/rework, electrificación, recuperación energética, CAPEX mayor) aplicados sobre baseline línea con shocks operativos/energéticos para estimar impacto técnico y económico.

## 14. Lógica de priorización de iniciativas
Score compuesto balanceando ahorro, impacto operativo, emisiones, factibilidad, payback y prioridad estratégica. Clasificación de decisión: ejecutar ahora, siguiente ola, analizar técnicamente, pipeline, no prioritaria.

## 15. Outputs esperados
- Datasets raw y processed.
- Marts SQL y vistas KPI.
- Features y scores diagnósticos.
- Escenarios y priorización final.
- 18 gráficos ejecutivos.
- Dashboard HTML autocontenido.
- Informes y memo ejecutivo.

## 16. Estructura completa de carpetas y archivos
- `data/raw/`, `data/processed/`
- `sql/` (11 scripts)
- `src/` (pipelines por dominio)
- `notebooks/`
- `outputs/charts/`, `outputs/dashboard/`, `outputs/reports/`
- `docs/`

## 17. Flujo end-to-end
1. Generación sintética.
2. Profiling `/explore-data`.
3. SQL staging/integration/marts/KPI/validation.
4. Feature engineering.
5. Diagnóstico interpretable.
6. Scenario engine.
7. Priorización de inversión.
8. Análisis avanzado `/analyze`.
9. Visualizaciones `/create-viz`.
10. Dashboard `/build-dashboard`.
11. Validación integral `/validate-data`.
12. Reporting final.

## 18. Riesgos metodológicos, analíticos y de comunicación
- Sobrerreliance en proxies económicos.
- Sensibilidad de rankings a pesos.
- Confundir correlación con causalidad.
- Riesgo de sobre-interpretación de datos sintéticos.
- Comunicación no alineada entre operaciones y finanzas.

## 19. Cómo hacer que parezca trabajo real para Tubacex
- Narrativa centrada en continuidad operacional, hornos/laminación/acabado.
- Indicadores de SEC por familia de proceso y disciplina de turno.
- Priorización de confiabilidad de equipos críticos y recuperación térmica.
- Roadmap quick wins + CAPEX con gating técnico-financiero.

## 20. Adaptación narrativa para Petronor sin rehacer técnica
- Reetiquetar entidades de línea/equipo a unidades de proceso/refino.
- Mantener arquitectura de datos, features y scoring.
- Ajustar catálogo de causas, escenarios y factores de emisión.
- Replantear lenguaje a eficiencia de unidades, utilidades y transición energética.
