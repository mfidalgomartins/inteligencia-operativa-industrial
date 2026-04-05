# Diccionario de Datos

## Dominios base (raw)
- `plantas`: catálogo de plantas industriales y estrategia de mejora.
- `lineas_proceso`: catálogo de líneas con capacidad, objetivos OEE/SEC y criticidad.
- `equipos`: catálogo de equipos por línea con potencia nominal, eficiencia y criticidad.
- `consumos_energia_horarios`: consumo y coste por equipo-hora-fuente.
- `produccion_turno`: producción por línea-turno con horas planificadas/operativas y calidad.
- `desvios_proceso`: eventos de desviación de variables críticas de proceso.
- `paradas`: eventos de parada planificados/no planificados con impacto proxy.
- `mantenimientos`: intervenciones de mantenimiento y resultado.
- `emisiones_estimadas`: emisiones proxy por línea-hora-fuente.
- `iniciativas_mejora`: cartera de iniciativas operativas, mantenimiento, proceso y energía.
- `capex_proyectos`: dimensión económica de iniciativas (CAPEX, ahorro, payback, riesgo).
- `contexto_operativo`: demanda, complejidad, temperatura y precio de energía por turno.
- `benchmark_externo_proxy`: referencias externas por familia de proceso.
- `escenarios_mejora`: shocks y restricciones para simulación de escenarios.

## Dominios SQL integrados (processed)
- `int_line_hour`: integración línea-hora de energía, producción, emisiones, desvíos, paradas y pérdida.
- `int_equipment_day`: integración equipo-día de consumo, disponibilidad, mantenimiento y riesgo.
- `int_line_shift`: integración línea-turno para variabilidad operacional.

## Analytical marts
- `mart_line_day`: desempeño energético-operativo línea-día.
- `mart_equipment_day`: eficiencia y riesgo de equipo-día.
- `mart_plant_week`: consolidado planta-semana.

## Features
- `line_day_features`: señales línea-día para benchmarking, pérdidas y tendencia.
- `equipment_day_features`: señales equipo-día para anomalías y degradación.
- `shift_features`: señales de varianza operacional por turno.
- `improvement_opportunity_features`: señales técnico-económicas por iniciativa.

## Scoring/decision
- `line_day_features_scored`
- `equipment_energy_anomaly_scores`
- `loss_root_cause_score`
- `line_criticality_scores`
- `opportunity_priority_scores`
- `scenario_ranking`
- `investment_prioritization_final`
- `validation_checklist_final`
