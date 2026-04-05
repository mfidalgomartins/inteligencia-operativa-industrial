# Diccionario de Features

Feature layer para benchmarking, anomalías, diagnóstico causal, escenarios y priorización.

| Tabla | Feature | Tipo | Lógica de negocio |
|---|---|---|---|
| line_day_features | production_volume | observada | Volumen diario de producción equivalente por línea. |
| line_day_features | sec_vs_target | derivada | Brecha de intensidad energética frente a objetivo de línea. |
| line_day_features | process_deviation_density | derivada | Desvíos por tonelada para diferenciar ruido vs problema estructural. |
| line_day_features | line_criticality_weight | derivada | Peso estratégico de línea para priorización operativa. |
| equipment_day_features | energy_intensity_relative | derivada | Consumo del equipo relativo al promedio de su tipo. |
| equipment_day_features | performance_drift | derivada | Pendiente móvil de deterioro de desempeño energético. |
| equipment_day_features | energy_risk_proxy | derivada | Score compuesto de sobreconsumo, paradas, anomalías y criticidad. |
| shift_features | operator_pressure_proxy | derivada | Proxy de presión operativa combinando demanda, tensión y mix. |
| shift_features | shift_variance_score | derivada | Varianza operativa de turno para comparar estabilidad. |
| improvement_opportunity_features | annual_saving_proxy | derivada | Ahorro anual potencial combinando ahorro declarado e impacto operativo. |
| improvement_opportunity_features | implementation_complexity_score | derivada | Complejidad técnica-económica considerando dificultad, CAPEX y horizonte. |
| improvement_opportunity_features | operational_urgency_score | derivada | Urgencia de intervención según pérdida y criticidad de línea. |