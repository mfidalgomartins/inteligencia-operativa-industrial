# Definiciones de Métricas SQL

## Métricas de eficiencia y energía
- `sec_kwh_ton`: `consumo_kwh / toneladas_equivalentes_hora`.
- `sec_gap`: `sec_kwh_ton - sec_target_kwh_ton`.
- `cost_energy_per_unit`: `coste_energia_eur / unidades_equivalentes_hora`.
- `loss_value_per_ton`: `loss_value_proxy_eur / production_volume_ton`.

## Métricas de desempeño operativo
- `availability_rate`: `horas_operativas / horas_planificadas` (agregado).
- `quality_rate_proxy`: `1 - scrap_pct`.
- `oee_synthetic`: `availability_rate * utilization_rate * quality_rate_proxy`.
- `shift_variance_proxy`: combinación ponderada de SEC, scrap, horas parada y desvíos.

## Métricas de riesgo y confiabilidad
- `consumo_relativo_tipo`: consumo equipo / promedio del tipo de equipo.
- `riesgo_equipo_proxy`: score compuesto de paradas, desvíos, eficiencia nominal y criticidad.
- `equipment_efficiency_risk_index`: score adicional de sobreconsumo + indisponibilidad + criticidad.
- `trend_deterioro_7d`: media móvil 7 días del índice de riesgo equipo.

## Métricas económicas y ambientales
- `loss_root_cause_score`: 50% impacto producción + 30% impacto energía + 20% horas parada ponderadas.
- `prioridad_compuesta` (value pool): impacto energía, OEE, emisiones, payback inverso y quick-win.
- `emisiones_evitables_proxy_kg`: fracción proxy de emisiones reducibles.
- `criticidad_global_score`: combinación de pérdida económica, SEC y penalización por bajo OEE.

## Métricas de priorización
- `payback_estimado_meses`: `capex_estimado / (ahorro_anual_estimado / 12)`.
- `kpi_iniciativas_mayor_valor`: ranking por `prioridad_compuesta`.
- `kpi_capex_mejor_payback`: ranking CAPEX por menor payback.

## Notas de uso
- Las métricas son proxies sintéticos para evaluación comparativa y priorización.
- En despliegue real, validar fórmulas con finanzas industriales y responsables de energía.
