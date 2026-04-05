# Diccionario de Métricas

## Eficiencia y energía
- `SEC (kWh/t)`: consumo específico energético por tonelada.
- `SEC vs target`: diferencia entre SEC real y objetivo de línea.
- `SEC vs benchmark`: diferencia entre SEC real y referencia externa por familia.
- `Coste energético por unidad`: coste energético medio por unidad producida.

## Operación
- `Availability rate`: horas operativas / horas planificadas.
- `OEE sintético`: disponibilidad * utilización * calidad proxy.
- `Performance loss proxy`: pérdida asociada a sub-utilización.
- `Quality loss proxy`: pérdida asociada a scrap/reproceso.
- `Stop density`: paradas por tonelada producida.
- `Process deviation density`: desvíos por tonelada.

## Riesgo
- `equipment_energy_anomaly_score`: score interpretable de sobreconsumo/anomalía en equipo.
- `process_deviation_risk_score`: score de riesgo por desvíos y pérdida de calidad.
- `line_criticality_score`: score compuesto de pérdida, SEC, OEE, desvíos y criticidad.

## Económico-ambiental
- `loss_value_proxy`: monetización proxy de pérdidas operativas.
- `loss_root_cause_score`: peso combinado económico/energético/operativo por causa raíz.
- `emissions_per_unit`: intensidad de emisiones por unidad proxy.
- `emisiones evitables proxy`: potencial de reducción asociado a iniciativas.

## Iniciativas e inversión
- `annual_saving_proxy`: ahorro anual estimado por iniciativa.
- `payback_months`: meses para recuperar inversión.
- `improvement_priority_index`: índice final de priorización multicriterio.
- `initiative_tier`: segmentación Tier 1-4 para secuencia de ejecución.
