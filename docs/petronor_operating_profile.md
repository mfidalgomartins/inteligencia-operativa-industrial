# Petronor Operating Profile

## Entidades clave
- Site, unidad de proceso y equipo crítico como jerarquía operativa.
- Familias reinterpretadas a refino: conversión, hornos de proceso, blending, expedición/calidad.
- Activos críticos orientados a continuidad de proceso continuo.

## KPIs críticos (peso de prioridad)
- `sec`: 0.24
- `oee_synthetic`: 0.16
- `availability_rate`: 0.18
- `loss_value_proxy`: 0.16
- `emissions_per_unit`: 0.26

## Lógica de benchmark
- Benchmark sectorial más exigente en energía y emisiones para unidades térmicas.
- Multiplicadores de emisiones por familia por encima del perfil metalúrgico.
- Foco principal: coste energético, carbono y disponibilidad de unidades críticas.

## Tipos de iniciativa y secuencia preferida
1. `MantenimientoPredictivo`
2. `ControlAvanzado`
3. `QuickWinOperativo`
4. `CAPEXTransformacional`

Categorías mandatory por perfil: `Confiabilidad`, `TransformacionIndustrial`.

## Sensibilidades de escenario
- `energy_price_beta`: 1.18
- `carbon_cost_beta`: 1.20
- `demand_pressure_beta`: 0.98
- `staff_availability_beta`: 0.90

## Caveats métricos
- `emissions_per_unit`: depende de mix combustible + severidad operativa.
- `loss_value_proxy`: señal de screening; no representa P&L auditado por sí sola.

## Riesgos de mal uso
- Tratar ahorro energético como margen neto sin reconciliación de mercado.
- Ignorar riesgo de ventanas de parada en proceso continuo.
- Comparar unidades con severidad/objetivo de producto distintos sin segmentación.

## Artefactos vinculados
- Config: `configs/sectors/petronor_profile.json`.
- Outputs: `data/processed/sector/petronor/`.
