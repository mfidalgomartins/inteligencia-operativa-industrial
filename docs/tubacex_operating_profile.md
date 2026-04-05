# Tubacex Operating Profile

## Entidades clave
- Planta, línea, equipo como jerarquía principal.
- Familias de proceso: Laminación, Tratamiento térmico, Acabado, Inspección final.
- Tipos de equipo críticos: horno, laminador, compresor, bomba, sistema de control.

## KPIs críticos (peso de prioridad)
- `sec`: 0.30
- `oee_synthetic`: 0.25
- `availability_rate`: 0.20
- `loss_value_proxy`: 0.15
- `emissions_per_unit`: 0.10

## Lógica de benchmark
- Referencia sectorial por familia con multiplicadores `sec` y `emisiones`.
- Ajuste de referencia `oee` y `disponibilidad` por familia.
- Foco principal: eficiencia energética térmico-mecánica + estabilidad operativa de línea.

## Tipos de iniciativa y secuencia preferida
1. `QuickWinOperativo`
2. `MantenimientoPredictivo`
3. `ControlAvanzado`
4. `CAPEXTransformacional`

Categoría marcada como mandatory por perfil: `Confiabilidad`.

## Sensibilidades de escenario
- `energy_price_beta`: 1.00
- `carbon_cost_beta`: 1.00
- `demand_pressure_beta`: 1.05
- `staff_availability_beta`: 0.95

## Caveats métricos
- `sec`: comparar solo entre familias con mix productivo equivalente.
- `oee_synthetic`: señal de screening y priorización, no KPI contractual auditado.

## Riesgos de mal uso
- Usar benchmark de familia como benchmark corporativo formal.
- Sobreestimar valor CAPEX sin validar ventana real de parada.
- Comparar criticidad entre equipos no equivalentes por función.

## Artefactos vinculados
- Config: `configs/sectors/tubacex_profile.json`.
- Outputs: `data/processed/sector/tubacex/`.
