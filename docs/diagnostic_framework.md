# Diagnostic Framework

## 1) Energy intensity benchmarking
- **Inputs**: `line_day_features.sec`, `line_day_features.sec_vs_target`, `line_day_features.sec_vs_benchmark`, benchmark externo por familia.
- **Lógica**: se estima brecha diaria SEC y persistencia a 7 días para separar desvío puntual vs estructural.
- **Supuestos**: benchmark externo proxy representa un nivel técnicamente alcanzable.
- **Limitaciones**: no se modela curva completa de carga parcial por producto.
- **Valor para operación**: prioriza líneas con gap persistente, no solo picos aislados.

## 2) Anomaly detection interpretable
- **Inputs**: `equipment_day_features.energy_intensity_relative`, `overload_events`, `anomaly_count`, `performance_drift`.
- **Lógica**: score aditivo y transparente con umbrales físicos (sobrecarga, drift, paradas repetidas).
- **Supuestos**: sobreconsumo relativo por tipo de equipo es señal de degradación.
- **Limitaciones**: no hay datos de sensor en alta frecuencia sub-horaria.
- **Valor para operación**: lista de equipos a inspeccionar en mantenimiento/operación.

## 3) Root-cause ranking
- **Inputs**: `vw_loss_root_cause_summary`, impacto producción, energía, horas de parada.
- **Lógica**: ranking por peso económico-operativo y recurrencia mensual.
- **Supuestos**: impacto proxy de parada representa pérdida de contribución marginal.
- **Limitaciones**: causalidad aproximada, no inferencia causal formal.
- **Valor para operación**: orden de ataque a causas de mayor valor.

## 4) Line/equipment risk scoring
- **Inputs**: brecha SEC, OEE, pérdida económica, desvíos, paradas, criticidad.
- **Lógica**: score compuesto normalizado para `line_criticality_score` y riesgo equipo.
- **Supuestos**: pesos reflejan prioridad industrial (coste + continuidad operativa).
- **Limitaciones**: pesos fijos, no calibrados con comité técnico.
- **Valor para operación**: semáforo de riesgo por línea y por activo.

## 5) Improvement priority logic
- **Inputs**: ahorro potencial, payback, complejidad, urgencia operativa.
- **Lógica**: `opportunity_priority_score` balancea valor económico con factibilidad.
- **Supuestos**: payback y ahorro proxy son consistentes por iniciativa.
- **Limitaciones**: no incorpora restricciones de caja reales ni ventanas de parada mayor.
- **Valor para operación**: transforma diagnóstico técnico en secuencia de ejecución.
