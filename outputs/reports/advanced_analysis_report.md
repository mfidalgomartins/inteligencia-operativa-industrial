# Informe Analítico Avanzado

## 1. Salud global de la operación
- **Insight principal**: la operación presenta brecha media SEC de -69.03 kWh/t y OEE sintético medio 0.936.
- **Evidencia cuantitativa**: pérdida económica proxy agregada 162,776 EUR.
- **Lectura operativa**: predominan pérdidas por ineficiencia en líneas críticas y variabilidad de turno.
- **Lectura económica**: el coste energético y paradas explican la mayor porción del valor perdido.
- **Lectura estratégica**: hay margen para capturar ahorro rápido sin esperar CAPEX largo.
- **Caveats**: modelo basado en proxies sintéticos; requiere calibración con históricos reales.
- **Recomendación asociada**: activar frente combinado de quick wins + mantenimiento focalizado.

## 2. Líneas y equipos críticos
- **Insight principal**: línea crítica prioritaria PLT_BIZ_L04 y equipo crítico PLT_BIZ_L02_EQ06.
- **Evidencia cuantitativa**: score línea 27.3; score equipo 61.6.
- **Lectura operativa**: hay patrón de deterioro persistente, no solo eventos aislados.
- **Lectura económica**: concentración de impacto facilita priorización por valor.
- **Lectura estratégica**: intervenir nodos críticos mejora el perfil de riesgo global.
- **Caveats**: score con escala anclada (v1), comparable en contexto operativo similar; evitar lectura cross-contexto sin calibración.
- **Recomendación asociada**: intervención secuencial por línea-equipo con verificación semanal.

## 3. Desvíos y causas raíz
- **Insight principal**: la causa raíz dominante es InestabilidadTorque.
- **Evidencia cuantitativa**: score causal agregado 7144.2.
- **Lectura operativa**: desvíos de proceso y paradas no planificadas convergen sobre pocas causas.
- **Lectura económica**: la recurrencia causal amplifica pérdida por indisponibilidad.
- **Lectura estratégica**: cerrar bucle causa-acción tiene mejor retorno que medidas dispersas.
- **Caveats**: no se modela causalidad contrafactual.
- **Recomendación asociada**: plan de mitigación por causa top-5 con owner operativo.

## 4. Coste industrial y emisiones
- **Insight principal**: energía y disponibilidad explican la mayor parte de pérdidas monetizadas proxy.
- **Evidencia cuantitativa**: ranking de escenarios muestra ahorro anual relevante con reducción de SEC y paradas.
- **Lectura operativa**: mejorar SEC reduce simultáneamente coste y presión térmica de proceso.
- **Lectura económica**: mejoras de payback corto financian parte de transformaciones CAPEX.
- **Lectura estratégica**: priorizar líneas de mayor intensidad de emisiones evita coste futuro regulatorio.
- **Caveats**: factor de emisiones y precio energético son proxies sintéticos.
- **Recomendación asociada**: roadmap dual de eficiencia energética + confiabilidad.

## 5. Oportunidades de mejora
- **Insight principal**: quick wins dominan el corto plazo y CAPEX domina impacto estructural.
- **Evidencia cuantitativa**: quick wins top score medio 60.7; CAPEX top score medio 47.1.
- **Lectura operativa**: quick wins estabilizan; CAPEX cambia frontera de desempeño.
- **Lectura económica**: existe trade-off retorno rápido vs valor acumulado a largo plazo.
- **Lectura estratégica**: conviene cartera balanceada por olas de ejecución.
- **Caveats**: ejecución real puede alterar plazos y retorno.
- **Recomendación asociada**: ejecutar ola 1 con quick wins + mantenimiento, preparar CAPEX con gating técnico.

## 6. Implicaciones estratégicas
- **Insight principal**: actuar primero sobre PLT_BIZ_L04 reduce riesgo operativo y pérdida económica más rápidamente.
- **Evidencia cuantitativa**: primera iniciativa de ranking INI_0000013 con score 66.0.
- **Lectura operativa**: diferir intervención en activos críticos eleva riesgo de eventos no planificados.
- **Lectura económica**: la secuencia recomendada maximiza ahorro acumulado ajustado por factibilidad.
- **Lectura estratégica**: mantener pipeline vivo para no perder capacidad de transformación.
- **Caveats**: sensibilidad de ranking a cambios de pesos y shocks energéticos.
- **Recomendación asociada**: gobernanza mensual de portafolio con revisión de sensibilidad.