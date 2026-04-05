# Supuestos Metodológicos

## Horizonte y granularidad
- Histórico sintético: 24 meses (`2024-01-01` a `2025-12-31`).
- Granos principales: equipo-hora, línea-turno-día, equipo-día, línea-día.

## Supuestos de operación
- Tres turnos (`A`, `B`, `C`) con comportamiento diferencial.
- Bajo carga parcial, aumenta SEC y empeora estabilidad de proceso.
- Desvíos de proceso incrementan scrap/reproceso y consumo específico.
- Paradas no planificadas impactan OEE, coste y riesgo.

## Supuestos energéticos y ambientales
- Precio energético variable por contexto y fuente.
- Emisiones proxy calculadas con factor de emisión por fuente energética.
- Brecha SEC vs benchmark representa potencial técnico de mejora.

## Supuestos económicos
- Monetización proxy de pérdidas por energía, paradas, calidad y emisiones.
- CAPEX, ahorro anual y payback derivados para priorización comparativa.
- Los valores son de referencia de portfolio, no valoración financiera oficial.

## Supuestos analíticos
- Preferencia por métodos interpretables (scores ponderados y reglas explícitas).
- Separación ruido vs estructural por persistencia temporal de señales.
- Ranking de iniciativas bajo enfoque multicriterio con sensibilidad de pesos.

## Limitaciones
- No sustituye datos reales de historian/MES/ERP.
- No modela todas las interacciones físico-químicas de ingeniería de proceso.
- Requiere calibración con expertos de operación, energía y mantenimiento.
