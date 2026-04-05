# Resumen de Lógica Sintética

## Principios de generación
- Horizonte temporal de 24 meses con granularidad horaria y por turno.
- Diferencias estructurales entre plantas, líneas y equipos.
- Comportamiento no estacionario: estacionalidad, shocks de energía y variación de mix.
- Relación causal proxy entre desvíos de proceso, scrap, consumo específico y paradas.
- Coexistencia de iniciativas quick win y CAPEX transformacional con perfiles de retorno distintos.

## Mecánicas simuladas
1. Contexto operativo: demanda, complejidad de mix, temperatura y precio de energía por planta/turno.
2. Paradas: planificadas y no planificadas con severidad distinta por familia de proceso.
3. Desvíos: probabilidad dependiente de tensión operativa y turno, con severidad y duración.
4. Producción por turno: afectada por downtime, desvíos, demanda y complejidad.
5. Consumo energético horario: sensible a carga, degradación de activo, pérdidas térmicas y sobreconsumo.
6. Emisiones estimadas: derivadas del consumo y del factor de emisión por fuente energética.
7. Iniciativas y CAPEX: impacto esperado heterogéneo, dificultad, riesgo y payback.

## Reproducibilidad
- Generación determinista bajo semilla fija.
- Salida estandarizada a CSV en `data/raw/`.
- Incluye reportes de plausibilidad y cardinalidades para auditoría.
