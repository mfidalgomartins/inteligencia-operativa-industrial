# Reproducibility Forensics Framework

## Objetivo
Elevar la reproducibilidad de “pipeline funcional” a “reproducibilidad forense”: mismo código, mismo entorno y mismos artefactos críticos con huella verificable.

## Artefactos forenses implementados
- `requirements.lock.txt`: snapshot pinneado del entorno Python usado en ejecución.
- `manifests/environment_fingerprint.json`: huella de runtime (Python, plataforma, hashes de requirements/lock, hash de `pip freeze`).
- `manifests/artifact_fingerprints_latest.csv`: hashes y tamaños de artefactos ejecutivos críticos.
- `data/processed/sample_data_manifest.csv`: manifiesto de datasets de muestra.
- `data/processed/packaging_audit_before_after.csv`: comparativa de tamaño repo vs packs de distribución.
- `data/processed/output_retention_audit.csv`: clasificación de retención por artefacto.

## Cadena de reproducibilidad
1. Instalar dependencias con `requirements.lock.txt`.
2. Ejecutar pipeline completo (`python -m src`).
3. Verificar que:
   - `manifests/environment_fingerprint.json` existe,
   - `lockfile_sync_with_runtime` es `true`,
   - fingerprints de artefactos están completos,
   - tests críticos pasan.

## Definición operativa de “reproducible”
Se considera reproducible cuando:
- rowcounts y cobertura temporal pasan tests de reproducibilidad;
- métricas ejecutivas y checks cross-output pasan;
- fingerprint de entorno y fingerprint de artefactos se generan sin faltantes.

## Estrategia de snapshots
- Snapshot ligero y versionable:
  - `data/samples/*.csv` (no datasets completos),
  - snapshots ejecutivos (KPI/validación/portfolio),
  - reports de decisión.
- Snapshot pesado:
  - `data/raw/*.csv` y gran parte de `data/processed/*.csv` quedan como regenerables locales.

## Riesgos mitigados
- Drift silencioso de paquetes Python.
- Inconsistencia entre runtime real y lock declarado.
- Review sin evidencia de qué artefactos se usaron realmente.
- Repetición manual no trazable entre releases.
