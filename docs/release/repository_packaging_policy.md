# Repository Packaging Policy

## Objetivo
Reducir fricción de revisión y evitar repositorios inflados sin perder auditabilidad de decisión.

## Clasificación de artefactos
- **Versionables (ligeros, revisables)**:
  - código (`src/`, `sql/`, `tests/`);
  - contratos y configs (`contracts/`, `configs/`);
  - docs (`docs/`);
  - muestras (`data/samples/`);
  - manifests clave y fingerprints.
- **Regenerables locales (no obligatorios en repo público)**:
  - `data/raw/*.csv` completos;
  - outputs analíticos masivos en `data/processed/*.csv`;
  - charts masivos y packs temporales.

## Estrategia de sample data
- Se generan muestras deterministas desde tablas raw/processed clave.
- Manifest asociado: `data/processed/sample_data_manifest.csv`.
- Finalidad: reproducibilidad para reviewer sin payload de cientos de MB.

## SQL cleanup policy
- SQL operativo oficial: `sql/01...11`.
- SQL histórico/legacy: `sql/legacy/`.
- Regla: ningún runner productivo debe leer `sql/legacy/`.

## Output retention policy
- Policy materializada en `data/processed/output_retention_audit.csv`.
- Campos: `artifact`, `size_bytes`, `retention_policy`.
- Valores actuales:
  - `publishable_small`,
  - `local_only_regenerable`.

## Repository modes
1. **Minimal public mode**
   - foco: claridad + reproducibilidad mínima + narrativa sólida;
   - dataset completo excluido, solo samples.
2. **Full local execution mode**
   - foco: regeneración total end-to-end en entorno local.
3. **Internal/extended mode**
   - foco: payload completo, packs de comité y trazabilidad extendida.

## Packaging governance (anti-overclaim)
- Auditoría obligatoria: `data/processed/release_pack_governance_audit.csv`.
- Regla de control:
  - `committee_pack` solo se permite cuando readiness y publish decision cumplen policy.
  - Si no cumple, se genera `README_BLOCKED.md` en el profile para prevenir uso indebido.
- Está prohibido presentar un profile `BLOCKED` como evidencia de comité.

## Reviewer friction points mitigados
- Falta de lockfile.
- Falta de hash/fingerprint de artefactos críticos.
- Mezcla entre outputs operativos y outputs legacy.
- Dificultad de navegación por volumen de datos no necesarios.
