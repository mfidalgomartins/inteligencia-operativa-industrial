# Framework de Parametrización Sectorial (Tubacex ↔ Petronor)

## Objetivo
Convertir la adaptabilidad sectorial desde narrativa a capacidad técnica versionable y auditable, manteniendo un núcleo analítico común.

## Núcleo común vs parametrización
- Núcleo común (estable): modelo raw, SQL marts, features base, scorecards, scenario engine, priorización, dashboard serving.
- Parametrización sectorial (configurable): taxonomía de procesos/activos, benchmark por familia, factores de emisiones, lógica de criticidad, librería de iniciativas, constraints sectoriales, caveats y riesgos de mal uso.

## Diseño técnico implementado
- Configuración común: `configs/sectors/common_core.json`.
- Perfiles sectoriales: `configs/sectors/tubacex_profile.json`, `configs/sectors/petronor_profile.json`.
- Módulo ejecutable: `src/sector_parameterization.py`.
- Paso del pipeline: `run_sector_parameterization` integrado en `src/run_pipeline.py`.

## Artefactos generados por perfil
Ruta base: `data/processed/sector/<profile_id>/`

Por cada perfil se generan:
- `taxonomy_mapping.csv`
- `asset_hierarchy.csv`
- `process_family_definitions.csv`
- `benchmark_set.csv`
- `emission_factors.csv`
- `initiative_library.csv`
- `criticality_logic.csv`
- `constraints_by_sector.csv`
- `scenario_sensitivities.csv`
- `metric_caveats.csv`
- `misuse_risks.csv`
- `kpi_priority.csv`
- `line_day_view.csv`
- `investment_view.csv`
- `profile_metadata.csv`

Artefactos globales:
- `data/processed/sector_profile_registry.csv`
- `data/processed/sector_adaptation_evidence.csv`
- `data/processed/sector_mapping_coverage.csv`

## Contratos de parametrización
Checks implementados:
- compatibilidad de versión con `common_core_version`;
- presencia de claves obligatorias de perfil;
- cobertura total de mappings para dominios observados (`familia_proceso`, `tipo_equipo`, `categoria_iniciativa`, `tipo_iniciativa`, `fuente_energia`);
- coherencia de cardinalidad entre vistas sectoriales y salidas core.

## Qué se parametriza realmente
- Traducción técnica de taxonomías (no solo labels UI).
- Ajuste de benchmarks por familia de proceso.
- Ajuste de factores de emisiones por fuente energética.
- Reglas sectoriales de criticidad y pesos KPI.
- Restricciones sectoriales CAPEX/OPEX y capacidad de ejecución.
- Sensibilidades de escenario y caveats de interpretación.

## Qué permanece como núcleo transversal
- Definición formal de KPIs certificados (metric governance).
- Marts semánticos base.
- Estructura de scoring y decisión.
- Framework de validación, traceability y release.

## Pruebas de consistencia sectorial
Suite: `tests/sector/test_sector_parameterization_consistency.py`
- registro y versiones de perfiles;
- cobertura de mappings al 100%;
- existencia/no-vacío de artefactos sectoriales;
- coherencia de filas respecto a outputs core;
- evidencia de divergencia real entre perfiles (constraints y factores de emisión).

## Claim defendible después de esta implementación
El proyecto ya soporta adaptación sectorial técnica gobernada por configuración versionada, con outputs diferenciados por perfil y tests de consistencia; la adaptación deja de depender únicamente de narrativa.
