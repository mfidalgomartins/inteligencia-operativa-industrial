# Release Operating Model

## Objetivo
Definir una disciplina de release consistente, auditable y orientada a revisión externa (GitHub + comité industrial).

## Flujo de release implementado
1. Ejecutar pipeline completo: `python -m src`.
2. Ejecutar suite completa: `pytest -q`.
3. Validar readiness:
   - `data/processed/validation_readiness_summary.csv`
   - `outputs/reports/validation_report.md`
4. Generar packs y huellas:
   - `outputs/release_packs/*`
   - `manifests/environment_fingerprint.json`
   - `manifests/artifact_fingerprints_latest.csv`
5. Publicar con nivel de claim acorde al readiness actual.

## Release checklist (obligatorio)
- [ ] `overall_status=SUCCESS` en `manifests/build_manifest_latest.json`.
- [ ] `pytest -q` sin fallos.
- [ ] `publish_decision` explícito (`publish`, `publish with caveat`, `no publish`).
- [ ] fingerprint de entorno generado y lockfile presente.
- [ ] fingerprint de artefactos críticos sin `exists_flag=0`.
- [ ] `minimal_public` y `github_reviewer` generados.
- [ ] `committee_pack` **ALLOWED** solo si readiness/publish decision lo habilitan; en caso contrario debe quedar **BLOCKED** con `README_BLOCKED.md`.
- [ ] caveats de madurez financiera y metodológica actualizados.

## Publish checklist (GitHub)
- [ ] README actualizado con alcance real (sin overclaim).
- [ ] links a dashboard y reportes ejecutivos válidos.
- [ ] evidencia de validación y trazabilidad incluida.
- [ ] datos pesados excluidos; samples incluidos.
- [ ] versiones/fechas de release visibles en manifest.

## Modos de distribución
- `minimal_public`: muestra técnica limpia para portfolio público.
- `github_reviewer`: pack orientado a revisión técnica de consistencia.
- `committee_pack`: pack orientado a comité (decisión, riesgos, sensibilidad, caveats), sujeto a governance gate por readiness.

## Governance de packs
- Audit trail oficial: `data/processed/release_pack_governance_audit.csv`.
- Política:
  - `publish_decision != publish` -> `committee_pack` debe quedar `BLOCKED`.
  - `publish_decision = publish` + readiness de comité suficiente -> `committee_pack` `ALLOWED`.
- Está prohibido presentar un pack bloqueado como material de comité.

## Política de claims por release
- `screening-grade-only`: prohibido claim de “business case final”.
- `decision-support-only`: válido para priorización operativa.
- `committee-candidate` / `committee-grade`: permitido claim de material de comité con condiciones del framework.
