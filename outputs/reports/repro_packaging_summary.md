# Reproducibility & Packaging Summary

## Environment Fingerprint
- Python: `3.12.13`
- Lockfile present: `True`
- Lock sync with runtime: `True`
- Pip freeze packages: `20`

## Packaging Audit
| mode            |   repo_total_size_bytes |   pack_size_bytes |   pack_size_mb |   reduction_vs_full_repo_pct |   files_in_pack |   missing_files |
|:----------------|------------------------:|------------------:|---------------:|-----------------------------:|----------------:|----------------:|
| committee_pack  |              1004568354 |               571 |          0.001 |                      100     |               1 |               0 |
| github_reviewer |              1004568354 |            390707 |          0.373 |                       99.961 |              21 |               0 |
| minimal_public  |              1004568354 |           2042701 |          1.948 |                       99.797 |              17 |               0 |

## Release Governance Audit
| profile         | governance_status   | governance_reason                                                                                                                                                                   | overall_readiness_level   | publish_decision    | committee_readiness_state   |
|:----------------|:--------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:--------------------------|:--------------------|:----------------------------|
| committee_pack  | BLOCKED             | blocked_by_governance: readiness=screening-grade-only; publish_decision=publish with caveat; allowed_states=['committee-candidate', 'committee-grade']; allowed_publish=['publish'] | screening-grade-only      | publish with caveat | not-committee-grade         |
| github_reviewer | ALLOWED             | allowed                                                                                                                                                                             | screening-grade-only      | publish with caveat | not-committee-grade         |
| minimal_public  | ALLOWED             | allowed                                                                                                                                                                             | screening-grade-only      | publish with caveat | not-committee-grade         |

## Top Heavy Artifacts (Retention Audit)
| artifact                                                            |   size_bytes | retention_policy       |
|:--------------------------------------------------------------------|-------------:|:-----------------------|
| data/raw/consumos_energia_horarios.csv                              |    152167503 | local_only_regenerable |
| data/processed/int_line_hour.csv                                    |     56242743 | local_only_regenerable |
| data/raw/emisiones_estimadas.csv                                    |     37563417 | local_only_regenerable |
| data/processed/equipment_energy_anomaly_scores.csv                  |     18302197 | local_only_regenerable |
| data/processed/analysis_support_equipment_energy_anomaly_scores.csv |     18299185 | local_only_regenerable |
| data/processed/vw_equipment_efficiency_risk.csv                     |     13530438 | local_only_regenerable |
| data/processed/equipment_day_features.csv                           |     13276986 | local_only_regenerable |
| data/processed/mart_equipment_day.csv                               |     12275109 | local_only_regenerable |

## Sample Data Manifest
| sample_file                                    | source_file                                        | status   |   rows_written |
|:-----------------------------------------------|:---------------------------------------------------|:---------|---------------:|
| raw_consumos_energia_sample.csv                | data/raw/consumos_energia_horarios.csv             | OK       |           4000 |
| raw_produccion_turno_sample.csv                | data/raw/produccion_turno.csv                      | OK       |           2000 |
| processed_line_day_features_sample.csv         | data/processed/line_day_features_scored.csv        | OK       |           2000 |
| processed_investment_prioritization_sample.csv | data/processed/investment_prioritization_final.csv | OK       |             48 |

## Cleanup Applied
- Lockfile pinneado (`requirements.lock.txt`) y fingerprint de entorno habilitado.
- Packs de publicación (`minimal_public`, `github_reviewer`, `committee_pack`) generados.