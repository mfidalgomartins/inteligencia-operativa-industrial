# Reproducibility & Packaging Summary

## Environment Fingerprint
- Python: `3.12.13`
- Lockfile present: `True`
- Lock sync with runtime: `True`
- Pip freeze packages: `20`

## Packaging Audit
| mode            |   repo_total_size_bytes |   pack_size_bytes |   pack_size_mb |   reduction_vs_full_repo_pct |   files_in_pack |   missing_files |
|:----------------|------------------------:|------------------:|---------------:|-----------------------------:|----------------:|----------------:|
| committee_pack  |               925343808 |               571 |          0.001 |                      100     |               1 |               0 |
| github_reviewer |               925343808 |            376745 |          0.359 |                       99.959 |              21 |               0 |
| minimal_public  |               925343808 |           2030674 |          1.937 |                       99.781 |              17 |               0 |

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
| data/processed/int_line_hour.csv                                    |     56237505 | local_only_regenerable |
| data/raw/emisiones_estimadas.csv                                    |     37563417 | local_only_regenerable |
| data/processed/equipment_energy_anomaly_scores.csv                  |     18302373 | local_only_regenerable |
| data/processed/analysis_support_equipment_energy_anomaly_scores.csv |     18299319 | local_only_regenerable |
| data/processed/vw_equipment_efficiency_risk.csv                     |     13530647 | local_only_regenerable |
| data/processed/equipment_day_features.csv                           |     13277216 | local_only_regenerable |
| data/processed/mart_equipment_day.csv                               |     12275365 | local_only_regenerable |

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