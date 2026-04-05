from __future__ import annotations

import pandas as pd
import pytest

from tests._helpers import DATA_RAW, read_csv


EXPECTED_RAW_COUNTS = {
    "consumos_energia_horarios.csv": 1263168,
    "produccion_turno.csv": 26316,
    "desvios_proceso.csv": 5939,
    "paradas.csv": 1266,
    "mantenimientos.csv": 1383,
    "emisiones_estimadas.csv": 421056,
}

EXPECTED_PROCESSED_COUNTS = {
    "line_day_features_scored.csv": 8772,
    "equipment_energy_anomaly_scores.csv": 52632,
    "scenario_table.csv": 192,
    "investment_prioritization_final.csv": 48,
    "shift_comparison_scores.csv": 36,
    "loss_root_cause_score.csv": 96,
}


@pytest.mark.critical
@pytest.mark.full
def test_reproducible_rowcount_signatures_raw() -> None:
    for file_name, expected_count in EXPECTED_RAW_COUNTS.items():
        observed = len(pd.read_csv(DATA_RAW / file_name))
        assert observed == expected_count, f"rowcount drift in {file_name}: {observed} != {expected_count}"


@pytest.mark.critical
@pytest.mark.full
def test_reproducible_rowcount_signatures_processed() -> None:
    for file_name, expected_count in EXPECTED_PROCESSED_COUNTS.items():
        observed = len(read_csv(file_name))
        assert observed == expected_count, f"rowcount drift in {file_name}: {observed} != {expected_count}"


@pytest.mark.high
@pytest.mark.quick
@pytest.mark.full
def test_reproducible_temporal_coverage_core_series() -> None:
    line = read_csv("line_day_features_scored.csv")
    energy = pd.read_csv(DATA_RAW / "consumos_energia_horarios.csv")

    line_dates = pd.to_datetime(line["fecha"])
    energy_ts = pd.to_datetime(energy["timestamp"])

    assert line_dates.min().strftime("%Y-%m-%d") == "2024-01-01"
    assert line_dates.max().strftime("%Y-%m-%d") == "2025-12-31"
    assert energy_ts.min().strftime("%Y-%m-%d %H:%M:%S") == "2024-01-01 00:00:00"
    assert energy_ts.max().strftime("%Y-%m-%d %H:%M:%S") == "2025-12-31 23:00:00"
