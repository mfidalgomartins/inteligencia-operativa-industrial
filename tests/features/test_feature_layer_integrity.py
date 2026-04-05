from __future__ import annotations

import numpy as np
import pytest

from tests._helpers import read_csv


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_line_features_required_columns_and_finite_values() -> None:
    line = read_csv("line_day_features_scored.csv")
    required = [
        "production_volume",
        "sec",
        "oee_synthetic",
        "loss_value_proxy",
        "trend_sec_7d",
        "trend_oee_7d",
        "process_deviation_risk_score",
    ]
    assert set(required).issubset(set(line.columns))
    numeric = line[required].astype(float)
    assert np.isfinite(numeric.to_numpy()).all()


@pytest.mark.high
@pytest.mark.full
def test_line_features_business_invariants() -> None:
    line = read_csv("line_day_features_scored.csv")
    assert line["planned_vs_actual_utilization"].between(0, 2.5).all()
    assert line["availability_rate"].between(0, 1.2).all()
    assert line["process_deviation_density"].ge(0).all()
    assert line["stop_density"].ge(0).all()


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_equipment_features_anomaly_consistency() -> None:
    eq = read_csv("equipment_energy_anomaly_scores.csv")
    assert eq["equipment_energy_anomaly_score"].between(0, 100).all()
    assert eq["overload_events"].isin([0, 1]).all()
    assert eq["repeated_stop_flag"].isin([0, 1]).all()
    # Si no hay anomalías ni sobrecarga, el score no debería ser extremo.
    safe = eq[(eq["anomaly_count"] == 0) & (eq["overload_events"] == 0)]
    if not safe.empty:
        assert float(safe["equipment_energy_anomaly_score"].quantile(0.95)) < 80


@pytest.mark.high
@pytest.mark.full
def test_shift_variance_and_pressure_ranges() -> None:
    shift = read_csv("shift_comparison_scores.csv")
    assert shift["shift_variance_score"].between(0, 100).all()
    assert shift["stop_minutes"].ge(0).all()
    assert shift["scrap_pct"].between(0, 100).all()
