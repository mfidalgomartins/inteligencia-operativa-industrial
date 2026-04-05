from __future__ import annotations

import pandas as pd
import pytest

from tests._helpers import DATA_RAW, read_csv


@pytest.mark.blocker
@pytest.mark.quick
@pytest.mark.full
def test_energy_raw_grain_uniqueness() -> None:
    energy = pd.read_csv(DATA_RAW / "consumos_energia_horarios.csv")
    dup = int(energy.duplicated(subset=["timestamp", "planta_id", "linea_id", "equipo_id", "fuente_energia"]).sum())
    assert dup == 0


@pytest.mark.blocker
@pytest.mark.quick
@pytest.mark.full
def test_line_day_scored_grain_uniqueness() -> None:
    line = read_csv("line_day_features_scored.csv")
    dup = int(line.duplicated(subset=["fecha", "planta_id", "linea_id"]).sum())
    assert dup == 0


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_critical_null_rates_under_threshold() -> None:
    line = read_csv("line_day_features_scored.csv")
    critical_cols = ["fecha", "planta_id", "linea_id", "production_volume", "sec", "oee_synthetic", "loss_value_proxy"]
    null_rate = line[critical_cols].isna().mean()
    assert (null_rate <= 0.001).all(), f"null rates above threshold: {null_rate[null_rate > 0.001].to_dict()}"


@pytest.mark.blocker
@pytest.mark.quick
@pytest.mark.full
def test_no_impossible_values_in_core_ranges() -> None:
    line = read_csv("line_day_features_scored.csv")
    assert line["production_volume"].gt(0).all()
    assert line["sec"].between(50, 3000).all()
    assert line["oee_synthetic"].between(0, 1.2).all()


@pytest.mark.high
@pytest.mark.full
def test_emissions_energy_consistency_proxy() -> None:
    emissions = pd.read_csv(DATA_RAW / "emisiones_estimadas.csv")
    ratio = emissions["emisiones_co2e_kg"] / emissions["consumo_kwh"].replace(0, pd.NA)
    delta = (ratio - emissions["factor_emision_proxy"]).abs()
    assert float(delta.quantile(0.99)) <= 0.02
