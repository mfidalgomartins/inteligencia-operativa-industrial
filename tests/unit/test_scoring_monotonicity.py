from __future__ import annotations

import numpy as np
import pytest


def _priority_index(
    energy_saving: float,
    operational_impact: float,
    emissions_reduction: float,
    implementation_feasibility: float,
    payback_score: float,
    strategic_priority: float,
    uncertainty_index: float,
) -> float:
    return (
        0.24 * energy_saving
        + 0.20 * operational_impact
        + 0.12 * emissions_reduction
        + 0.16 * implementation_feasibility
        + 0.14 * payback_score
        + 0.10 * strategic_priority
        + 0.04 * uncertainty_index
    )


@pytest.mark.blocker
@pytest.mark.quick
@pytest.mark.full
def test_priority_formula_weights_sum_to_one() -> None:
    weights = np.array([0.24, 0.20, 0.12, 0.16, 0.14, 0.10, 0.04], dtype=float)
    assert np.isclose(weights.sum(), 1.0)


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_priority_index_monotonicity_property() -> None:
    rng = np.random.default_rng(20260331)
    base = rng.uniform(0, 100, size=(500, 7))
    deltas = rng.uniform(0.01, 10.0, size=500)

    # Si solo sube una componente positiva, el índice no puede bajar.
    for col in range(7):
        before = np.array([_priority_index(*row) for row in base])
        after_rows = base.copy()
        after_rows[:, col] = np.clip(after_rows[:, col] + deltas, 0, 100)
        after = np.array([_priority_index(*row) for row in after_rows])
        assert np.all(after >= before - 1e-9), f"monotonicity broke at component index {col}"


@pytest.mark.high
@pytest.mark.full
def test_priority_index_bounded_when_components_bounded() -> None:
    rng = np.random.default_rng(20260401)
    data = rng.uniform(0, 100, size=(1000, 7))
    scores = np.array([_priority_index(*row) for row in data])
    assert np.nanmin(scores) >= 0.0
    assert np.nanmax(scores) <= 100.0
