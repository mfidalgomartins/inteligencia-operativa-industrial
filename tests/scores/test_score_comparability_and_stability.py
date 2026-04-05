from __future__ import annotations

import pytest

from src.score_stability import ALLOWED_COMPARABILITY_TAGS
from tests._helpers import read_csv


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_score_comparability_registry_uses_allowed_tags() -> None:
    registry = read_csv("score_comparability_registry.csv")
    assert not registry.empty
    assert set(registry["comparability_tag"].astype(str)).issubset(ALLOWED_COMPARABILITY_TAGS)
    assert registry["scale_version"].astype(str).str.len().gt(0).all()


@pytest.mark.high
@pytest.mark.quick
@pytest.mark.full
def test_anchored_outlier_shift_is_not_worse_than_legacy_for_most_scores() -> None:
    stability = read_csv("score_stability_before_after.csv")
    assert not stability.empty
    wins = (
        stability["anchored_outlier_median_abs_shift"]
        <= stability["legacy_outlier_median_abs_shift"] + 1e-9
    ).mean()
    assert float(wins) >= 0.75


@pytest.mark.critical
@pytest.mark.full
def test_priority_ranking_outlier_robustness_improves_or_matches_legacy() -> None:
    ranking = read_csv("score_ranking_robustness_before_after.csv")
    assert not ranking.empty
    row = ranking.iloc[0]
    assert float(row["anchored_top10_overlap_after_outlier"]) >= float(row["legacy_top10_overlap_after_outlier"])


@pytest.mark.high
@pytest.mark.full
def test_investment_scores_have_scaling_metadata() -> None:
    invest = read_csv("investment_prioritization_final.csv")
    assert "score_scaling_method" in invest.columns
    assert "score_scale_version" in invest.columns
    assert "improvement_priority_comparability_tag" in invest.columns
    assert invest["score_scaling_method"].astype(str).eq("anchored_reference_linear").all()
    assert invest["score_scale_version"].astype(str).eq("anchored_reference_v1").all()
