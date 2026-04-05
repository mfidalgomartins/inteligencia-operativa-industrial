from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from tests._helpers import DATA_PROCESSED, PROJECT_ROOT, read_csv


SECTOR_DIR = DATA_PROCESSED / "sector"
EXPECTED_PROFILES = {"tubacex", "petronor"}


def _profile_file(profile_id: str, filename: str) -> Path:
    return SECTOR_DIR / profile_id / filename


@pytest.mark.blocker
@pytest.mark.quick
@pytest.mark.full
def test_sector_profile_registry_contains_required_profiles() -> None:
    registry = read_csv("sector_profile_registry.csv")
    assert not registry.empty
    assert EXPECTED_PROFILES.issubset(set(registry["sector_profile_id"]))
    assert registry["profile_version"].astype(str).str.len().gt(0).all()


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_sector_mapping_coverage_is_complete_for_observed_domains() -> None:
    coverage = read_csv("sector_mapping_coverage.csv")
    assert not coverage.empty
    subset = coverage[coverage["sector_profile_id"].isin(EXPECTED_PROFILES)]
    assert not subset.empty
    assert (subset["mapping_coverage_pct"] >= 100.0 - 1e-9).all()


@pytest.mark.critical
@pytest.mark.full
def test_sector_profile_artifacts_exist_and_are_non_empty() -> None:
    expected_files = [
        "taxonomy_mapping.csv",
        "asset_hierarchy.csv",
        "process_family_definitions.csv",
        "benchmark_set.csv",
        "emission_factors.csv",
        "initiative_library.csv",
        "criticality_logic.csv",
        "constraints_by_sector.csv",
        "scenario_sensitivities.csv",
        "metric_caveats.csv",
        "kpi_priority.csv",
        "line_day_view.csv",
        "investment_view.csv",
        "profile_metadata.csv",
    ]

    for profile_id in EXPECTED_PROFILES:
        for filename in expected_files:
            path = _profile_file(profile_id, filename)
            assert path.exists(), f"Missing artifact: {path}"
            df = pd.read_csv(path)
            assert not df.empty, f"Empty artifact: {path}"


@pytest.mark.high
@pytest.mark.full
def test_sector_views_keep_row_coherence_with_core_outputs() -> None:
    line_core = read_csv("line_day_features_scored.csv")
    invest_core = read_csv("investment_prioritization_final.csv")

    for profile_id in EXPECTED_PROFILES:
        line_view = pd.read_csv(_profile_file(profile_id, "line_day_view.csv"))
        invest_view = pd.read_csv(_profile_file(profile_id, "investment_view.csv"))

        assert len(line_view) == len(line_core)
        assert len(invest_view) == len(invest_core)
        assert line_view["sector_profile_id"].eq(profile_id).all()
        assert invest_view["sector_profile_id"].eq(profile_id).all()


@pytest.mark.high
@pytest.mark.full
def test_sector_profiles_are_not_just_label_variants() -> None:
    tubacex_constraints = pd.read_csv(_profile_file("tubacex", "constraints_by_sector.csv"))
    petronor_constraints = pd.read_csv(_profile_file("petronor", "constraints_by_sector.csv"))
    merged_constraints = tubacex_constraints.merge(
        petronor_constraints,
        on="constraint_name",
        suffixes=("_tubacex", "_petronor"),
        how="inner",
    )
    assert not merged_constraints.empty
    assert (
        merged_constraints["constraint_value_tubacex"].astype(str)
        != merged_constraints["constraint_value_petronor"].astype(str)
    ).any()

    tubacex_emission = pd.read_csv(_profile_file("tubacex", "emission_factors.csv"))
    petronor_emission = pd.read_csv(_profile_file("petronor", "emission_factors.csv"))
    merged_emission = tubacex_emission.merge(
        petronor_emission,
        on="fuente_energia",
        suffixes=("_tubacex", "_petronor"),
        how="inner",
    )
    assert not merged_emission.empty
    assert (
        merged_emission["factor_multiplier_tubacex"] != merged_emission["factor_multiplier_petronor"]
    ).any()


@pytest.mark.high
@pytest.mark.quick
@pytest.mark.full
def test_sector_docs_exist() -> None:
    docs = [
        PROJECT_ROOT / "docs" / "sector_parameterization_framework.md",
        PROJECT_ROOT / "docs" / "tubacex_operating_profile.md",
        PROJECT_ROOT / "docs" / "petronor_operating_profile.md",
    ]
    missing = [str(path) for path in docs if not path.exists()]
    assert not missing, f"Missing docs: {missing}"
