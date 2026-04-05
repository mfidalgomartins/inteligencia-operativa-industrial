from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from .config import DATA_PROCESSED_DIR, DATA_RAW_DIR, OUTPUT_REPORTS_DIR, PROJECT_ROOT


SECTOR_CONFIG_DIR = PROJECT_ROOT / "configs" / "sectors"
SECTOR_OUTPUT_DIR = DATA_PROCESSED_DIR / "sector"

REQUIRED_PROFILE_KEYS = [
    "profile_id",
    "profile_name",
    "profile_version",
    "common_core_version_compat",
    "taxonomy_mapping",
    "process_family_definitions",
    "benchmark_set",
    "energy_logic",
    "emission_factors",
    "maintenance_logic",
    "initiative_library",
    "criticality_logic",
    "constraints_by_sector",
    "scenario_sensitivities",
    "kpi_priority",
    "metric_caveats",
    "misuse_risks",
]


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_common_core() -> dict:
    path = SECTOR_CONFIG_DIR / "common_core.json"
    if not path.exists():
        raise FileNotFoundError(f"No se encontró configuración común sectorial: {path}")
    return _read_json(path)


def _load_profiles() -> list[dict]:
    profile_paths = sorted(SECTOR_CONFIG_DIR.glob("*_profile.json"))
    if not profile_paths:
        raise FileNotFoundError("No se encontraron perfiles sectoriales en configs/sectors/*_profile.json")
    return [_read_json(path) for path in profile_paths]


def _validate_profile(profile: dict, common_core_version: str) -> list[str]:
    errors: list[str] = []
    missing = [key for key in REQUIRED_PROFILE_KEYS if key not in profile]
    if missing:
        errors.append(f"missing_keys={','.join(missing)}")

    if str(profile.get("common_core_version_compat", "")) != str(common_core_version):
        errors.append(
            f"common_core_version_mismatch profile={profile.get('profile_id','unknown')} expected={common_core_version}"
        )

    taxonomy = profile.get("taxonomy_mapping", {})
    required_taxonomy = [
        "familia_proceso",
        "tipo_equipo",
        "categoria_iniciativa",
        "tipo_iniciativa",
        "fuente_energia",
    ]
    for key in required_taxonomy:
        if key not in taxonomy or not isinstance(taxonomy[key], dict) or not taxonomy[key]:
            errors.append(f"taxonomy_mapping_incomplete={key}")
    return errors


def _flatten_taxonomy(profile_id: str, taxonomy_mapping: dict[str, dict[str, str]]) -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    for domain, mapping in taxonomy_mapping.items():
        for source_value, target_value in mapping.items():
            rows.append(
                {
                    "sector_profile_id": profile_id,
                    "domain": domain,
                    "source_value": str(source_value),
                    "target_value": str(target_value),
                }
            )
    return pd.DataFrame(rows)


def _build_asset_hierarchy(
    profile_id: str,
    taxonomy_mapping: dict[str, dict[str, str]],
) -> pd.DataFrame:
    plantas = pd.read_csv(DATA_RAW_DIR / "plantas.csv")
    lineas = pd.read_csv(DATA_RAW_DIR / "lineas_proceso.csv")
    equipos = pd.read_csv(DATA_RAW_DIR / "equipos.csv")

    hierarchy = (
        equipos.merge(lineas[["linea_id", "planta_id", "familia_proceso", "nombre_linea"]], on="linea_id", how="left")
        .merge(plantas[["planta_id", "nombre_planta", "tipo_planta", "region"]], on="planta_id", how="left")
        .copy()
    )

    family_map = taxonomy_mapping.get("familia_proceso", {})
    equipment_map = taxonomy_mapping.get("tipo_equipo", {})
    hierarchy["sector_profile_id"] = profile_id
    hierarchy["sector_process_family"] = hierarchy["familia_proceso"].map(family_map).fillna(hierarchy["familia_proceso"])
    hierarchy["sector_equipment_type"] = hierarchy["tipo_equipo"].map(equipment_map).fillna(hierarchy["tipo_equipo"])

    cols = [
        "sector_profile_id",
        "planta_id",
        "nombre_planta",
        "tipo_planta",
        "region",
        "linea_id",
        "nombre_linea",
        "familia_proceso",
        "sector_process_family",
        "equipo_id",
        "tipo_equipo",
        "sector_equipment_type",
        "subsistema",
    ]
    return hierarchy[cols].sort_values(["planta_id", "linea_id", "equipo_id"]).reset_index(drop=True)


def _build_process_family_definitions(profile_id: str, profile: dict) -> pd.DataFrame:
    raw_defs = profile.get("process_family_definitions", {})
    rows: list[dict[str, object]] = []
    for family, payload in raw_defs.items():
        rows.append(
            {
                "sector_profile_id": profile_id,
                "familia_proceso": family,
                "sector_process_group": payload.get("sector_process_group", ""),
                "benchmark_priority": payload.get("benchmark_priority", ""),
                "kpi_focus": "|".join(payload.get("kpi_focus", [])),
                "operational_risk_focus": payload.get("operational_risk_focus", ""),
            }
        )
    return pd.DataFrame(rows).sort_values("familia_proceso").reset_index(drop=True)


def _build_benchmark_set(profile_id: str, profile: dict) -> pd.DataFrame:
    benchmark = pd.read_csv(DATA_RAW_DIR / "benchmark_externo_proxy.csv").copy()
    benchmark["periodo"] = pd.to_datetime(benchmark["periodo"], errors="coerce")
    last_period = benchmark["periodo"].max()
    latest = benchmark[benchmark["periodo"] == last_period].copy()

    bench_cfg = profile.get("benchmark_set", {})
    sec_mult = bench_cfg.get("sec_multiplier_by_family", {})
    oee_shift = bench_cfg.get("oee_shift_by_family", {})
    availability_shift = bench_cfg.get("availability_shift_by_family", {})
    emissions_mult = bench_cfg.get("emissions_multiplier_by_family", {})

    latest["sector_profile_id"] = profile_id
    latest["sec_multiplier"] = latest["familia_proceso"].map(sec_mult).fillna(1.0)
    latest["oee_shift"] = latest["familia_proceso"].map(oee_shift).fillna(0.0)
    latest["availability_shift"] = latest["familia_proceso"].map(availability_shift).fillna(0.0)
    latest["emissions_multiplier"] = latest["familia_proceso"].map(emissions_mult).fillna(1.0)

    latest["sec_referencia_sector"] = latest["sec_referencia"] * latest["sec_multiplier"]
    latest["oee_referencia_sector"] = (latest["oee_referencia"] + latest["oee_shift"]).clip(lower=0.0, upper=1.0)
    latest["disponibilidad_referencia_sector"] = (
        latest["disponibilidad_referencia"] + latest["availability_shift"]
    ).clip(lower=0.0, upper=1.0)
    latest["emisiones_referencia_sector"] = latest["emisiones_referencia"] * latest["emissions_multiplier"]

    cols = [
        "sector_profile_id",
        "periodo",
        "familia_proceso",
        "sec_referencia",
        "sec_multiplier",
        "sec_referencia_sector",
        "oee_referencia",
        "oee_shift",
        "oee_referencia_sector",
        "disponibilidad_referencia",
        "availability_shift",
        "disponibilidad_referencia_sector",
        "emisiones_referencia",
        "emissions_multiplier",
        "emisiones_referencia_sector",
    ]
    return latest[cols].sort_values("familia_proceso").reset_index(drop=True)


def _build_emission_factors(profile_id: str, profile: dict) -> pd.DataFrame:
    emissions = pd.read_csv(DATA_RAW_DIR / "emisiones_estimadas.csv").copy()
    base = (
        emissions.groupby("fuente_energia", as_index=False)["factor_emision_proxy"]
        .median()
        .rename(columns={"factor_emision_proxy": "factor_emision_base"})
    )
    ef_cfg = profile.get("emission_factors", {})
    multipliers = ef_cfg.get("multiplier_by_source", {})
    base["sector_profile_id"] = profile_id
    base["factor_multiplier"] = base["fuente_energia"].map(multipliers).fillna(1.0)
    base["factor_emision_sector"] = base["factor_emision_base"] * base["factor_multiplier"]
    base["carbon_cost_eur_ton"] = float(ef_cfg.get("carbon_cost_eur_ton_base", 85.0))
    return base[
        [
            "sector_profile_id",
            "fuente_energia",
            "factor_emision_base",
            "factor_multiplier",
            "factor_emision_sector",
            "carbon_cost_eur_ton",
        ]
    ].sort_values("fuente_energia")


def _build_initiative_library(profile_id: str, profile: dict) -> pd.DataFrame:
    initiatives = pd.read_csv(DATA_RAW_DIR / "iniciativas_mejora.csv")
    lib_cfg = profile.get("initiative_library", {})
    category_map = profile.get("taxonomy_mapping", {}).get("categoria_iniciativa", {})
    type_map = profile.get("taxonomy_mapping", {}).get("tipo_iniciativa", {})
    mandatory_categories = set(lib_cfg.get("mandatory_categories", []))
    preferred_sequence = {name: i + 1 for i, name in enumerate(lib_cfg.get("preferred_sequence", []))}

    out = initiatives[["iniciativa_id", "categoria_iniciativa", "tipo_iniciativa", "quick_win_flag"]].copy()
    out["sector_profile_id"] = profile_id
    out["categoria_sectorial"] = out["categoria_iniciativa"].map(category_map).fillna(out["categoria_iniciativa"])
    out["tipo_sectorial"] = out["tipo_iniciativa"].map(type_map).fillna(out["tipo_iniciativa"])
    out["mandatory_by_profile"] = out["categoria_iniciativa"].isin(mandatory_categories).astype(int)
    out["recommended_wave_index"] = out["tipo_iniciativa"].map(preferred_sequence).fillna(99).astype(int)
    return out[
        [
            "sector_profile_id",
            "iniciativa_id",
            "categoria_iniciativa",
            "categoria_sectorial",
            "tipo_iniciativa",
            "tipo_sectorial",
            "quick_win_flag",
            "mandatory_by_profile",
            "recommended_wave_index",
        ]
    ].sort_values("iniciativa_id")


def _build_constraints(profile_id: str, profile: dict) -> pd.DataFrame:
    constraints = profile.get("constraints_by_sector", {})
    rows = [{"sector_profile_id": profile_id, "constraint_name": k, "constraint_value": v} for k, v in constraints.items()]
    return pd.DataFrame(rows).sort_values("constraint_name").reset_index(drop=True)


def _build_criticality_logic(profile_id: str, profile: dict) -> pd.DataFrame:
    crit_cfg = profile.get("criticality_logic", {})
    rows: list[dict[str, object]] = []
    for name, value in crit_cfg.items():
        if isinstance(value, dict):
            for nested_key, nested_value in value.items():
                rows.append(
                    {
                        "sector_profile_id": profile_id,
                        "logic_group": name,
                        "logic_key": nested_key,
                        "logic_value": nested_value,
                    }
                )
        else:
            rows.append(
                {
                    "sector_profile_id": profile_id,
                    "logic_group": "weights",
                    "logic_key": name,
                    "logic_value": value,
                }
            )
    return pd.DataFrame(rows).sort_values(["logic_group", "logic_key"]).reset_index(drop=True)


def _build_scenario_sensitivities(profile_id: str, profile: dict) -> pd.DataFrame:
    scen = profile.get("scenario_sensitivities", {})
    rows = [{"sector_profile_id": profile_id, "sensitivity_name": k, "sensitivity_value": v} for k, v in scen.items()]
    return pd.DataFrame(rows).sort_values("sensitivity_name").reset_index(drop=True)


def _build_metric_caveats(profile_id: str, profile: dict) -> pd.DataFrame:
    rows = []
    for entry in profile.get("metric_caveats", []):
        rows.append(
            {
                "sector_profile_id": profile_id,
                "metric_name": entry.get("metric_name", ""),
                "caveat": entry.get("caveat", ""),
                "misuse_prevention": entry.get("misuse_prevention", ""),
            }
        )
    return pd.DataFrame(rows)


def _build_misuse_risks(profile_id: str, profile: dict) -> pd.DataFrame:
    rows = []
    for i, value in enumerate(profile.get("misuse_risks", []), start=1):
        rows.append({"sector_profile_id": profile_id, "risk_id": f"{profile_id.upper()}_R{i:02d}", "risk": value})
    return pd.DataFrame(rows)


def _build_kpi_priority(profile_id: str, profile: dict) -> pd.DataFrame:
    priorities = profile.get("kpi_priority", {}).get("kpi_weights", {})
    rows = [{"sector_profile_id": profile_id, "kpi_name": k, "priority_weight": v} for k, v in priorities.items()]
    return pd.DataFrame(rows).sort_values("kpi_name").reset_index(drop=True)


def _build_sector_line_day_view(profile_id: str, profile: dict) -> pd.DataFrame:
    line_day = pd.read_csv(DATA_PROCESSED_DIR / "line_day_features_scored.csv").copy()
    tax = profile.get("taxonomy_mapping", {})
    bench = profile.get("benchmark_set", {})
    sec_mult = bench.get("sec_multiplier_by_family", {})
    emissions_mult = bench.get("emissions_multiplier_by_family", {})

    line_day["sector_profile_id"] = profile_id
    line_day["sector_process_family"] = (
        line_day["familia_proceso"].map(tax.get("familia_proceso", {})).fillna(line_day["familia_proceso"])
    )
    line_day["sec_multiplier_sector"] = line_day["familia_proceso"].map(sec_mult).fillna(1.0)
    line_day["sec_sector_adjusted"] = line_day["sec"] * line_day["sec_multiplier_sector"]
    line_day["emissions_multiplier_sector"] = line_day["familia_proceso"].map(emissions_mult).fillna(1.0)
    line_day["emissions_per_unit_sector_adjusted"] = line_day["emissions_per_unit"] * line_day["emissions_multiplier_sector"]
    line_day["sector_benchmark_gap"] = line_day["sec_sector_adjusted"] - line_day["sec_vs_benchmark"]

    cols = [
        "sector_profile_id",
        "fecha",
        "planta_id",
        "linea_id",
        "familia_proceso",
        "sector_process_family",
        "sec",
        "sec_sector_adjusted",
        "sec_vs_benchmark",
        "sector_benchmark_gap",
        "oee_synthetic",
        "availability_rate",
        "loss_value_proxy",
        "emissions_per_unit",
        "emissions_per_unit_sector_adjusted",
    ]
    return line_day[cols]


def _build_sector_investment_view(profile_id: str, profile: dict) -> pd.DataFrame:
    invest = pd.read_csv(DATA_PROCESSED_DIR / "investment_prioritization_final.csv").copy()
    tax = profile.get("taxonomy_mapping", {})
    constraints = profile.get("constraints_by_sector", {})
    sens = profile.get("scenario_sensitivities", {})

    invest["sector_profile_id"] = profile_id
    invest["sector_category"] = invest["categoria_iniciativa"].map(tax.get("categoria_iniciativa", {})).fillna(
        invest["categoria_iniciativa"]
    )
    invest["sector_type"] = invest["tipo_iniciativa"].map(tax.get("tipo_iniciativa", {})).fillna(invest["tipo_iniciativa"])
    invest["sector_capex_budget"] = float(constraints.get("capex_budget_default_eur", 0.0))
    invest["sector_opex_budget"] = float(constraints.get("opex_budget_default_eur", 0.0))
    invest["sector_energy_price_beta"] = float(sens.get("energy_price_beta", 1.0))
    invest["sector_carbon_cost_beta"] = float(sens.get("carbon_cost_beta", 1.0))
    invest["downside_adjusted_value_sector"] = (
        invest["downside_adjusted_value"] * invest["sector_energy_price_beta"] * (0.75 + 0.25 * invest["sector_carbon_cost_beta"])
    )

    cols = [
        "sector_profile_id",
        "iniciativa_id",
        "planta_id",
        "linea_id",
        "categoria_iniciativa",
        "sector_category",
        "tipo_iniciativa",
        "sector_type",
        "quick_win_flag",
        "improvement_priority_index",
        "decision_rule",
        "portfolio_wave",
        "selected_portfolio_flag",
        "annual_saving_proxy",
        "downside_adjusted_value",
        "downside_adjusted_value_sector",
        "capex_estimado",
        "sector_capex_budget",
        "sector_opex_budget",
    ]
    return invest[cols]


def _build_profile_metadata(profile: dict, common_core: dict) -> pd.DataFrame:
    profile_id = profile["profile_id"]
    return pd.DataFrame(
        [
            {
                "sector_profile_id": profile_id,
                "profile_name": profile.get("profile_name", profile_id),
                "profile_version": profile.get("profile_version", "0.0.0"),
                "common_core_version_compat": profile.get("common_core_version_compat", ""),
                "common_core_version_active": common_core.get("common_core_version", ""),
                "build_timestamp_utc": datetime.utcnow().isoformat(timespec="seconds"),
                "kpi_priority_count": len(profile.get("kpi_priority", {}).get("kpi_weights", {})),
                "metric_caveats_count": len(profile.get("metric_caveats", [])),
                "misuse_risks_count": len(profile.get("misuse_risks", [])),
            }
        ]
    )


def run_sector_parameterization() -> dict[str, pd.DataFrame]:
    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    SECTOR_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    common_core = _load_common_core()
    common_core_version = str(common_core.get("common_core_version", ""))
    profiles = _load_profiles()

    registry_rows: list[dict[str, object]] = []
    evidence_rows: list[dict[str, object]] = []
    mapping_coverage_rows: list[dict[str, object]] = []

    lines = pd.read_csv(DATA_RAW_DIR / "lineas_proceso.csv")
    equipos = pd.read_csv(DATA_RAW_DIR / "equipos.csv")
    iniciativas = pd.read_csv(DATA_RAW_DIR / "iniciativas_mejora.csv")
    energia = pd.read_csv(DATA_RAW_DIR / "consumos_energia_horarios.csv")

    observed_domains = {
        "familia_proceso": sorted(lines["familia_proceso"].dropna().astype(str).unique().tolist()),
        "tipo_equipo": sorted(equipos["tipo_equipo"].dropna().astype(str).unique().tolist()),
        "categoria_iniciativa": sorted(iniciativas["categoria_iniciativa"].dropna().astype(str).unique().tolist()),
        "tipo_iniciativa": sorted(iniciativas["tipo_iniciativa"].dropna().astype(str).unique().tolist()),
        "fuente_energia": sorted(energia["fuente_energia"].dropna().astype(str).unique().tolist()),
    }

    for profile in profiles:
        profile_id = str(profile["profile_id"])
        errors = _validate_profile(profile=profile, common_core_version=common_core_version)
        if errors:
            raise ValueError(f"Perfil sectorial inválido '{profile_id}': {'; '.join(errors)}")

        profile_dir = SECTOR_OUTPUT_DIR / profile_id
        profile_dir.mkdir(parents=True, exist_ok=True)

        taxonomy = _flatten_taxonomy(profile_id=profile_id, taxonomy_mapping=profile["taxonomy_mapping"])
        asset_hierarchy = _build_asset_hierarchy(profile_id=profile_id, taxonomy_mapping=profile["taxonomy_mapping"])
        process_defs = _build_process_family_definitions(profile_id=profile_id, profile=profile)
        benchmark_set = _build_benchmark_set(profile_id=profile_id, profile=profile)
        emission_factors = _build_emission_factors(profile_id=profile_id, profile=profile)
        initiative_library = _build_initiative_library(profile_id=profile_id, profile=profile)
        criticality_logic = _build_criticality_logic(profile_id=profile_id, profile=profile)
        constraints = _build_constraints(profile_id=profile_id, profile=profile)
        scenario_sens = _build_scenario_sensitivities(profile_id=profile_id, profile=profile)
        metric_caveats = _build_metric_caveats(profile_id=profile_id, profile=profile)
        misuse_risks = _build_misuse_risks(profile_id=profile_id, profile=profile)
        kpi_priority = _build_kpi_priority(profile_id=profile_id, profile=profile)
        line_view = _build_sector_line_day_view(profile_id=profile_id, profile=profile)
        investment_view = _build_sector_investment_view(profile_id=profile_id, profile=profile)
        metadata = _build_profile_metadata(profile=profile, common_core=common_core)

        taxonomy.to_csv(profile_dir / "taxonomy_mapping.csv", index=False)
        asset_hierarchy.to_csv(profile_dir / "asset_hierarchy.csv", index=False)
        process_defs.to_csv(profile_dir / "process_family_definitions.csv", index=False)
        benchmark_set.to_csv(profile_dir / "benchmark_set.csv", index=False)
        emission_factors.to_csv(profile_dir / "emission_factors.csv", index=False)
        initiative_library.to_csv(profile_dir / "initiative_library.csv", index=False)
        criticality_logic.to_csv(profile_dir / "criticality_logic.csv", index=False)
        constraints.to_csv(profile_dir / "constraints_by_sector.csv", index=False)
        scenario_sens.to_csv(profile_dir / "scenario_sensitivities.csv", index=False)
        metric_caveats.to_csv(profile_dir / "metric_caveats.csv", index=False)
        misuse_risks.to_csv(profile_dir / "misuse_risks.csv", index=False)
        kpi_priority.to_csv(profile_dir / "kpi_priority.csv", index=False)
        line_view.to_csv(profile_dir / "line_day_view.csv", index=False)
        investment_view.to_csv(profile_dir / "investment_view.csv", index=False)
        metadata.to_csv(profile_dir / "profile_metadata.csv", index=False)

        for domain, observed_values in observed_domains.items():
            mapped_values = profile.get("taxonomy_mapping", {}).get(domain, {})
            coverage = sum(1 for value in observed_values if value in mapped_values)
            mapping_coverage_rows.append(
                {
                    "sector_profile_id": profile_id,
                    "domain": domain,
                    "observed_values": len(observed_values),
                    "mapped_values": coverage,
                    "mapping_coverage_pct": 100.0 * coverage / len(observed_values) if observed_values else 100.0,
                }
            )

        registry_rows.append(
            {
                "sector_profile_id": profile_id,
                "profile_name": profile.get("profile_name", profile_id),
                "profile_version": profile.get("profile_version", "0.0.0"),
                "common_core_version_compat": profile.get("common_core_version_compat", ""),
                "profile_dir": str(profile_dir.relative_to(PROJECT_ROOT)),
            }
        )
        evidence_rows.append(
            {
                "sector_profile_id": profile_id,
                "taxonomy_rows": len(taxonomy),
                "asset_hierarchy_rows": len(asset_hierarchy),
                "benchmark_rows": len(benchmark_set),
                "initiative_rows": len(initiative_library),
                "line_day_rows": len(line_view),
                "investment_rows": len(investment_view),
            }
        )

    registry = pd.DataFrame(registry_rows)
    evidence = pd.DataFrame(evidence_rows)
    mapping_coverage = pd.DataFrame(mapping_coverage_rows)

    registry.to_csv(DATA_PROCESSED_DIR / "sector_profile_registry.csv", index=False)
    evidence.to_csv(DATA_PROCESSED_DIR / "sector_adaptation_evidence.csv", index=False)
    mapping_coverage.to_csv(DATA_PROCESSED_DIR / "sector_mapping_coverage.csv", index=False)

    lines = [
        "# Evidencia de Parametrización Sectorial",
        "",
        "## Perfiles activos",
        registry.to_markdown(index=False),
        "",
        "## Cobertura de mappings",
        mapping_coverage.to_markdown(index=False),
        "",
        "## Volumen de artefactos por perfil",
        evidence.to_markdown(index=False),
    ]
    (OUTPUT_REPORTS_DIR / "sector_parameterization_evidence.md").write_text(
        "\n".join(lines),
        encoding="utf-8",
    )

    return {
        "sector_profile_registry": registry,
        "sector_adaptation_evidence": evidence,
        "sector_mapping_coverage": mapping_coverage,
    }


if __name__ == "__main__":
    run_sector_parameterization()
