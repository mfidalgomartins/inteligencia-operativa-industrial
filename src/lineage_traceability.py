from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from .config import DATA_PROCESSED_DIR, OUTPUT_DASHBOARD_DIR, OUTPUT_REPORTS_DIR, PROJECT_ROOT


DATA_CONTRACTS_PATH = PROJECT_ROOT / "contracts" / "data" / "data_contracts.json"
METRIC_REGISTRY_PATH = PROJECT_ROOT / "contracts" / "metrics" / "metric_registry.json"


SCORE_WEIGHTS = {
    "energy_saving_score": 0.24,
    "operational_impact_score": 0.20,
    "emissions_reduction_score": 0.12,
    "implementation_feasibility_score": 0.16,
    "payback_score": 0.14,
    "strategic_priority_score": 0.10,
    "uncertainty_index": 0.04,
}


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _dominant_drivers(row: pd.Series) -> str:
    contrib = {score: float(row.get(score, 0.0)) * weight for score, weight in SCORE_WEIGHTS.items()}
    top = sorted(contrib.items(), key=lambda item: item[1], reverse=True)[:3]
    return ";".join([f"{name}:{value:.2f}" for name, value in top])


def _tradeoff_label(row: pd.Series) -> str:
    payback = float(row.get("payback_months", 0))
    capex = float(row.get("capex_estimado", 0))
    energy_score = float(row.get("energy_saving_score", 0))
    op_score = float(row.get("operational_impact_score", 0))
    emission_score = float(row.get("emissions_reduction_score", 0))

    if capex >= 700_000 and payback > 24:
        return "alto_impacto_estructural_vs_time_to_value"
    if payback <= 18 and energy_score >= 60:
        return "captura_rapida_vs_impacto_estructural"
    if op_score >= energy_score and op_score >= emission_score:
        return "continuidad_operativa_vs_ahorro_energetico"
    if emission_score >= max(energy_score, op_score):
        return "descarbonizacion_vs_retorno_financiero"
    return "equilibrio_multicriterio"


def _confidence_score(
    row: pd.Series,
    readiness_level: str,
    success_probability: float,
) -> tuple[float, str]:
    base = {
        "committee-grade": 0.90,
        "committee-candidate": 0.80,
        "not-committee-grade": 0.60,
        "decision-support-only": 0.55,
        "screening-grade-only": 0.50,
        "analytically-acceptable": 0.65,
        "structurally-valid": 0.50,
        "publish-blocked": 0.35,
    }.get(readiness_level, 0.50)

    uncertainty = float(row.get("uncertainty_index", 50.0))
    uncertainty_component = 1.0 - (uncertainty / 100.0)
    conf = 0.50 * base + 0.30 * float(success_probability) + 0.20 * uncertainty_component
    conf = float(np.clip(conf, 0.0, 1.0))
    label = "high" if conf >= 0.75 else ("medium" if conf >= 0.55 else "low")
    return conf, label


def build_traceability_layer(run_id: str) -> dict[str, pd.DataFrame]:
    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # Inputs
    invest = pd.read_csv(DATA_PROCESSED_DIR / "investment_prioritization_final.csv")
    scenario = pd.read_csv(DATA_PROCESSED_DIR / "scenario_table.csv")
    scenario_ranking = pd.read_csv(DATA_PROCESSED_DIR / "scenario_ranking.csv")
    line_scores = pd.read_csv(DATA_PROCESSED_DIR / "line_criticality_scores.csv")
    snapshot = pd.read_csv(DATA_PROCESSED_DIR / "executive_kpi_snapshot.csv").iloc[0]
    readiness = pd.read_csv(DATA_PROCESSED_DIR / "validation_readiness_summary.csv").iloc[0]
    validation_issues = pd.read_csv(DATA_PROCESSED_DIR / "validation_issues_found.csv")
    cross_output = pd.read_csv(DATA_PROCESSED_DIR / "cross_output_consistency.csv")
    portfolio_summary = pd.read_csv(DATA_PROCESSED_DIR / "portfolio_summary.csv").iloc[0]
    metric_registry = pd.read_csv(DATA_PROCESSED_DIR / "semantic_metric_registry.csv")

    metric_registry_json = _read_json(METRIC_REGISTRY_PATH)
    data_contracts_json = _read_json(DATA_CONTRACTS_PATH)

    metric_version_set = str(snapshot["metric_version_set"])
    registry_version = str(metric_registry["registry_version"].iloc[0]) if "registry_version" in metric_registry.columns else "unknown"
    readiness_level = str(readiness["overall_readiness_level"])

    # --- mapping artefacts ---
    metric_to_score_rows = [
        {"metric_name": "annual_saving_proxy", "score_name": "energy_saving_score", "relationship_type": "primary"},
        {"metric_name": "oee_synthetic", "score_name": "operational_impact_score", "relationship_type": "primary"},
        {"metric_name": "loss_value_proxy", "score_name": "operational_impact_score", "relationship_type": "secondary"},
        {"metric_name": "emissions_reduction_proxy", "score_name": "emissions_reduction_score", "relationship_type": "primary"},
        {"metric_name": "payback_months", "score_name": "payback_score", "relationship_type": "primary"},
        {"metric_name": "implementation_complexity_score", "score_name": "implementation_feasibility_score", "relationship_type": "primary"},
        {"metric_name": "execution_risk_score", "score_name": "implementation_feasibility_score", "relationship_type": "secondary"},
        {"metric_name": "strategic_alignment_score", "score_name": "strategic_priority_score", "relationship_type": "primary"},
        {"metric_name": "strategic_relevance_score", "score_name": "strategic_priority_score", "relationship_type": "secondary"},
        {"metric_name": "risk_adjusted_annual_savings_min_gap", "score_name": "uncertainty_index", "relationship_type": "primary"},
    ]
    metric_to_score_map = pd.DataFrame(metric_to_score_rows)

    score_to_decision_rows = [
        {
            "score_name": "improvement_priority_index",
            "decision_output": "decision_rule",
            "rule_logic": ">=56 & uncertainty>=45 => piloto; >=48 => analizar; >=38 => pipeline; else descartar",
            "priority": 1,
        },
        {
            "score_name": "improvement_priority_index",
            "decision_output": "selected_portfolio_flag",
            "rule_logic": "selección sujeta a constraints CAPEX/OPEX/capacidad/dependencias/conflictos",
            "priority": 1,
        },
        {
            "score_name": "payback_score",
            "decision_output": "portfolio_wave",
            "rule_logic": "OLA_1 favorece payback<=24 y burden<=75",
            "priority": 2,
        },
    ]
    score_to_decision_map = pd.DataFrame(score_to_decision_rows)

    # --- lineage levels ---
    dataset_lineage = pd.DataFrame(
        [
            {
                "dataset_name": "mart_line_day",
                "lineage_level": "dataset lineage",
                "layer": "semantic_mart",
                "upstream_datasets": "int_line_hour",
                "owner": "Analytics Engineering",
                "contract_ref": "contracts/data/data_contracts.json#mart_line_day",
            },
            {
                "dataset_name": "line_day_features_scored",
                "lineage_level": "dataset lineage",
                "layer": "scores",
                "upstream_datasets": "mart_line_day;line_day_features",
                "owner": "Industrial Analytics",
                "contract_ref": "contracts/data/data_contracts.json#line_day_features_scored",
            },
            {
                "dataset_name": "scenario_table",
                "lineage_level": "dataset lineage",
                "layer": "decision",
                "upstream_datasets": "mart_line_day;stg_initiatives;stg_capex;stg_scenarios",
                "owner": "Decision Science",
                "contract_ref": "n/a",
            },
            {
                "dataset_name": "investment_prioritization_final",
                "lineage_level": "dataset lineage",
                "layer": "decision",
                "upstream_datasets": "scenario_table;opportunity_priority_scores;scenario_dependencies;scenario_conflicts",
                "owner": "Strategy + PMO Analytics",
                "contract_ref": "contracts/data/data_contracts.json#investment_prioritization_final",
            },
            {
                "dataset_name": "executive_kpi_snapshot",
                "lineage_level": "executive output lineage",
                "layer": "executive_serving",
                "upstream_datasets": "line_day_features_scored;line_criticality_scores;investment_prioritization_final",
                "owner": "BI Lead",
                "contract_ref": "contracts/metrics/metric_registry.json",
            },
            {
                "dataset_name": "industrial-operating-command-center.html",
                "lineage_level": "executive output lineage",
                "layer": "executive_serving",
                "upstream_datasets": "executive_kpi_snapshot;investment_prioritization_final;scenario_table;line_criticality_scores",
                "owner": "BI Lead",
                "contract_ref": "outputs/dashboard/industrial-operating-command-center.html",
            },
        ]
    )

    metric_lineage = metric_registry[["technical_name", "formula", "owner", "metric_version_set", "registry_version"]].copy()
    metric_lineage = metric_lineage.rename(columns={"technical_name": "metric_name"})
    metric_lineage["lineage_level"] = "metric lineage"
    metric_lineage["canonical_dataset"] = metric_lineage["metric_name"].map(
        {
            "production_volume": "line_day_features_scored",
            "oee_synthetic": "line_day_features_scored",
            "sec": "line_day_features_scored",
            "loss_value_proxy": "line_day_features_scored",
            "annual_saving_proxy": "investment_prioritization_final",
            "npv_risk_adjusted": "investment_prioritization_final",
            "improvement_priority_index": "investment_prioritization_final",
        }
    )
    metric_lineage["contract_match_flag"] = metric_lineage["metric_name"].isin(set(metric_registry["technical_name"])).astype(int)

    score_lineage = pd.DataFrame(
        [
            {
                "score_name": score_name,
                "lineage_level": "score lineage",
                "formula": f"{weight} * {score_name}",
                "component_metrics": ";".join(metric_to_score_map.loc[metric_to_score_map["score_name"] == score_name, "metric_name"].tolist()),
                "weight": weight,
                "upstream_dataset": "investment_prioritization_final",
            }
            for score_name, weight in SCORE_WEIGHTS.items()
        ]
    )
    score_lineage = pd.concat(
        [
            score_lineage,
            pd.DataFrame(
                [
                    {
                        "score_name": "improvement_priority_index",
                        "lineage_level": "score lineage",
                        "formula": "sum(weight_i * component_score_i)",
                        "component_metrics": ";".join(SCORE_WEIGHTS.keys()),
                        "weight": 1.0,
                        "upstream_dataset": "investment_prioritization_final",
                    }
                ]
            ),
        ],
        ignore_index=True,
    )

    # Scenario base enrichment per recommendation
    scenario_base = scenario[scenario["macro_scenario"].str.lower() == "base"].copy()
    if scenario_base.empty:
        scenario_base = scenario.copy()
    scenario_base = scenario_base[
        [
            "iniciativa_id",
            "macro_scenario",
            "scenario_key",
            "success_probability",
            "dependency_penalty_pct",
            "conflict_penalty_pct",
            "implementation_burden",
            "strategic_relevance_score",
        ]
    ].copy()
    scenario_base = scenario_base.groupby("iniciativa_id", as_index=False).agg(
        macro_scenario=("macro_scenario", "first"),
        scenario_key=("scenario_key", "first"),
        success_probability=("success_probability", "mean"),
        dependency_penalty_pct=("dependency_penalty_pct", "max"),
        conflict_penalty_pct=("conflict_penalty_pct", "max"),
        implementation_burden_scenario=("implementation_burden", "mean"),
        strategic_relevance_score_scenario=("strategic_relevance_score", "mean"),
    )

    scenario_rank = (
        scenario_ranking[["iniciativa_id", "score_tradeoff", "ranking"]]
        .groupby("iniciativa_id", as_index=False)
        .agg(
            score_tradeoff=("score_tradeoff", "mean"),
            ranking=("ranking", "min"),
        )
    )

    rec = invest.merge(scenario_base, on="iniciativa_id", how="left").merge(scenario_rank, on="iniciativa_id", how="left")
    rec["line_criticality_score"] = rec.merge(line_scores[["planta_id", "linea_id", "line_criticality_score"]], on=["planta_id", "linea_id"], how="left")[
        "line_criticality_score"
    ].fillna(0.0)

    caveat_codes = validation_issues[validation_issues["effective_status"].isin(["WARN", "ERROR"])]["issue_id"].dropna().astype(str).tolist()
    caveat_codes_str = ";".join(caveat_codes) if caveat_codes else "none"

    constraints_core = (
        f"capex_budget={float(portfolio_summary['capex_budget']):.0f};"
        f"opex_budget={float(portfolio_summary['opex_budget']):.0f};"
        "selection_capacity=24;wave1<=10;wave2<=10"
    )

    recommendation_rows: list[dict[str, object]] = []
    decision_lineage_rows: list[dict[str, object]] = []
    for row in rec.itertuples(index=False):
        conf_value, conf_label = _confidence_score(
            row=row._asdict(),  # type: ignore[arg-type]
            readiness_level=readiness_level,
            success_probability=float(getattr(row, "success_probability", 0.6) or 0.6),
        )
        metrics_involved = "annual_saving_proxy;npv_risk_adjusted;payback_months;oee_synthetic;sec;loss_value_proxy"
        scores_involved = ";".join(list(SCORE_WEIGHTS.keys()) + ["improvement_priority_index"])
        active_constraints = (
            constraints_core
            + f";dependency_penalty={float(getattr(row, 'dependency_penalty_pct', 0.0) or 0.0):.2f}"
            + f";conflict_penalty={float(getattr(row, 'conflict_penalty_pct', 0.0) or 0.0):.2f}"
            + f";constraint_reason={str(getattr(row, 'portfolio_constraint_reason', 'n/a'))}"
        )

        recommendation_rows.append(
            {
                "recommendation_id": str(row.iniciativa_id),
                "run_id": run_id,
                "selected_flag": int(row.selected_portfolio_flag),
                "decision_rule": str(row.decision_rule),
                "portfolio_wave": str(row.portfolio_wave),
                "objective_contribution": float(row.portfolio_npv_contribution),
                "objective_metric": "portfolio_npv_contribution",
                "dominant_drivers": _dominant_drivers(pd.Series(row._asdict())),
                "metrics_involved": metrics_involved,
                "scores_involved": scores_involved,
                "active_constraints": active_constraints,
                "scenario_used": str(getattr(row, "macro_scenario", "Base")),
                "scenario_key_used": str(getattr(row, "scenario_key", "n/a")),
                "trade_offs": _tradeoff_label(pd.Series(row._asdict())),
                "confidence_score": conf_value,
                "confidence_band": conf_label,
                "caveat_codes": caveat_codes_str,
                "metric_version_set": metric_version_set,
                "metric_registry_version": registry_version,
                "data_contract_version": str(data_contracts_json.get("version", "unknown")),
                "readiness_level": readiness_level,
                "validation_evidence_grade": str(readiness["evidence_grade"]),
                "recommended_sequence": str(row.recommended_sequence),
                "initiative_tier": str(row.initiative_tier),
                "main_business_case": str(row.main_business_case),
            }
        )

        decision_lineage_rows.append(
            {
                "decision_id": f"DEC_{row.iniciativa_id}",
                "recommendation_id": str(row.iniciativa_id),
                "lineage_level": "decision lineage",
                "selected_flag": int(row.selected_portfolio_flag),
                "decision_output": str(row.decision_rule),
                "upstream_scores": scores_involved,
                "constraints_applied": active_constraints,
                "scenario_reference": str(getattr(row, "macro_scenario", "Base")),
                "objective_contribution": float(row.portfolio_npv_contribution),
                "caveat_codes": caveat_codes_str,
            }
        )

    recommendation_trace_table = pd.DataFrame(recommendation_rows)
    decision_lineage = pd.DataFrame(decision_lineage_rows)

    # Executive output traceability
    executive_output_trace_table = pd.DataFrame(
        [
            {
                "output_id": "EXEC_SNAPSHOT_MAIN",
                "output_name": "Executive KPI Snapshot",
                "output_path": "data/processed/executive_kpi_snapshot.csv",
                "output_type": "dataset",
                "run_id": run_id,
                "source_datasets": "line_day_features_scored;line_criticality_scores;investment_prioritization_final",
                "source_metrics": "production_volume;oee_synthetic;sec;loss_value_proxy;annual_saving_proxy;portfolio_npv_risk_adjusted",
                "source_scores": "line_criticality_score;improvement_priority_index",
                "decision_dependencies": "selected_portfolio_flag;portfolio_npv_contribution",
                "contracts_active": f"metrics={metric_version_set};data_contracts={data_contracts_json.get('version', 'unknown')}",
                "readiness_level": readiness_level,
                "caveat_codes": caveat_codes_str,
            },
            {
                "output_id": "EXEC_DASHBOARD_DECISION",
                "output_name": "Dashboard Decision Block",
                "output_path": "outputs/dashboard/industrial-operating-command-center.html",
                "output_type": "dashboard",
                "run_id": run_id,
                "source_datasets": "executive_kpi_snapshot;investment_prioritization_final;line_criticality_scores;scenario_table",
                "source_metrics": "portfolio_npv_risk_adjusted;annual_saving_proxy;line_criticality_score",
                "source_scores": "equipment_energy_anomaly_score;improvement_priority_index;line_criticality_score",
                "decision_dependencies": "decision_rule;portfolio_wave;selected_portfolio_flag",
                "contracts_active": f"metrics={metric_version_set};data_contracts={data_contracts_json.get('version', 'unknown')}",
                "readiness_level": readiness_level,
                "caveat_codes": caveat_codes_str,
            },
            {
                "output_id": "EXEC_ADVANCED_ANALYSIS_REPORT",
                "output_name": "Advanced Analysis Report",
                "output_path": "outputs/reports/advanced_analysis_report.md",
                "output_type": "report",
                "run_id": run_id,
                "source_datasets": "line_criticality_scores;equipment_energy_anomaly_scores;loss_root_cause_score;investment_prioritization_final",
                "source_metrics": "line_criticality_score;equipment_energy_anomaly_score;loss_root_cause_score;annual_saving_proxy",
                "source_scores": "opportunity_priority_score;improvement_priority_index",
                "decision_dependencies": "main_findings_prioritized;decision_rule",
                "contracts_active": f"metrics={metric_version_set};data_contracts={data_contracts_json.get('version', 'unknown')}",
                "readiness_level": readiness_level,
                "caveat_codes": caveat_codes_str,
            },
        ]
    )

    # Version dependency map
    version_dependency_map = pd.DataFrame(
        [
            {
                "run_id": run_id,
                "metric_version_set": metric_version_set,
                "metric_registry_version": registry_version,
                "data_contract_version": str(data_contracts_json.get("version", "unknown")),
                "metric_registry_sha256": _sha256(METRIC_REGISTRY_PATH),
                "data_contract_sha256": _sha256(DATA_CONTRACTS_PATH),
                "scenario_engine_code_sha256": _sha256(PROJECT_ROOT / "src" / "scenario_engine.py"),
                "investment_prioritization_code_sha256": _sha256(PROJECT_ROOT / "src" / "investment_prioritization.py"),
                "lineage_module_sha256": _sha256(PROJECT_ROOT / "src" / "lineage_traceability.py"),
                "dashboard_sha256": _sha256(OUTPUT_DASHBOARD_DIR / "industrial-operating-command-center.html")
                if (OUTPUT_DASHBOARD_DIR / "industrial-operating-command-center.html").exists()
                else "",
                "validation_readiness_level": readiness_level,
                "validation_publish_decision": str(readiness["publish_decision"]),
                "generated_at_utc": datetime.utcnow().isoformat(timespec="seconds"),
            }
        ]
    )

    # Registry + edges (DAG)
    lineage_registry = pd.concat(
        [
            dataset_lineage.rename(columns={"dataset_name": "node_name"}).assign(
                node_id=lambda df: "dataset::" + df["node_name"],
                node_type="dataset",
                version=run_id,
                description="dataset lineage node",
            )[["node_id", "node_name", "node_type", "lineage_level", "layer", "owner", "version", "contract_ref", "description"]],
            metric_lineage.assign(
                node_id=lambda df: "metric::" + df["metric_name"],
                node_name=lambda df: df["metric_name"],
                node_type="metric",
                layer="semantic_layer",
                version=lambda df: df["metric_version_set"],
                contract_ref="contracts/metrics/metric_registry.json",
                description="metric lineage node",
            )[["node_id", "node_name", "node_type", "lineage_level", "layer", "owner", "version", "contract_ref", "description"]],
            score_lineage.assign(
                node_id=lambda df: "score::" + df["score_name"],
                node_name=lambda df: df["score_name"],
                node_type="score",
                layer="scores",
                owner="Analytics Engineering",
                version=run_id,
                contract_ref="contracts/metrics/metric_registry.json",
                description="score lineage node",
            )[["node_id", "node_name", "node_type", "lineage_level", "layer", "owner", "version", "contract_ref", "description"]],
            decision_lineage.assign(
                node_id=lambda df: "decision::" + df["decision_id"],
                node_name=lambda df: df["decision_id"],
                node_type="decision",
                layer="decision",
                owner="Strategy + PMO",
                version=run_id,
                contract_ref="data/processed/investment_prioritization_final.csv",
                description="decision lineage node",
            )[["node_id", "node_name", "node_type", "lineage_level", "layer", "owner", "version", "contract_ref", "description"]],
            executive_output_trace_table.assign(
                node_id=lambda df: "output::" + df["output_id"],
                node_name=lambda df: df["output_name"],
                node_type="executive_output",
                lineage_level="executive output lineage",
                layer="executive_serving",
                owner="BI Lead",
                version=run_id,
                contract_ref=lambda df: df["output_path"],
                description="executive output lineage node",
            )[["node_id", "node_name", "node_type", "lineage_level", "layer", "owner", "version", "contract_ref", "description"]],
        ],
        ignore_index=True,
    )

    edge_rows: list[dict[str, str]] = []
    dataset_edges = [
        ("dataset::mart_line_day", "dataset::line_day_features_scored"),
        ("dataset::line_day_features_scored", "dataset::executive_kpi_snapshot"),
        ("dataset::scenario_table", "dataset::investment_prioritization_final"),
        ("dataset::investment_prioritization_final", "output::EXEC_SNAPSHOT_MAIN"),
        ("dataset::investment_prioritization_final", "output::EXEC_DASHBOARD_DECISION"),
        ("dataset::line_day_features_scored", "output::EXEC_DASHBOARD_DECISION"),
        ("dataset::line_day_features_scored", "output::EXEC_ADVANCED_ANALYSIS_REPORT"),
    ]
    for parent, child in dataset_edges:
        edge_rows.append({"parent_node_id": parent, "child_node_id": child, "edge_type": "dataset_flow"})

    for row in metric_to_score_map.itertuples(index=False):
        edge_rows.append(
            {
                "parent_node_id": f"metric::{row.metric_name}",
                "child_node_id": f"score::{row.score_name}",
                "edge_type": "metric_to_score",
            }
        )

    for decision in decision_lineage.itertuples(index=False):
        for score_name in SCORE_WEIGHTS.keys():
            edge_rows.append(
                {
                    "parent_node_id": f"score::{score_name}",
                    "child_node_id": f"decision::{decision.decision_id}",
                    "edge_type": "score_to_decision",
                }
            )
        edge_rows.append(
            {
                "parent_node_id": "score::improvement_priority_index",
                "child_node_id": f"decision::{decision.decision_id}",
                "edge_type": "score_to_decision",
            }
        )

    lineage_edges = pd.DataFrame(edge_rows).drop_duplicates().reset_index(drop=True)

    # Persist artefacts
    dataset_lineage.to_csv(DATA_PROCESSED_DIR / "dataset_lineage.csv", index=False)
    metric_lineage.to_csv(DATA_PROCESSED_DIR / "metric_lineage.csv", index=False)
    score_lineage.to_csv(DATA_PROCESSED_DIR / "score_lineage.csv", index=False)
    decision_lineage.to_csv(DATA_PROCESSED_DIR / "decision_lineage.csv", index=False)
    executive_output_trace_table.to_csv(DATA_PROCESSED_DIR / "executive_output_lineage.csv", index=False)

    lineage_registry.to_csv(DATA_PROCESSED_DIR / "lineage_registry.csv", index=False)
    lineage_edges.to_csv(DATA_PROCESSED_DIR / "lineage_edges.csv", index=False)
    metric_to_score_map.to_csv(DATA_PROCESSED_DIR / "metric_to_score_map.csv", index=False)
    score_to_decision_map.to_csv(DATA_PROCESSED_DIR / "score_to_decision_map.csv", index=False)
    recommendation_trace_table.to_csv(DATA_PROCESSED_DIR / "recommendation_trace_table.csv", index=False)
    executive_output_trace_table.to_csv(DATA_PROCESSED_DIR / "executive_output_trace_table.csv", index=False)
    version_dependency_map.to_csv(DATA_PROCESSED_DIR / "version_dependency_map.csv", index=False)

    return {
        "dataset_lineage": dataset_lineage,
        "metric_lineage": metric_lineage,
        "score_lineage": score_lineage,
        "decision_lineage": decision_lineage,
        "lineage_registry": lineage_registry,
        "lineage_edges": lineage_edges,
        "metric_to_score_map": metric_to_score_map,
        "score_to_decision_map": score_to_decision_map,
        "recommendation_trace_table": recommendation_trace_table,
        "executive_output_trace_table": executive_output_trace_table,
        "version_dependency_map": version_dependency_map,
    }


if __name__ == "__main__":
    snapshot_path = DATA_PROCESSED_DIR / "executive_kpi_snapshot.csv"
    if snapshot_path.exists():
        rid = str(pd.read_csv(snapshot_path).iloc[0]["run_id"])
    else:
        rid = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    build_traceability_layer(run_id=rid)
