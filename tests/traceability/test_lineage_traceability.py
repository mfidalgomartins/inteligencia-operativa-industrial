from __future__ import annotations

import json
from collections import defaultdict, deque

import pytest

from tests._helpers import MANIFEST_LATEST, read_csv


def _is_dag(edges_df) -> bool:
    nodes = set(edges_df["parent_node_id"].tolist()) | set(edges_df["child_node_id"].tolist())
    indegree = {node: 0 for node in nodes}
    graph = defaultdict(list)

    for row in edges_df.itertuples(index=False):
        parent = row.parent_node_id
        child = row.child_node_id
        graph[parent].append(child)
        indegree[child] += 1

    queue = deque([node for node, deg in indegree.items() if deg == 0])
    visited = 0
    while queue:
        node = queue.popleft()
        visited += 1
        for nxt in graph[node]:
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                queue.append(nxt)
    return visited == len(nodes)


@pytest.mark.blocker
@pytest.mark.quick
@pytest.mark.full
def test_trace_completeness_recommendation_level() -> None:
    rec_trace = read_csv("recommendation_trace_table.csv")
    invest = read_csv("investment_prioritization_final.csv")

    required_cols = {
        "recommendation_id",
        "selected_flag",
        "objective_contribution",
        "dominant_drivers",
        "metrics_involved",
        "scores_involved",
        "active_constraints",
        "scenario_used",
        "trade_offs",
        "confidence_score",
        "caveat_codes",
        "metric_version_set",
        "data_contract_version",
    }
    assert required_cols.issubset(set(rec_trace.columns))
    assert len(rec_trace) == len(invest)
    assert rec_trace["recommendation_id"].nunique() == len(rec_trace)
    assert rec_trace["confidence_score"].between(0, 1).all()


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_version_consistency_across_trace_snapshot_manifest() -> None:
    rec_trace = read_csv("recommendation_trace_table.csv")
    versions = read_csv("version_dependency_map.csv").iloc[0]
    snapshot = read_csv("executive_kpi_snapshot.csv").iloc[0]

    assert set(rec_trace["run_id"].astype(str)) == {str(snapshot["run_id"])}
    assert set(rec_trace["metric_version_set"].astype(str)) == {str(snapshot["metric_version_set"])}
    assert str(versions["run_id"]) == str(snapshot["run_id"])
    assert str(versions["metric_version_set"]) == str(snapshot["metric_version_set"])

    payload = json.loads(MANIFEST_LATEST.read_text(encoding="utf-8"))
    assert payload["run_id"] == str(snapshot["run_id"])
    assert "lineage_traceability" in payload
    assert payload["lineage_traceability"].get("lineage_ready") is True


@pytest.mark.blocker
@pytest.mark.quick
@pytest.mark.full
def test_lineage_dag_acyclicity() -> None:
    edges = read_csv("lineage_edges.csv")
    assert not edges.empty
    assert _is_dag(edges)


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_metric_contract_match_in_lineage() -> None:
    metric_lineage = read_csv("metric_lineage.csv")
    registry = read_csv("semantic_metric_registry.csv")

    registry_metrics = set(registry["technical_name"].astype(str))
    lineage_metrics = set(metric_lineage["metric_name"].astype(str))
    assert lineage_metrics.issubset(registry_metrics)
    assert metric_lineage["contract_match_flag"].eq(1).all()


@pytest.mark.critical
@pytest.mark.full
def test_score_component_traceability() -> None:
    score_lineage = read_csv("score_lineage.csv")
    metric_to_score = read_csv("metric_to_score_map.csv")
    score_to_decision = read_csv("score_to_decision_map.csv")
    rec_trace = read_csv("recommendation_trace_table.csv")

    score_nodes = set(score_lineage["score_name"].astype(str))
    assert set(metric_to_score["score_name"].astype(str)).issubset(score_nodes)
    assert set(score_to_decision["score_name"].astype(str)).issubset(score_nodes)

    traced_scores = set()
    for val in rec_trace["scores_involved"].astype(str):
        traced_scores.update([s for s in val.split(";") if s])
    assert set(score_to_decision["score_name"].astype(str)).issubset(traced_scores)


@pytest.mark.critical
@pytest.mark.quick
@pytest.mark.full
def test_executive_output_traceability() -> None:
    out_trace = read_csv("executive_output_trace_table.csv")
    required_ids = {
        "EXEC_SNAPSHOT_MAIN",
        "EXEC_DASHBOARD_DECISION",
        "EXEC_ADVANCED_ANALYSIS_REPORT",
    }
    assert required_ids.issubset(set(out_trace["output_id"].astype(str)))
    for col in ["source_datasets", "source_metrics", "source_scores", "contracts_active"]:
        assert out_trace[col].astype(str).str.len().gt(0).all()
