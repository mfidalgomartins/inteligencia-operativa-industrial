from __future__ import annotations

from datetime import datetime
from time import perf_counter

from .analysis_report import run_advanced_analysis
from .dashboard import build_dashboard
from .data_generation import generate_synthetic_data
from .diagnostics import build_root_cause_ranking
from .explore_data_audit import run_explore_data_audit
from .feature_engineering import build_feature_layer
from .investment_prioritization import run_investment_prioritization
from .lineage_traceability import build_traceability_layer
from .metric_governance import run_metric_governance
from .notebooks_builder import build_notebooks
from .release_management import write_build_manifest, write_phase_status_report
from .repro_packaging import run_repro_packaging
from .reporting import build_reports
from .scenario_engine import run_scenario_engine
from .sector_parameterization import run_sector_parameterization
from .sql_pipeline import run_sql_pipeline
from .validate_project import run_full_validation
from .visualization import generate_charts


def run_all() -> None:
    run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    # Bootstrap manifest para que el packaging/release pack tenga referencia desde el inicio.
    write_build_manifest(run_id=run_id, steps=[], overall_status="RUNNING")
    write_phase_status_report(run_id=run_id, steps=[], overall_status="RUNNING")

    steps: list[tuple[str, callable]] = [
        ("generate_synthetic_data", generate_synthetic_data),
        ("run_explore_data_audit", run_explore_data_audit),
        ("run_sql_pipeline", run_sql_pipeline),
        ("build_feature_layer", build_feature_layer),
        ("build_root_cause_ranking", build_root_cause_ranking),
        ("run_scenario_engine", run_scenario_engine),
        ("run_investment_prioritization", run_investment_prioritization),
        ("run_metric_governance", lambda: run_metric_governance(run_id=run_id)),
        ("run_sector_parameterization", run_sector_parameterization),
        ("run_advanced_analysis", run_advanced_analysis),
        ("generate_charts", generate_charts),
        ("build_dashboard", build_dashboard),
        ("run_full_validation", run_full_validation),
        # Segunda pasada para alinear dashboard con readiness recién calculado
        # y validar de nuevo consistencia final del artefacto ejecutivo.
        ("build_dashboard_post_validation", build_dashboard),
        ("run_full_validation_final", run_full_validation),
        ("build_traceability_layer", lambda: build_traceability_layer(run_id=run_id)),
        ("build_reports", build_reports),
        ("run_repro_packaging", run_repro_packaging),
        ("build_notebooks", build_notebooks),
    ]

    step_results: list[dict[str, object]] = []
    overall_status = "SUCCESS"

    for step_name, step_fn in steps:
        start = perf_counter()
        try:
            step_fn()
            status = "OK"
        except Exception as exc:  # pragma: no cover - fail fast with manifest
            status = "ERROR"
            overall_status = "FAILED"
            step_results.append(
                {
                    "step": step_name,
                    "status": status,
                    "duration_seconds": perf_counter() - start,
                    "error": str(exc),
                }
            )
            write_build_manifest(run_id=run_id, steps=step_results, overall_status=overall_status)
            write_phase_status_report(run_id=run_id, steps=step_results, overall_status=overall_status)
            raise

        step_results.append(
            {
                "step": step_name,
                "status": status,
                "duration_seconds": perf_counter() - start,
            }
        )

    write_build_manifest(run_id=run_id, steps=step_results, overall_status=overall_status)
    write_phase_status_report(run_id=run_id, steps=step_results, overall_status=overall_status)


if __name__ == "__main__":
    run_all()
