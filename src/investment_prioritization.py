from __future__ import annotations

import math
from time import perf_counter

import numpy as np
import pandas as pd

from .config import DATA_PROCESSED_DIR, DOCS_DIR, OUTPUT_REPORTS_DIR
from .score_stability import (
    COMPARABILITY_ABSOLUTE,
    COMPARABILITY_CONTEXTUAL,
    SCORE_SCALE_VERSION,
    SCORE_SCALING_METHOD,
    anchored_score,
    legacy_local_minmax_score,
    outlier_impact_median_abs_delta,
    spearman_rank_corr,
    topk_overlap,
)


CAPEX_BUDGET = 8_000_000.0
OPEX_BUDGET = 1_800_000.0
MAX_INITIATIVES = 24
MAX_WAVE1 = 10
MAX_WAVE2 = 10
OPTIMIZER_TIME_LIMIT_SEC = 6.0


def _classify_initiative(row: pd.Series) -> str:
    tipo = str(row["tipo_iniciativa"]).lower()
    categoria = str(row["categoria_iniciativa"]).lower()

    if "compliance" in categoria or "regulator" in categoria:
        return "compliance-driven"
    if "mantenimiento" in tipo or "confiabilidad" in categoria:
        return "reliability-protection"
    if row["strategic_alignment_score"] >= 85 and row["operational_urgency_score"] >= 75:
        return "mandatory"
    return "discretionary"


def _financial_stage_label(stage: str) -> str:
    stage = str(stage)
    if stage == "business_case_candidate":
        return "business case candidate (no final)"
    if stage == "pre_feasibility":
        return "pre-feasibility"
    return "screening financiero"


def _tier_from_score(score: float) -> str:
    if score >= 80:
        return "Tier 1"
    if score >= 66:
        return "Tier 2"
    if score >= 52:
        return "Tier 3"
    return "Tier 4"


def _business_case(row: pd.Series) -> str:
    if row["initiative_class"] == "mandatory":
        return "Protección de continuidad operativa crítica"
    if row["initiative_class"] == "compliance-driven":
        return "Cumplimiento regulatorio y mitigación de exposición"
    if row["initiative_class"] == "reliability-protection":
        return "Confiabilidad y disponibilidad estructural"
    if str(row["financial_maturity_stage"]) == "business_case_candidate":
        return "Candidato business case (requiere cierre financiero corporativo)"
    if row["payback_months"] <= 18 and row["energy_saving_score"] >= 65:
        return "Ahorro energético de captura rápida"
    if row["capex_estimado"] >= 700_000 and row["downside_adjusted_value"] > 0:
        return "Transformación CAPEX con valor downside-adjusted positivo"
    return "Optimización incremental orientada a valor"


def _decision_bucket(row: pd.Series) -> str:
    if (
        row["selected_portfolio_flag"] == 1
        and row["portfolio_wave"] == "OLA_1"
        and row["improvement_priority_index"] >= 50
        and row["npv_risk_adjusted"] > 0
        and row["payback_months"] <= 24
    ):
        return "ejecutar ahora"
    if row["selected_portfolio_flag"] == 1 and row["portfolio_wave"] in {"OLA_2", "OLA_3"}:
        return "ejecutar en siguiente ola"
    if row["selected_portfolio_flag"] == 1:
        return "ejecutar en siguiente ola"
    if row["improvement_priority_index"] >= 58 and row["uncertainty_index"] >= 45:
        return "piloto"
    if row["improvement_priority_index"] >= 50:
        return "analizar técnicamente"
    if row["improvement_priority_index"] >= 40:
        return "mantener en pipeline"
    return "descartar"


def _build_dependency_map(dependencies: pd.DataFrame) -> dict[str, list[str]]:
    if dependencies.empty:
        return {}
    return dependencies.groupby("iniciativa_id")["depends_on_iniciativa_id"].apply(lambda s: [str(v) for v in s]).to_dict()


def _build_conflict_map(conflicts: pd.DataFrame) -> dict[str, set[str]]:
    conflict_map: dict[str, set[str]] = {}
    if conflicts.empty:
        return conflict_map

    for row in conflicts.itertuples(index=False):
        i1 = str(row.iniciativa_id_1)
        i2 = str(row.iniciativa_id_2)
        conflict_map.setdefault(i1, set()).add(i2)
        conflict_map.setdefault(i2, set()).add(i1)
    return conflict_map


def _compute_objective_value(df: pd.DataFrame) -> pd.Series:
    # Objetivo financiero principal (EUR): downside + coste de retraso + NPV - riesgo.
    return (
        df["downside_adjusted_value"].fillna(0.0)
        + 0.30 * df["cost_of_delay_12m"].fillna(0.0)
        + 0.10 * df["npv_risk_adjusted"].clip(lower=0.0)
        - 0.15 * df["screening_var_95_npv"].fillna(0.0)
    )


def _compute_anchored_priority_components(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    out["energy_saving_score"] = anchored_score(
        df["net_captured_value_base"].fillna(df["annual_saving_proxy"]),
        low=150_000.0,
        high=1_100_000.0,
    )
    out["operational_impact_score"] = anchored_score(
        df["expected_oee_gain"] * 0.6 + df["operational_urgency_score"] * 0.4,
        low=20.0,
        high=85.0,
    )
    out["emissions_reduction_score"] = anchored_score(
        df["expected_emissions_reduction"] * 0.6 + df["reduccion_emisiones_pct"].fillna(0.0) * 0.4,
        low=5.0,
        high=130.0,
    )
    out["implementation_feasibility_score"] = anchored_score(
        df["implementation_complexity_score"] * 0.45
        + df["execution_risk_score"] * 0.25
        + df["implementation_burden"].fillna(0.0) * 0.30,
        low=20.0,
        high=92.0,
        invert=True,
    )
    out["payback_score"] = anchored_score(df["payback_months"], low=6.0, high=36.0, invert=True)
    out["strategic_priority_score"] = anchored_score(
        df["strategic_alignment_score"] * 0.65 + df["strategic_relevance_score"].fillna(0.0) * 0.35,
        low=30.0,
        high=95.0,
    )
    out["financial_resilience_score"] = anchored_score(
        df["downside_adjusted_value"].fillna(0.0)
        - 0.35 * df["screening_var_95_npv"].fillna(0.0)
        + 0.20 * df["capital_efficiency"].fillna(0.0),
        low=-350_000.0,
        high=1_600_000.0,
    )
    out["uncertainty_index"] = anchored_score(
        df["screening_var_95_npv"].fillna(0.0),
        low=300_000.0,
        high=1_300_000.0,
        invert=True,
    )
    return out


def _compute_legacy_priority_components(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    out["energy_saving_score"] = legacy_local_minmax_score(df["net_captured_value_base"].fillna(df["annual_saving_proxy"]))
    out["operational_impact_score"] = legacy_local_minmax_score(
        df["expected_oee_gain"] * 0.6 + df["operational_urgency_score"] * 0.4
    )
    out["emissions_reduction_score"] = legacy_local_minmax_score(
        df["expected_emissions_reduction"] * 0.6 + df["reduccion_emisiones_pct"].fillna(0.0) * 0.4
    )
    out["implementation_feasibility_score"] = legacy_local_minmax_score(
        df["implementation_complexity_score"] * 0.45
        + df["execution_risk_score"] * 0.25
        + df["implementation_burden"].fillna(0.0) * 0.30,
        invert=True,
    )
    out["payback_score"] = legacy_local_minmax_score(df["payback_months"], invert=True)
    out["strategic_priority_score"] = legacy_local_minmax_score(
        df["strategic_alignment_score"] * 0.65 + df["strategic_relevance_score"].fillna(0.0) * 0.35
    )
    out["financial_resilience_score"] = legacy_local_minmax_score(
        df["downside_adjusted_value"].fillna(0.0)
        - 0.35 * df["screening_var_95_npv"].fillna(0.0)
        + 0.20 * df["capital_efficiency"].fillna(0.0),
    )
    out["uncertainty_index"] = legacy_local_minmax_score(df["screening_var_95_npv"].fillna(0.0), invert=True)
    return out


def _topological_sort(ids: list[str], dep_map: dict[str, list[str]], density_map: dict[str, float]) -> tuple[list[str], list[str]]:
    ids_set = set(ids)
    indeg = {i: 0 for i in ids}
    children: dict[str, list[str]] = {i: [] for i in ids}

    for child in ids:
        for dep in dep_map.get(child, []):
            if dep in ids_set:
                indeg[child] += 1
                children[dep].append(child)

    queue = sorted([i for i in ids if indeg[i] == 0], key=lambda x: density_map.get(x, 0.0), reverse=True)
    ordered: list[str] = []

    while queue:
        node = queue.pop(0)
        ordered.append(node)
        for ch in children.get(node, []):
            indeg[ch] -= 1
            if indeg[ch] == 0:
                queue.append(ch)
                queue = sorted(queue, key=lambda x: density_map.get(x, 0.0), reverse=True)

    cycle_ids = [i for i in ids if i not in set(ordered)]
    return ordered, cycle_ids


def _run_branch_and_bound(
    df: pd.DataFrame,
    dep_map: dict[str, list[str]],
    conflict_map: dict[str, set[str]],
    forced_ids: set[str],
    reliability_ids: set[str],
    reliability_min: int,
    capex_budget: float,
    opex_budget: float,
    max_initiatives: int,
    time_limit_sec: float,
) -> tuple[set[str], dict[str, object]]:
    all_ids = set(df["iniciativa_id"].astype(str))
    obj_map = df.set_index("iniciativa_id")["objective_value_eur"].to_dict()
    capex_map = df.set_index("iniciativa_id")["capex_estimado"].to_dict()
    opex_map = df.set_index("iniciativa_id")["implementation_opex"].to_dict()

    forced_selected = set(forced_ids)
    forced_dropped: set[str] = set()

    # Resolver conflictos entre forzadas conservando mayor valor objetivo.
    for fid in sorted(list(forced_selected), key=lambda x: obj_map.get(x, 0.0), reverse=True):
        for c in conflict_map.get(fid, set()):
            if c in forced_selected and c != fid:
                if obj_map.get(fid, 0.0) >= obj_map.get(c, 0.0):
                    forced_dropped.add(c)
                else:
                    forced_dropped.add(fid)
    forced_selected -= forced_dropped

    # Cierre de dependencias para forzadas.
    changed = True
    while changed:
        changed = False
        for fid in list(forced_selected):
            for dep in dep_map.get(fid, []):
                if dep in all_ids and dep not in forced_selected and dep not in forced_dropped:
                    # Solo añadimos dependencia si no choca con otra forzada activa.
                    dep_conflicts = conflict_map.get(dep, set())
                    if any(c in forced_selected for c in dep_conflicts):
                        continue
                    forced_selected.add(dep)
                    changed = True

    forced_capex = float(sum(capex_map.get(i, 0.0) for i in forced_selected))
    forced_opex = float(sum(opex_map.get(i, 0.0) for i in forced_selected))

    # Si forzadas exceden presupuestos, relajamos únicamente dependencias forzadas no-mandatory/compliance.
    if forced_capex > capex_budget or forced_opex > opex_budget:
        protected_hard = set(
            df[df["initiative_class"].isin(["mandatory", "compliance-driven"])]["iniciativa_id"].astype(str).tolist()
        )
        removable = sorted(
            [i for i in forced_selected if i not in protected_hard],
            key=lambda x: obj_map.get(x, 0.0),
        )
        for rid in removable:
            forced_selected.remove(rid)
            forced_dropped.add(rid)
            forced_capex = float(sum(capex_map.get(i, 0.0) for i in forced_selected))
            forced_opex = float(sum(opex_map.get(i, 0.0) for i in forced_selected))
            if forced_capex <= capex_budget and forced_opex <= opex_budget:
                break

    base_infeasible = bool(forced_capex > capex_budget or forced_opex > opex_budget or len(forced_selected) > max_initiatives)

    candidate_ids = [
        i
        for i in df["iniciativa_id"].astype(str).tolist()
        if i not in forced_selected and i not in forced_dropped
    ]

    preblocked: dict[str, str] = {}
    filtered_candidates: list[str] = []

    for cid in candidate_ids:
        deps = dep_map.get(cid, [])
        missing_dep = False
        unavailable_dep = False
        for d in deps:
            if d not in all_ids:
                missing_dep = True
                break
            if d in forced_dropped:
                unavailable_dep = True
                break
        if missing_dep:
            preblocked[cid] = "dependency_missing_reference"
            continue
        if unavailable_dep:
            preblocked[cid] = "dependency_unavailable"
            continue
        if any(c in forced_selected for c in conflict_map.get(cid, set())):
            preblocked[cid] = "conflict_with_forced"
            continue
        filtered_candidates.append(cid)

    density_map = {
        i: obj_map.get(i, 0.0)
        / (
            1e-6
            + 0.65 * (capex_map.get(i, 0.0) / max(capex_budget, 1.0))
            + 0.35 * (opex_map.get(i, 0.0) / max(opex_budget, 1.0))
        )
        for i in filtered_candidates
    }

    ordered_ids, cycle_ids = _topological_sort(filtered_candidates, dep_map=dep_map, density_map=density_map)
    for c in cycle_ids:
        preblocked[c] = "dependency_cycle"
    ordered_ids = [i for i in ordered_ids if i not in cycle_ids]

    n = len(ordered_ids)
    if base_infeasible:
        meta = {
            "status": "infeasible_forced_constraints",
            "optimality_degree": "no-feasible-formulation",
            "solver_family": "branch_and_bound_binary_selection",
            "time_limit_sec": time_limit_sec,
            "runtime_sec": 0.0,
            "nodes_visited": 0,
            "nodes_pruned_bound": 0,
            "nodes_pruned_infeasible": 0,
            "timed_out": 0,
            "objective_best": float("-inf"),
            "forced_selected_count": len(forced_selected),
            "forced_dropped_count": len(forced_dropped),
            "preblocked_count": len(preblocked),
            "cycle_blocked_count": len(cycle_ids),
            "reliability_min_required": reliability_min,
            "reliability_selected": 0,
            "formulation_note": "No feasible con restricciones hard de forzadas y presupuesto.",
            "preblocked": preblocked,
            "forced_dropped": list(forced_dropped),
            "forced_selected": list(forced_selected),
        }
        return forced_selected, meta

    if n == 0:
        selected_reliability = len(forced_selected & reliability_ids)
        feasible_n0 = selected_reliability >= reliability_min
        status_n0 = "solved_exact" if feasible_n0 else "infeasible_no_feasible_solution"
        optimality_n0 = "exact_for_formulation" if feasible_n0 else "no-feasible-formulation"
        note_n0 = (
            "Sin candidatos discrecionales elegibles tras bloqueos."
            if feasible_n0
            else "Sin candidatos elegibles y cobertura reliability mínima no alcanzable."
        )
        meta = {
            "status": status_n0,
            "optimality_degree": optimality_n0,
            "solver_family": "branch_and_bound_binary_selection",
            "time_limit_sec": time_limit_sec,
            "runtime_sec": 0.0,
            "nodes_visited": 0,
            "nodes_pruned_bound": 0,
            "nodes_pruned_infeasible": 0,
            "timed_out": 0,
            "objective_best": (
                float(sum(obj_map.get(i, 0.0) for i in forced_selected))
                if feasible_n0
                else float("nan")
            ),
            "forced_selected_count": len(forced_selected),
            "forced_dropped_count": len(forced_dropped),
            "preblocked_count": len(preblocked),
            "cycle_blocked_count": len(cycle_ids),
            "reliability_min_required": reliability_min,
            "reliability_selected": selected_reliability,
            "formulation_note": note_n0,
            "preblocked": preblocked,
            "forced_dropped": list(forced_dropped),
            "forced_selected": list(forced_selected),
        }
        return forced_selected, meta

    idx_map = {i: k for k, i in enumerate(ordered_ids)}

    obj = np.array([float(obj_map.get(i, 0.0)) for i in ordered_ids], dtype=float)
    capex = np.array([float(capex_map.get(i, 0.0)) for i in ordered_ids], dtype=float)
    opex = np.array([float(opex_map.get(i, 0.0)) for i in ordered_ids], dtype=float)
    rel = np.array([1 if i in reliability_ids else 0 for i in ordered_ids], dtype=int)

    dep_idx: list[list[int]] = []
    for i in ordered_ids:
        deps_i = []
        for d in dep_map.get(i, []):
            if d in idx_map:
                deps_i.append(idx_map[d])
        dep_idx.append(deps_i)

    conflict_idx: list[set[int]] = []
    for i in ordered_ids:
        cset = set()
        for c in conflict_map.get(i, set()):
            if c in idx_map:
                cset.add(idx_map[c])
        conflict_idx.append(cset)

    rel_suffix = np.zeros(n + 1, dtype=int)
    pos_suffix = np.zeros(n + 1, dtype=float)
    for i in range(n - 1, -1, -1):
        rel_suffix[i] = rel_suffix[i + 1] + rel[i]
        pos_suffix[i] = pos_suffix[i + 1] + max(obj[i], 0.0)

    capex_used_base = forced_capex
    opex_used_base = forced_opex
    count_base = len(forced_selected)
    rel_base = len(forced_selected & reliability_ids)

    max_slots = max_initiatives
    best_obj = float("-inf")
    best_sel_idx: set[int] = set()
    timed_out = False

    nodes_visited = 0
    nodes_pruned_bound = 0
    nodes_pruned_infeasible = 0

    selected_idx: set[int] = set()
    start = perf_counter()

    def upper_bound(idx: int, slots_left: int) -> float:
        if idx >= n or slots_left <= 0:
            return 0.0
        rem = obj[idx:]
        rem_pos = rem[rem > 0]
        if len(rem_pos) == 0:
            return 0.0
        if len(rem_pos) <= slots_left:
            return float(rem_pos.sum())
        # Cota optimista por top valores positivos restantes.
        return float(np.partition(rem_pos, -slots_left)[-slots_left:].sum())

    def dfs(idx: int, capex_used: float, opex_used: float, count_used: int, rel_used: int, curr_obj: float) -> None:
        nonlocal best_obj, best_sel_idx, timed_out
        nonlocal nodes_visited, nodes_pruned_bound, nodes_pruned_infeasible

        if perf_counter() - start > time_limit_sec:
            timed_out = True
            return

        nodes_visited += 1

        if count_used > max_slots or capex_used > capex_budget + 1e-9 or opex_used > opex_budget + 1e-9:
            nodes_pruned_infeasible += 1
            return

        if rel_used + rel_suffix[idx] < reliability_min:
            nodes_pruned_infeasible += 1
            return

        slots_left = max_slots - count_used
        optimistic = curr_obj + upper_bound(idx, slots_left)
        if optimistic <= best_obj + 1e-9:
            nodes_pruned_bound += 1
            return

        if idx == n:
            if rel_used >= reliability_min and curr_obj > best_obj + 1e-9:
                best_obj = curr_obj
                best_sel_idx = set(selected_idx)
            return

        # Branch include first for faster incumbent.
        can_include = True

        if any(d not in selected_idx for d in dep_idx[idx]):
            can_include = False

        if can_include:
            for c in conflict_idx[idx]:
                if c in selected_idx:
                    can_include = False
                    break

        if can_include:
            selected_idx.add(idx)
            dfs(
                idx + 1,
                capex_used + capex[idx],
                opex_used + opex[idx],
                count_used + 1,
                rel_used + rel[idx],
                curr_obj + obj[idx],
            )
            selected_idx.remove(idx)

        dfs(idx + 1, capex_used, opex_used, count_used, rel_used, curr_obj)

    dfs(
        idx=0,
        capex_used=capex_used_base,
        opex_used=opex_used_base,
        count_used=count_base,
        rel_used=rel_base,
        curr_obj=float(sum(obj_map.get(i, 0.0) for i in forced_selected)),
    )

    feasible_found = np.isfinite(best_obj)
    selected_candidates = {ordered_ids[i] for i in best_sel_idx} if feasible_found else set()
    selected_ids = set(forced_selected) | selected_candidates

    if not feasible_found:
        status = "infeasible_no_feasible_solution"
        optimality_degree = "no-feasible-formulation"
        formulation_note = "No se encontró solución factible bajo restricciones activas y cobertura reliability requerida."
    elif timed_out:
        status = "time_limited_best_feasible"
        optimality_degree = "semi_formal_approximation"
        formulation_note = "Búsqueda time-limited: mejor solución factible conocida, sin prueba completa de optimalidad."
    else:
        status = "solved_exact"
        optimality_degree = "exact_for_formulation"
        formulation_note = "Óptimo exacto para formulación binaria con branch-and-bound en horizonte actual."

    meta = {
        "status": status,
        "optimality_degree": optimality_degree,
        "solver_family": "branch_and_bound_binary_selection",
        "time_limit_sec": time_limit_sec,
        "runtime_sec": perf_counter() - start,
        "nodes_visited": nodes_visited,
        "nodes_pruned_bound": nodes_pruned_bound,
        "nodes_pruned_infeasible": nodes_pruned_infeasible,
        "timed_out": int(timed_out),
        "objective_best": float(best_obj) if feasible_found else float("nan"),
        "forced_selected_count": len(forced_selected),
        "forced_dropped_count": len(forced_dropped),
        "preblocked_count": len(preblocked),
        "cycle_blocked_count": len(cycle_ids),
        "reliability_min_required": int(reliability_min),
        "reliability_selected": int(len(selected_ids & reliability_ids)),
        "feasible_solution_found": int(feasible_found),
        "formulation_note": formulation_note,
        "preblocked": preblocked,
        "forced_dropped": list(forced_dropped),
        "forced_selected": list(forced_selected),
    }
    return selected_ids, meta


def _portfolio_constraint_reason(
    row: pd.Series,
    selected_ids: set[str],
    dep_map: dict[str, list[str]],
    conflict_map: dict[str, set[str]],
    meta: dict[str, object],
    capex_spare: float,
    opex_spare: float,
) -> str:
    ini = str(row["iniciativa_id"])
    preblocked = meta.get("preblocked", {}) if isinstance(meta.get("preblocked", {}), dict) else {}

    if ini in selected_ids:
        return "seleccionada_modelo_optimizacion"
    if ini in preblocked:
        return str(preblocked[ini])

    deps = dep_map.get(ini, [])
    if deps and not all(d in selected_ids for d in deps):
        return "dependencia_no_satisfecha"

    if any(c in selected_ids for c in conflict_map.get(ini, set())):
        return "conflicto_con_cartera"

    if float(row["capex_estimado"]) > capex_spare + 1e-6 and float(row["implementation_opex"]) > opex_spare + 1e-6:
        return "presion_capex_y_opex"
    if float(row["capex_estimado"]) > capex_spare + 1e-6:
        return "presion_capex"
    if float(row["implementation_opex"]) > opex_spare + 1e-6:
        return "presion_opex"
    return "dominada_por_funcion_objetivo"


def _assign_waves(final: pd.DataFrame) -> tuple[pd.Series, pd.DataFrame]:
    wave = pd.Series(["BACKLOG"] * len(final), index=final.index)
    chosen = final[final["selected_portfolio_flag"] == 1].sort_values(
        ["portfolio_objective_score", "improvement_priority_index"],
        ascending=False,
    )

    wave1_count = 0
    wave2_count = 0
    explanation_rows: list[dict[str, object]] = []

    for idx, row in chosen.iterrows():
        if wave1_count < MAX_WAVE1 and row["payback_months"] <= 24 and row["implementation_burden"] <= 75 and row["npv_risk_adjusted"] > 0:
            wave[idx] = "OLA_1"
            wave1_count += 1
            rule = "payback<=24 & burden<=75 & npv>0"
        elif wave2_count < MAX_WAVE2:
            wave[idx] = "OLA_2"
            wave2_count += 1
            rule = "capacidad_wave2_disponible"
        else:
            wave[idx] = "OLA_3"
            rule = "ola3_por_capacidad"

        explanation_rows.append(
            {
                "iniciativa_id": row["iniciativa_id"],
                "portfolio_wave": wave[idx],
                "wave_rule": rule,
                "payback_months": float(row["payback_months"]),
                "implementation_burden": float(row["implementation_burden"]),
                "npv_risk_adjusted": float(row["npv_risk_adjusted"]),
                "portfolio_objective_score": float(row["portfolio_objective_score"]),
            }
        )

    return wave, pd.DataFrame(explanation_rows)


def _run_selection_model(
    df: pd.DataFrame,
    dependencies: pd.DataFrame,
    conflicts: pd.DataFrame,
    capex_budget: float,
    opex_budget: float,
    max_initiatives: int,
    time_limit_sec: float,
) -> tuple[set[str], dict[str, object]]:
    dep_map = _build_dependency_map(dependencies)
    conflict_map = _build_conflict_map(conflicts)

    forced_ids = set(
        df[df["initiative_class"].isin(["mandatory", "compliance-driven"])]["iniciativa_id"].astype(str).tolist()
    )
    reliability_ids = set(df[df["initiative_class"] == "reliability-protection"]["iniciativa_id"].astype(str).tolist())
    reliability_min = int(math.ceil(0.60 * len(reliability_ids))) if len(reliability_ids) > 0 else 0

    selected_ids, meta = _run_branch_and_bound(
        df=df,
        dep_map=dep_map,
        conflict_map=conflict_map,
        forced_ids=forced_ids,
        reliability_ids=reliability_ids,
        reliability_min=reliability_min,
        capex_budget=capex_budget,
        opex_budget=opex_budget,
        max_initiatives=max_initiatives,
        time_limit_sec=time_limit_sec,
    )
    return selected_ids, meta


def _objective_total(df: pd.DataFrame, selected_ids: set[str]) -> float:
    return float(df[df["iniciativa_id"].isin(selected_ids)]["objective_value_eur"].sum())


def run_investment_prioritization() -> dict[str, pd.DataFrame]:
    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    base = pd.read_csv(DATA_PROCESSED_DIR / "opportunity_priority_scores.csv")
    scenario = pd.read_csv(DATA_PROCESSED_DIR / "scenario_table.csv")
    dependencies = pd.read_csv(DATA_PROCESSED_DIR / "scenario_dependencies.csv")
    conflicts = pd.read_csv(DATA_PROCESSED_DIR / "scenario_conflicts.csv")

    by_macro = (
        scenario.groupby(["iniciativa_id", "macro_scenario"], as_index=False)
        .agg(
            net_captured_value=("net_captured_value", "mean"),
            downside_adjusted_annual=("downside_adjusted_annual", "mean"),
            discounted_value=("discounted_value", "mean"),
            downside_adjusted_value=("downside_adjusted_value", "mean"),
            payback_meses=("payback_meses", "mean"),
            cost_of_delay_12m=("cost_of_delay_12m", "mean"),
            screening_var_95_npv=("screening_var_95_npv", "mean"),
        )
    )

    base_scenario = by_macro[by_macro["macro_scenario"].str.lower() == "base"].copy()
    if base_scenario.empty:
        base_scenario = by_macro.copy()
    base_scenario = base_scenario.rename(
        columns={
            "net_captured_value": "net_captured_value_base_scenario",
            "downside_adjusted_annual": "downside_adjusted_annual_base_scenario",
            "discounted_value": "discounted_value_base_scenario",
            "downside_adjusted_value": "downside_adjusted_value_base_scenario",
            "payback_meses": "payback_meses_base_scenario",
            "cost_of_delay_12m": "cost_of_delay_12m_base_scenario",
            "screening_var_95_npv": "screening_var_95_npv_base_scenario",
        }
    )[
        [
            "iniciativa_id",
            "net_captured_value_base_scenario",
            "downside_adjusted_annual_base_scenario",
            "discounted_value_base_scenario",
            "downside_adjusted_value_base_scenario",
            "payback_meses_base_scenario",
            "cost_of_delay_12m_base_scenario",
            "screening_var_95_npv_base_scenario",
        ]
    ]

    robust = scenario.groupby("iniciativa_id", as_index=False).agg(
        gross_technical_value_base=("gross_technical_value", "mean"),
        avoided_loss_base=("avoided_loss", "mean"),
        net_operational_value_base=("net_operational_value", "mean"),
        net_captured_value_base=("net_captured_value", "mean"),
        downside_adjusted_annual_base=("downside_adjusted_annual", "mean"),
        discounted_value_mean=("discounted_value", "mean"),
        discounted_value_min=("discounted_value", "min"),
        downside_adjusted_value_mean=("downside_adjusted_value", "mean"),
        downside_adjusted_value_min=("downside_adjusted_value", "min"),
        npv_base=("npv_base", "mean"),
        npv_risk_adjusted=("npv_risk_adjusted", "mean"),
        payback_meses=("payback_meses", "mean"),
        irr_proxy_pct=("irr_proxy_pct", "mean"),
        screening_irr_pct=("screening_irr_pct", "mean"),
        formal_irr_candidate_pct=("formal_irr_candidate_pct", "mean"),
        formal_irr_candidate_flag=("formal_irr_candidate_flag", "max"),
        value_at_risk_95=("value_at_risk_95", "mean"),
        screening_var_95_npv=("screening_var_95_npv", "mean"),
        robust_risk_metric_candidate_npv=("robust_risk_metric_candidate_npv", "mean"),
        capital_efficiency=("capital_efficiency", "mean"),
        cost_of_delay_12m=("cost_of_delay_12m", "mean"),
        implementation_opex=("implementation_opex", "mean"),
        implementation_burden=("implementation_burden", "mean"),
        strategic_relevance_score=("strategic_relevance_score", "mean"),
        success_probability=("success_probability", "mean"),
        reduccion_emisiones_pct=("reduccion_emisiones_pct", "mean"),
        financial_maturity_stage=("financial_maturity_stage", lambda s: s.mode().iloc[0] if not s.mode().empty else "screening"),
        committee_wording_status=("committee_wording_status", lambda s: s.mode().iloc[0] if not s.mode().empty else "not_committee_grade_proxy"),
    )

    base = base.merge(robust, on="iniciativa_id", how="left")
    base = base.merge(base_scenario, on="iniciativa_id", how="left")

    base["annual_saving_proxy"] = (
        base["net_captured_value_base_scenario"]
        .fillna(base["net_captured_value_base"])
        .fillna(base["annual_saving_proxy"])
    )
    base["downside_adjusted_annual"] = (
        base["downside_adjusted_annual_base_scenario"]
        .fillna(base["downside_adjusted_annual_base"])
        .fillna(base["annual_saving_proxy"] * 0.75)
    )
    base["npv_risk_adjusted"] = base["discounted_value_base_scenario"].fillna(base["npv_risk_adjusted"])
    base["downside_adjusted_value"] = base["downside_adjusted_value_base_scenario"].fillna(base["downside_adjusted_value_mean"])
    base["payback_months"] = base["payback_meses_base_scenario"].fillna(base["payback_meses"]).fillna(base["payback_months"])

    base["gross_technical_value"] = base["gross_technical_value_base"].fillna(base["annual_saving_proxy"])
    base["avoided_loss"] = base["avoided_loss_base"].fillna(0.0)
    base["net_captured_value"] = base["annual_saving_proxy"]
    base["discounted_value"] = base["npv_risk_adjusted"]
    base["cost_of_delay_12m"] = base["cost_of_delay_12m_base_scenario"].fillna(base["cost_of_delay_12m"])

    anchored_components = _compute_anchored_priority_components(base)
    legacy_components = _compute_legacy_priority_components(base)
    for component_col in anchored_components.columns:
        base[component_col] = anchored_components[component_col]

    base["improvement_priority_index"] = (
        0.20 * base["energy_saving_score"]
        + 0.18 * base["operational_impact_score"]
        + 0.10 * base["emissions_reduction_score"]
        + 0.14 * base["implementation_feasibility_score"]
        + 0.12 * base["payback_score"]
        + 0.10 * base["strategic_priority_score"]
        + 0.10 * base["financial_resilience_score"]
        + 0.06 * base["uncertainty_index"]
    ).clip(0, 100)

    downside_norm = anchored_score(base["downside_adjusted_value"].fillna(0), low=0.0, high=1_900_000.0)
    capital_eff_norm = anchored_score(base["capital_efficiency"].fillna(0), low=0.0, high=110.0)
    base["portfolio_objective_score"] = (
        0.55 * downside_norm
        + 0.20 * capital_eff_norm
        + 0.15 * base["financial_resilience_score"]
        + 0.10 * base["strategic_priority_score"]
    ).clip(0, 100)
    base["score_scaling_method"] = SCORE_SCALING_METHOD
    base["score_scale_version"] = SCORE_SCALE_VERSION
    base["component_score_comparability_tag"] = COMPARABILITY_ABSOLUTE
    base["improvement_priority_comparability_tag"] = COMPARABILITY_CONTEXTUAL
    base["portfolio_objective_comparability_tag"] = COMPARABILITY_CONTEXTUAL

    legacy_improvement_priority = (
        0.20 * legacy_components["energy_saving_score"]
        + 0.18 * legacy_components["operational_impact_score"]
        + 0.10 * legacy_components["emissions_reduction_score"]
        + 0.14 * legacy_components["implementation_feasibility_score"]
        + 0.12 * legacy_components["payback_score"]
        + 0.10 * legacy_components["strategic_priority_score"]
        + 0.10 * legacy_components["financial_resilience_score"]
        + 0.06 * legacy_components["uncertainty_index"]
    ).clip(0, 100)

    legacy_portfolio_objective = (
        0.55 * legacy_local_minmax_score(base["downside_adjusted_value"].fillna(0))
        + 0.20 * legacy_local_minmax_score(base["capital_efficiency"].fillna(0))
        + 0.15 * legacy_components["financial_resilience_score"]
        + 0.10 * legacy_components["strategic_priority_score"]
    ).clip(0, 100)

    base["initiative_class"] = base.apply(_classify_initiative, axis=1)
    base["mandatory_flag"] = (base["initiative_class"] == "mandatory").astype(int)
    base["compliance_flag"] = (base["initiative_class"] == "compliance-driven").astype(int)
    base["reliability_flag"] = (base["initiative_class"] == "reliability-protection").astype(int)

    base["financial_use_level"] = base["financial_maturity_stage"].apply(_financial_stage_label)
    base["committee_claim_allowed_flag"] = (base["financial_maturity_stage"] == "business_case_candidate").astype(int)
    base["committee_claim_caveat"] = np.where(
        base["committee_claim_allowed_flag"] == 1,
        "Candidato para comité con caveat: requiere business case corporativo auditado.",
        "No committee-grade: usar solo para screening/pre-feasibility.",
    )

    base["objective_value_eur"] = _compute_objective_value(base)

    selected_ids, opt_meta = _run_selection_model(
        df=base,
        dependencies=dependencies,
        conflicts=conflicts,
        capex_budget=CAPEX_BUDGET,
        opex_budget=OPEX_BUDGET,
        max_initiatives=MAX_INITIATIVES,
        time_limit_sec=OPTIMIZER_TIME_LIMIT_SEC,
    )

    final = base.copy()
    final["selected_portfolio_flag"] = final["iniciativa_id"].astype(str).isin(selected_ids).astype(int)

    selected_capex = float(final.loc[final["selected_portfolio_flag"] == 1, "capex_estimado"].sum())
    selected_opex = float(final.loc[final["selected_portfolio_flag"] == 1, "implementation_opex"].sum())
    capex_spare = CAPEX_BUDGET - selected_capex
    opex_spare = OPEX_BUDGET - selected_opex

    dep_map = _build_dependency_map(dependencies)
    conflict_map = _build_conflict_map(conflicts)

    final["portfolio_constraint_reason"] = final.apply(
        lambda r: _portfolio_constraint_reason(
            row=r,
            selected_ids=selected_ids,
            dep_map=dep_map,
            conflict_map=conflict_map,
            meta=opt_meta,
            capex_spare=capex_spare,
            opex_spare=opex_spare,
        ),
        axis=1,
    )

    final["portfolio_wave"], wave_explanation = _assign_waves(final)

    final["portfolio_npv_contribution"] = np.where(
        final["selected_portfolio_flag"] == 1,
        final["npv_risk_adjusted"],
        0.0,
    )
    final["portfolio_downside_value_contribution"] = np.where(
        final["selected_portfolio_flag"] == 1,
        final["downside_adjusted_value"],
        0.0,
    )

    final["selection_variable_x"] = final["selected_portfolio_flag"]
    final["optimizer_status"] = str(opt_meta.get("status", "unknown"))
    final["optimality_degree"] = str(opt_meta.get("optimality_degree", "unknown"))
    final["selection_model"] = str(opt_meta.get("solver_family", "unknown"))

    final = final.sort_values(["portfolio_objective_score", "improvement_priority_index"], ascending=False).reset_index(drop=True)

    final["initiative_tier"] = final["improvement_priority_index"].apply(_tier_from_score)
    final["recommended_sequence"] = [f"OLA_{idx:02d}" for idx in range(1, len(final) + 1)]
    final["main_business_case"] = final.apply(_business_case, axis=1)
    final["decision_rule"] = final.apply(_decision_bucket, axis=1)

    # Sensibilidad de score (ranking) para robustez de priorización.
    scenarios_weights = [
        {"name": "base", "w": [0.20, 0.18, 0.10, 0.14, 0.12, 0.10, 0.10, 0.06]},
        {"name": "energia_alta", "w": [0.28, 0.15, 0.12, 0.12, 0.12, 0.07, 0.10, 0.04]},
        {"name": "operacion_critica", "w": [0.15, 0.28, 0.08, 0.17, 0.10, 0.10, 0.08, 0.04]},
    ]

    sensitivity_rows: list[dict[str, str | float | int]] = []
    for scenario_w in scenarios_weights:
        w1, w2, w3, w4, w5, w6, w7, w8 = scenario_w["w"]
        score = (
            w1 * final["energy_saving_score"]
            + w2 * final["operational_impact_score"]
            + w3 * final["emissions_reduction_score"]
            + w4 * final["implementation_feasibility_score"]
            + w5 * final["payback_score"]
            + w6 * final["strategic_priority_score"]
            + w7 * final["financial_resilience_score"]
            + w8 * final["uncertainty_index"]
        )
        tmp = final[["iniciativa_id"]].copy()
        tmp["scenario"] = scenario_w["name"]
        tmp["score"] = score
        tmp = tmp.sort_values("score", ascending=False).reset_index(drop=True)
        tmp["ranking"] = tmp.index + 1
        sensitivity_rows.extend(tmp.to_dict("records"))

    sensitivity = pd.DataFrame(sensitivity_rows)

    # Estabilidad: before/after local vs anchored (inter-release comparability hardening).
    priority_raw = base["net_captured_value_base"].fillna(base["annual_saving_proxy"])
    operational_raw = base["expected_oee_gain"] * 0.6 + base["operational_urgency_score"] * 0.4
    emissions_raw = base["expected_emissions_reduction"] * 0.6 + base["reduccion_emisiones_pct"].fillna(0.0) * 0.4
    feasibility_raw = (
        base["implementation_complexity_score"] * 0.45
        + base["execution_risk_score"] * 0.25
        + base["implementation_burden"].fillna(0.0) * 0.30
    )
    strategic_raw = base["strategic_alignment_score"] * 0.65 + base["strategic_relevance_score"].fillna(0.0) * 0.35
    fin_res_raw = (
        base["downside_adjusted_value"].fillna(0.0)
        - 0.35 * base["screening_var_95_npv"].fillna(0.0)
        + 0.20 * base["capital_efficiency"].fillna(0.0)
    )
    uncertainty_raw = base["screening_var_95_npv"].fillna(0.0)

    stability_rows = [
        {
            "score_name": "energy_saving_score",
            "legacy_outlier_median_abs_shift": outlier_impact_median_abs_delta(
                priority_raw,
                lambda s: legacy_local_minmax_score(s),
            ),
            "anchored_outlier_median_abs_shift": outlier_impact_median_abs_delta(
                priority_raw,
                lambda s: anchored_score(s, low=150_000.0, high=1_100_000.0),
            ),
            "spearman_anchored_vs_legacy": spearman_rank_corr(base["energy_saving_score"], legacy_components["energy_saving_score"]),
            "comparability_tag": COMPARABILITY_ABSOLUTE,
        },
        {
            "score_name": "operational_impact_score",
            "legacy_outlier_median_abs_shift": outlier_impact_median_abs_delta(
                operational_raw,
                lambda s: legacy_local_minmax_score(s),
            ),
            "anchored_outlier_median_abs_shift": outlier_impact_median_abs_delta(
                operational_raw,
                lambda s: anchored_score(s, low=20.0, high=85.0),
            ),
            "spearman_anchored_vs_legacy": spearman_rank_corr(base["operational_impact_score"], legacy_components["operational_impact_score"]),
            "comparability_tag": COMPARABILITY_ABSOLUTE,
        },
        {
            "score_name": "emissions_reduction_score",
            "legacy_outlier_median_abs_shift": outlier_impact_median_abs_delta(
                emissions_raw,
                lambda s: legacy_local_minmax_score(s),
            ),
            "anchored_outlier_median_abs_shift": outlier_impact_median_abs_delta(
                emissions_raw,
                lambda s: anchored_score(s, low=5.0, high=130.0),
            ),
            "spearman_anchored_vs_legacy": spearman_rank_corr(base["emissions_reduction_score"], legacy_components["emissions_reduction_score"]),
            "comparability_tag": COMPARABILITY_ABSOLUTE,
        },
        {
            "score_name": "implementation_feasibility_score",
            "legacy_outlier_median_abs_shift": outlier_impact_median_abs_delta(
                feasibility_raw,
                lambda s: legacy_local_minmax_score(s, invert=True),
            ),
            "anchored_outlier_median_abs_shift": outlier_impact_median_abs_delta(
                feasibility_raw,
                lambda s: anchored_score(s, low=20.0, high=92.0, invert=True),
            ),
            "spearman_anchored_vs_legacy": spearman_rank_corr(base["implementation_feasibility_score"], legacy_components["implementation_feasibility_score"]),
            "comparability_tag": COMPARABILITY_ABSOLUTE,
        },
        {
            "score_name": "payback_score",
            "legacy_outlier_median_abs_shift": outlier_impact_median_abs_delta(
                base["payback_months"],
                lambda s: legacy_local_minmax_score(s, invert=True),
            ),
            "anchored_outlier_median_abs_shift": outlier_impact_median_abs_delta(
                base["payback_months"],
                lambda s: anchored_score(s, low=6.0, high=36.0, invert=True),
            ),
            "spearman_anchored_vs_legacy": spearman_rank_corr(base["payback_score"], legacy_components["payback_score"]),
            "comparability_tag": COMPARABILITY_ABSOLUTE,
        },
        {
            "score_name": "strategic_priority_score",
            "legacy_outlier_median_abs_shift": outlier_impact_median_abs_delta(
                strategic_raw,
                lambda s: legacy_local_minmax_score(s),
            ),
            "anchored_outlier_median_abs_shift": outlier_impact_median_abs_delta(
                strategic_raw,
                lambda s: anchored_score(s, low=30.0, high=95.0),
            ),
            "spearman_anchored_vs_legacy": spearman_rank_corr(base["strategic_priority_score"], legacy_components["strategic_priority_score"]),
            "comparability_tag": COMPARABILITY_ABSOLUTE,
        },
        {
            "score_name": "financial_resilience_score",
            "legacy_outlier_median_abs_shift": outlier_impact_median_abs_delta(
                fin_res_raw,
                lambda s: legacy_local_minmax_score(s),
            ),
            "anchored_outlier_median_abs_shift": outlier_impact_median_abs_delta(
                fin_res_raw,
                lambda s: anchored_score(s, low=-350_000.0, high=1_600_000.0),
            ),
            "spearman_anchored_vs_legacy": spearman_rank_corr(base["financial_resilience_score"], legacy_components["financial_resilience_score"]),
            "comparability_tag": COMPARABILITY_CONTEXTUAL,
        },
        {
            "score_name": "uncertainty_index",
            "legacy_outlier_median_abs_shift": outlier_impact_median_abs_delta(
                uncertainty_raw,
                lambda s: legacy_local_minmax_score(s, invert=True),
            ),
            "anchored_outlier_median_abs_shift": outlier_impact_median_abs_delta(
                uncertainty_raw,
                lambda s: anchored_score(s, low=300_000.0, high=1_300_000.0, invert=True),
            ),
            "spearman_anchored_vs_legacy": spearman_rank_corr(base["uncertainty_index"], legacy_components["uncertainty_index"]),
            "comparability_tag": COMPARABILITY_CONTEXTUAL,
        },
    ]

    # Robustez de ranking frente a outlier en señal económica dominante.
    base_outlier = base.copy()
    outlier_idx = priority_raw.idxmax()
    base_outlier.loc[outlier_idx, "net_captured_value_base"] = base_outlier.loc[outlier_idx, "net_captured_value_base"] * 3.0
    anchored_outlier_components = _compute_anchored_priority_components(base_outlier)
    legacy_outlier_components = _compute_legacy_priority_components(base_outlier)
    anchored_priority_outlier = (
        0.20 * anchored_outlier_components["energy_saving_score"]
        + 0.18 * anchored_outlier_components["operational_impact_score"]
        + 0.10 * anchored_outlier_components["emissions_reduction_score"]
        + 0.14 * anchored_outlier_components["implementation_feasibility_score"]
        + 0.12 * anchored_outlier_components["payback_score"]
        + 0.10 * anchored_outlier_components["strategic_priority_score"]
        + 0.10 * anchored_outlier_components["financial_resilience_score"]
        + 0.06 * anchored_outlier_components["uncertainty_index"]
    ).clip(0, 100)
    legacy_priority_outlier = (
        0.20 * legacy_outlier_components["energy_saving_score"]
        + 0.18 * legacy_outlier_components["operational_impact_score"]
        + 0.10 * legacy_outlier_components["emissions_reduction_score"]
        + 0.14 * legacy_outlier_components["implementation_feasibility_score"]
        + 0.12 * legacy_outlier_components["payback_score"]
        + 0.10 * legacy_outlier_components["strategic_priority_score"]
        + 0.10 * legacy_outlier_components["financial_resilience_score"]
        + 0.06 * legacy_outlier_components["uncertainty_index"]
    ).clip(0, 100)

    ranking_robustness_before_after = pd.DataFrame(
        [
            {
                "score_name": "improvement_priority_index",
                "legacy_top10_overlap_after_outlier": topk_overlap(
                    legacy_improvement_priority,
                    legacy_priority_outlier,
                    base["iniciativa_id"],
                    topk=10,
                ),
                "anchored_top10_overlap_after_outlier": topk_overlap(
                    base["improvement_priority_index"],
                    anchored_priority_outlier,
                    base["iniciativa_id"],
                    topk=10,
                ),
                "legacy_vs_anchored_spearman": spearman_rank_corr(
                    legacy_improvement_priority,
                    base["improvement_priority_index"],
                ),
            }
        ]
    )

    selected = final[final["selected_portfolio_flag"] == 1].copy()

    portfolio_summary = pd.DataFrame(
        [
            {
                "capex_budget": CAPEX_BUDGET,
                "opex_budget": OPEX_BUDGET,
                "capex_selected": selected_capex,
                "opex_selected": selected_opex,
                "initiatives_selected": int(selected["selected_portfolio_flag"].sum()),
                "npv_risk_adjusted_portfolio": selected["npv_risk_adjusted"].sum(),
                "annual_saving_risk_adjusted_portfolio": selected["annual_saving_proxy"].sum(),
                "weighted_payback_portfolio": np.average(
                    selected["payback_months"],
                    weights=selected["improvement_priority_index"],
                )
                if len(selected) > 0
                else 0.0,
                "gross_technical_value_portfolio": selected["gross_technical_value"].sum(),
                "net_captured_value_portfolio": selected["net_captured_value"].sum(),
                "discounted_value_portfolio": selected["discounted_value"].sum(),
                "downside_adjusted_value_portfolio": selected["downside_adjusted_value"].sum(),
                "cost_of_delay_12m_portfolio": selected["cost_of_delay_12m"].sum(),
                "screening_var_95_portfolio": selected["value_at_risk_95"].sum(),
                "screening_var_95_npv_portfolio": selected["screening_var_95_npv"].sum(),
                "capital_efficiency_weighted": np.average(
                    selected["capital_efficiency"],
                    weights=selected["capex_estimado"].replace(0, 1),
                )
                if len(selected) > 0
                else 0.0,
                "formal_irr_candidates_selected": int((selected["formal_irr_candidate_flag"] > 0).sum()),
                "screening_count_selected": int((selected["financial_maturity_stage"] == "screening").sum()),
                "pre_feasibility_count_selected": int((selected["financial_maturity_stage"] == "pre_feasibility").sum()),
                "business_case_candidate_count_selected": int((selected["financial_maturity_stage"] == "business_case_candidate").sum()),
                "financial_readiness_label": "screening/pre-feasibility",
                "selection_model": str(opt_meta.get("solver_family", "unknown")),
                "optimization_status": str(opt_meta.get("status", "unknown")),
                "optimality_degree": str(opt_meta.get("optimality_degree", "unknown")),
                "optimization_runtime_sec": float(opt_meta.get("runtime_sec", 0.0)),
                "optimization_nodes_visited": int(opt_meta.get("nodes_visited", 0)),
                "optimization_time_limited_flag": int(opt_meta.get("timed_out", 0)),
                "objective_value_eur_portfolio": float(_objective_total(final, selected_ids)),
            }
        ]
    )

    # Analítica de presión de restricciones y valor desbloqueable.
    def _simulate_relaxed(capex_b: float, opex_b: float, cap_n: int) -> tuple[set[str], dict[str, object], float]:
        sim_selected, sim_meta = _run_selection_model(
            df=base,
            dependencies=dependencies,
            conflicts=conflicts,
            capex_budget=capex_b,
            opex_budget=opex_b,
            max_initiatives=cap_n,
            time_limit_sec=max(3.0, OPTIMIZER_TIME_LIMIT_SEC * 0.75),
        )
        return sim_selected, sim_meta, _objective_total(base, sim_selected)

    base_objective = float(_objective_total(final, selected_ids))
    _, _, obj_capex_relaxed = _simulate_relaxed(CAPEX_BUDGET * 1.10, OPEX_BUDGET, MAX_INITIATIVES)
    _, _, obj_opex_relaxed = _simulate_relaxed(CAPEX_BUDGET, OPEX_BUDGET * 1.10, MAX_INITIATIVES)
    _, _, obj_capacity_relaxed = _simulate_relaxed(CAPEX_BUDGET, OPEX_BUDGET, MAX_INITIATIVES + 4)
    _, _, obj_all_relaxed = _simulate_relaxed(CAPEX_BUDGET * 1.10, OPEX_BUDGET * 1.10, MAX_INITIATIVES + 4)

    def _non_negative_delta(value: float) -> float:
        delta = max(0.0, value)
        return 0.0 if delta < 1e-3 else float(delta)

    unlocked = pd.DataFrame(
        [
            {
                "constraint_relaxation": "capex_plus_10pct",
                "baseline_objective_eur": base_objective,
                "relaxed_objective_eur": obj_capex_relaxed,
                "unlocked_value_eur": _non_negative_delta(obj_capex_relaxed - base_objective),
            },
            {
                "constraint_relaxation": "opex_plus_10pct",
                "baseline_objective_eur": base_objective,
                "relaxed_objective_eur": obj_opex_relaxed,
                "unlocked_value_eur": _non_negative_delta(obj_opex_relaxed - base_objective),
            },
            {
                "constraint_relaxation": "capacity_plus_4",
                "baseline_objective_eur": base_objective,
                "relaxed_objective_eur": obj_capacity_relaxed,
                "unlocked_value_eur": _non_negative_delta(obj_capacity_relaxed - base_objective),
            },
            {
                "constraint_relaxation": "all_plus",
                "baseline_objective_eur": base_objective,
                "relaxed_objective_eur": obj_all_relaxed,
                "unlocked_value_eur": _non_negative_delta(obj_all_relaxed - base_objective),
            },
        ]
    )

    selected_reliability = int((selected["initiative_class"] == "reliability-protection").sum())
    total_reliability = int((final["initiative_class"] == "reliability-protection").sum())
    reliability_min = int(math.ceil(0.60 * total_reliability)) if total_reliability > 0 else 0

    constraint_pressure = pd.DataFrame(
        [
            {
                "constraint_name": "capex_budget",
                "used": selected_capex,
                "limit": CAPEX_BUDGET,
                "utilization_pct": 100 * selected_capex / CAPEX_BUDGET,
                "active_flag": int(selected_capex >= 0.98 * CAPEX_BUDGET),
                "marginal_unlocked_value_eur": float(unlocked.loc[unlocked["constraint_relaxation"] == "capex_plus_10pct", "unlocked_value_eur"].iloc[0]),
            },
            {
                "constraint_name": "opex_budget",
                "used": selected_opex,
                "limit": OPEX_BUDGET,
                "utilization_pct": 100 * selected_opex / OPEX_BUDGET,
                "active_flag": int(selected_opex >= 0.98 * OPEX_BUDGET),
                "marginal_unlocked_value_eur": float(unlocked.loc[unlocked["constraint_relaxation"] == "opex_plus_10pct", "unlocked_value_eur"].iloc[0]),
            },
            {
                "constraint_name": "selection_capacity",
                "used": int(len(selected)),
                "limit": MAX_INITIATIVES,
                "utilization_pct": 100 * len(selected) / MAX_INITIATIVES,
                "active_flag": int(len(selected) >= MAX_INITIATIVES),
                "marginal_unlocked_value_eur": float(unlocked.loc[unlocked["constraint_relaxation"] == "capacity_plus_4", "unlocked_value_eur"].iloc[0]),
            },
            {
                "constraint_name": "reliability_min_coverage",
                "used": selected_reliability,
                "limit": reliability_min,
                "utilization_pct": 100 * selected_reliability / max(reliability_min, 1),
                "active_flag": int(selected_reliability <= reliability_min and reliability_min > 0),
                "marginal_unlocked_value_eur": 0.0,
            },
        ]
    )

    # Tabla detallada selected vs excluded.
    selected_vs_excluded = final[
        [
            "iniciativa_id",
            "initiative_class",
            "selected_portfolio_flag",
            "selection_variable_x",
            "portfolio_constraint_reason",
            "objective_value_eur",
            "downside_adjusted_value",
            "capex_estimado",
            "implementation_opex",
            "payback_months",
            "portfolio_wave",
        ]
    ].copy()

    # Before/after de lógica de selección (documentable en entrevista/comité).
    before_after = pd.DataFrame(
        [
            {
                "dimension": "selection_method",
                "before": "greedy ranking con checks secuenciales",
                "after": "branch-and-bound binario con restricciones explícitas",
                "impact": "mayor trazabilidad y defensabilidad de selección",
            },
            {
                "dimension": "constraint_handling",
                "before": "validación inline durante recorrido",
                "after": "formulación explícita CAPEX/OPEX/capacidad/dependencias/conflictos",
                "impact": "mejor control de factibilidad y auditoría",
            },
            {
                "dimension": "optimality_statement",
                "before": "no cuantificada",
                "after": str(opt_meta.get("optimality_degree", "unknown")),
                "impact": "claim de optimalidad acotado y verificable",
            },
            {
                "dimension": "constraint_pressure",
                "before": "no disponible",
                "after": "constraint_pressure_summary + unlocked_value_analysis",
                "impact": "visibilidad de valor perdido por restricción",
            },
        ]
    )

    # Guardado principal.
    keep_cols = [
        "iniciativa_id",
        "planta_id",
        "linea_id",
        "categoria_iniciativa",
        "tipo_iniciativa",
        "quick_win_flag",
        "initiative_class",
        "mandatory_flag",
        "compliance_flag",
        "reliability_flag",
        "financial_maturity_stage",
        "financial_use_level",
        "committee_claim_allowed_flag",
        "committee_claim_caveat",
        "capex_estimado",
        "gross_technical_value",
        "avoided_loss",
        "net_captured_value",
        "downside_adjusted_annual",
        "annual_saving_proxy",
        "discounted_value",
        "downside_adjusted_value",
        "cost_of_delay_12m",
        "value_at_risk_95",
        "screening_var_95_npv",
        "robust_risk_metric_candidate_npv",
        "npv_base",
        "npv_risk_adjusted",
        "irr_proxy_pct",
        "screening_irr_pct",
        "formal_irr_candidate_pct",
        "formal_irr_candidate_flag",
        "payback_months",
        "implementation_burden",
        "strategic_relevance_score",
        "capital_efficiency",
        "energy_saving_score",
        "operational_impact_score",
        "emissions_reduction_score",
        "implementation_feasibility_score",
        "payback_score",
        "strategic_priority_score",
        "financial_resilience_score",
        "uncertainty_index",
        "portfolio_objective_score",
        "score_scaling_method",
        "score_scale_version",
        "component_score_comparability_tag",
        "improvement_priority_comparability_tag",
        "portfolio_objective_comparability_tag",
        "objective_value_eur",
        "improvement_priority_index",
        "initiative_tier",
        "selected_portfolio_flag",
        "selection_variable_x",
        "selection_model",
        "optimizer_status",
        "optimality_degree",
        "portfolio_wave",
        "portfolio_constraint_reason",
        "portfolio_npv_contribution",
        "portfolio_downside_value_contribution",
        "recommended_sequence",
        "main_business_case",
        "decision_rule",
    ]

    final_table = final[keep_cols].copy()

    final_table.to_csv(DATA_PROCESSED_DIR / "investment_prioritization_final.csv", index=False)
    sensitivity.to_csv(DATA_PROCESSED_DIR / "investment_prioritization_sensitivity.csv", index=False)
    portfolio_summary.to_csv(DATA_PROCESSED_DIR / "portfolio_summary.csv", index=False)
    final_table.to_csv(DATA_PROCESSED_DIR / "portfolio_recommendation.csv", index=False)
    selected_vs_excluded.to_csv(DATA_PROCESSED_DIR / "portfolio_selected_vs_excluded.csv", index=False)
    unlocked.to_csv(DATA_PROCESSED_DIR / "portfolio_unlocked_value_analysis.csv", index=False)
    constraint_pressure.to_csv(DATA_PROCESSED_DIR / "portfolio_constraint_pressure_summary.csv", index=False)
    wave_explanation.to_csv(DATA_PROCESSED_DIR / "portfolio_wave_logic_explained.csv", index=False)
    before_after.to_csv(DATA_PROCESSED_DIR / "portfolio_selection_before_after.csv", index=False)
    ranking_robustness_before_after.to_csv(DATA_PROCESSED_DIR / "score_ranking_robustness_before_after.csv", index=False)

    diagnostics_stability_path = DATA_PROCESSED_DIR / "score_stability_before_after_diagnostics.csv"
    diagnostics_stability = pd.read_csv(diagnostics_stability_path) if diagnostics_stability_path.exists() else pd.DataFrame()
    invest_stability = pd.DataFrame(stability_rows)
    score_stability_before_after = pd.concat([diagnostics_stability, invest_stability], ignore_index=True)
    score_stability_before_after.to_csv(DATA_PROCESSED_DIR / "score_stability_before_after.csv", index=False)

    score_comparability_registry = pd.DataFrame(
        [
            {
                "score_name": "equipment_energy_anomaly_score",
                "previous_scaling": "local_minmax_per_run",
                "scaling_method": SCORE_SCALING_METHOD,
                "scale_version": SCORE_SCALE_VERSION,
                "comparability_tag": COMPARABILITY_ABSOLUTE,
                "comparability_policy": "comparable entre releases cuando se mantiene scale_version",
                "window_comparison_allowed": 1,
            },
            {
                "score_name": "process_deviation_risk_score",
                "previous_scaling": "local_minmax_per_run",
                "scaling_method": SCORE_SCALING_METHOD,
                "scale_version": SCORE_SCALE_VERSION,
                "comparability_tag": COMPARABILITY_ABSOLUTE,
                "comparability_policy": "comparable entre releases cuando se mantiene scale_version",
                "window_comparison_allowed": 1,
            },
            {
                "score_name": "line_criticality_score",
                "previous_scaling": "local_minmax_per_run",
                "scaling_method": SCORE_SCALING_METHOD,
                "scale_version": SCORE_SCALE_VERSION,
                "comparability_tag": COMPARABILITY_CONTEXTUAL,
                "comparability_policy": "comparable por contexto operativo similar; evitar comparar periodos con mix extremo distinto",
                "window_comparison_allowed": 1,
            },
            {
                "score_name": "opportunity_priority_score",
                "previous_scaling": "local_minmax_per_run",
                "scaling_method": SCORE_SCALING_METHOD,
                "scale_version": SCORE_SCALE_VERSION,
                "comparability_tag": COMPARABILITY_CONTEXTUAL,
                "comparability_policy": "comparable para priorización táctica; no usar como benchmark absoluto cross-site",
                "window_comparison_allowed": 1,
            },
            {
                "score_name": "improvement_priority_index",
                "previous_scaling": "mixed_local_minmax_components",
                "scaling_method": SCORE_SCALING_METHOD,
                "scale_version": SCORE_SCALE_VERSION,
                "comparability_tag": COMPARABILITY_CONTEXTUAL,
                "comparability_policy": "comparable entre releases con misma taxonomía de inputs y versionado estable",
                "window_comparison_allowed": 1,
            },
            {
                "score_name": "portfolio_objective_score",
                "previous_scaling": "mixed_local_minmax_components",
                "scaling_method": SCORE_SCALING_METHOD,
                "scale_version": SCORE_SCALE_VERSION,
                "comparability_tag": COMPARABILITY_CONTEXTUAL,
                "comparability_policy": "comparable para decisiones de cartera en mismo marco financiero y restricciones",
                "window_comparison_allowed": 1,
            },
        ]
    )
    score_comparability_registry.to_csv(DATA_PROCESSED_DIR / "score_comparability_registry.csv", index=False)
    score_scaling_audit = score_comparability_registry.copy()
    score_scaling_audit["drift_risk_before"] = "high_under_local_minmax"
    score_scaling_audit["drift_risk_after"] = "reduced_with_anchored_reference"
    score_scaling_audit.to_csv(DATA_PROCESSED_DIR / "score_scaling_audit.csv", index=False)

    optimization_solution = final[["iniciativa_id", "selection_variable_x", "objective_value_eur", "initiative_class"]].copy()
    optimization_solution["selection_variable_x"] = optimization_solution["selection_variable_x"].astype(int)
    optimization_solution.to_csv(DATA_PROCESSED_DIR / "portfolio_optimization_solution.csv", index=False)

    optimization_meta_df = pd.DataFrame(
        [
            {
                "selection_model": str(opt_meta.get("solver_family", "unknown")),
                "optimization_status": str(opt_meta.get("status", "unknown")),
                "optimality_degree": str(opt_meta.get("optimality_degree", "unknown")),
                "runtime_sec": float(opt_meta.get("runtime_sec", 0.0)),
                "time_limit_sec": float(opt_meta.get("time_limit_sec", OPTIMIZER_TIME_LIMIT_SEC)),
                "nodes_visited": int(opt_meta.get("nodes_visited", 0)),
                "nodes_pruned_bound": int(opt_meta.get("nodes_pruned_bound", 0)),
                "nodes_pruned_infeasible": int(opt_meta.get("nodes_pruned_infeasible", 0)),
                "timed_out_flag": int(opt_meta.get("timed_out", 0)),
                "feasible_solution_found": int(opt_meta.get("feasible_solution_found", 1)),
                "objective_best": float(opt_meta.get("objective_best", 0.0)),
                "formulation_note": str(opt_meta.get("formulation_note", "")),
            }
        ]
    )
    optimization_meta_df.to_csv(DATA_PROCESSED_DIR / "portfolio_optimization_metadata.csv", index=False)

    top_quick_wins = final_table[final_table["quick_win_flag"] == 1].sort_values("improvement_priority_index", ascending=False).head(8)
    top_capex = final_table[final_table["capex_estimado"] >= 700_000].sort_values("improvement_priority_index", ascending=False).head(8)
    top_quick_wins.to_csv(DATA_PROCESSED_DIR / "top_quick_wins.csv", index=False)
    top_capex.to_csv(DATA_PROCESSED_DIR / "top_capex_transformacionales.csv", index=False)

    framework_doc = [
        "# Investment Prioritization Framework",
        "",
        "## Posicionamiento de madurez financiera",
        "- Screening financiero: comparabilidad temprana de iniciativas.",
        "- Pre-feasibility: discounted value/downside/cost of delay con supuestos explícitos.",
        "- Business case candidate: requiere validación corporativa adicional (no final).",
        "",
        "## Taxonomía financiera activa",
        "- gross_technical_value",
        "- avoided_loss",
        "- net_captured_value",
        "- discounted_value",
        "- downside_adjusted_value",
        "- cost_of_delay_12m",
        "- capital_efficiency",
        "- screening_irr_pct",
        "- formal_irr_candidate_pct",
        "- screening_var_95_npv",
        "",
        "## Regla de credibilidad",
        "- No presentar `screening_irr_pct` como IRR formal de comité.",
        "- No presentar `business_case_candidate` como FID final aprobado.",
        f"- Scoring gobernado con `{SCORE_SCALING_METHOD}` y versión `{SCORE_SCALE_VERSION}`.",
        "",
        "## Motor de selección de cartera",
        "- Modelo binario branch-and-bound con restricciones explícitas.",
        "- Dependencias y exclusiones técnicas tratadas como restricciones hard.",
        "- Mandatory/compliance forzadas y cobertura mínima reliability en formulación.",
        f"- Estado de resolución actual: {str(opt_meta.get('status', 'unknown'))}.",
        f"- Grado de optimalidad declarado: {str(opt_meta.get('optimality_degree', 'unknown'))}.",
    ]
    (DOCS_DIR / "investment_prioritization_framework.md").write_text("\n".join(framework_doc), encoding="utf-8")

    return {
        "final_table": final_table,
        "sensitivity": sensitivity,
        "top_quick_wins": top_quick_wins,
        "top_capex": top_capex,
        "portfolio_summary": portfolio_summary,
        "optimization_meta": optimization_meta_df,
        "constraint_pressure": constraint_pressure,
        "unlocked": unlocked,
        "selected_vs_excluded": selected_vs_excluded,
        "wave_explanation": wave_explanation,
        "score_stability_before_after": score_stability_before_after,
        "score_ranking_robustness_before_after": ranking_robustness_before_after,
        "score_comparability_registry": score_comparability_registry,
        "score_scaling_audit": score_scaling_audit,
    }


if __name__ == "__main__":
    run_investment_prioritization()
