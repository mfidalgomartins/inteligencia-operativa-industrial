from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


KEY_FINDINGS_START = "<!-- AUTO:KEY_FINDINGS:START -->"
KEY_FINDINGS_END = "<!-- AUTO:KEY_FINDINGS:END -->"
EXEC_DECISION_START = "<!-- AUTO:EXEC_DECISION:START -->"
EXEC_DECISION_END = "<!-- AUTO:EXEC_DECISION:END -->"


def _replace_marked_block(text: str, start: str, end: str, block_lines: list[str]) -> str:
    block = "\n".join([start, *block_lines, end])
    pattern = rf"{re.escape(start)}.*?{re.escape(end)}"
    if re.search(pattern, text, flags=re.DOTALL):
        return re.sub(pattern, block, text, flags=re.DOTALL)
    return text.rstrip() + "\n\n" + block + "\n"


def _safe_row(row: pd.Series | dict[str, object]) -> dict[str, object]:
    if isinstance(row, pd.Series):
        return row.to_dict()
    return dict(row)


def sync_readme_runtime_claims(
    readme_path: Path,
    snapshot_row: pd.Series | dict[str, object],
    decision_row: pd.Series | dict[str, object],
    readiness_row: pd.Series | dict[str, object] | None = None,
) -> None:
    if not readme_path.exists():
        return

    snapshot = _safe_row(snapshot_row)
    decision = _safe_row(decision_row)
    readiness = _safe_row(readiness_row) if readiness_row is not None else {}

    key_findings_lines = [
        f"- Línea más crítica: **{decision.get('linea_prioritaria', '')}** (`line_criticality_score = {float(decision.get('line_criticality_score', 0.0)):.2f}`).",
        f"- Equipo más anómalo: **{decision.get('equipo_prioritario', '')}** (`equipment_energy_anomaly_score = {float(decision.get('equipment_energy_anomaly_score', 0.0)):.2f}`).",
        f"- Ahorro anual risk-adjusted **proxy** total: **{float(snapshot.get('annual_saving_proxy', 0.0)):,.2f} EUR/año**.",
        f"- NPV cartera ajustada por riesgo (**screening**): **{float(snapshot.get('portfolio_npv_risk_adjusted', 0.0)):,.2f} EUR**.",
        f"- Valor downside-adjusted de cartera (**screening**): **{float(snapshot.get('downside_adjusted_value_portfolio', 0.0)):,.2f} EUR**.",
    ]
    if readiness:
        key_findings_lines.append(
            f"- Readiness actual de validación: **{readiness.get('overall_readiness_level', 'n/a')}** (publish_decision: **{readiness.get('publish_decision', 'n/a')}**)."
        )

    exec_decision_lines = [
        f"- Línea a intervenir primero: **{decision.get('linea_prioritaria', '')}**.",
        f"- Equipo a priorizar: **{decision.get('equipo_prioritario', '')}**.",
        f"- Ahorro potencial de actuar ahora (**estimación proxy top-5**): **{float(decision.get('ahorro_now_top5', 0.0)):,.2f} EUR/año**.",
        "- Riesgo si se retrasa: aumento de pérdida proxy acumulada y degradación de criticidad en nodos top.",
        "- Cuándo quick win: captura de valor <12 meses con bajo burden.",
        "- Cuándo CAPEX: brecha estructural SEC/OEE con criticidad alta persistente.",
    ]

    text = readme_path.read_text(encoding="utf-8")
    text = _replace_marked_block(text, KEY_FINDINGS_START, KEY_FINDINGS_END, key_findings_lines)
    text = _replace_marked_block(text, EXEC_DECISION_START, EXEC_DECISION_END, exec_decision_lines)
    readme_path.write_text(text, encoding="utf-8")
