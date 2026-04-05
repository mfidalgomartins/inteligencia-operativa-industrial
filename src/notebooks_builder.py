from __future__ import annotations

import json
from pathlib import Path

from .config import NOTEBOOKS_DIR


def _code_cell(source: str) -> dict:
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": source.splitlines(keepends=True),
    }


def _md_cell(source: str) -> dict:
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": source.splitlines(keepends=True),
    }


def _write_notebook(path: Path, cells: list[dict]) -> None:
    nb = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {"name": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    path.write_text(json.dumps(nb, ensure_ascii=False, indent=2), encoding="utf-8")


def build_notebooks() -> None:
    NOTEBOOKS_DIR.mkdir(parents=True, exist_ok=True)

    nb_main = [
        _md_cell(
            "# Notebook Principal\n"
            "Análisis ejecutivo de eficiencia, energía, riesgo y priorización en planta industrial."
        ),
        _code_cell(
            "import pandas as pd\n"
            "from pathlib import Path\n\n"
            "root = Path.cwd().resolve().parents[0]\n"
            "processed = root / 'data' / 'processed'\n\n"
            "line = pd.read_csv(processed / 'line_criticality_scores.csv')\n"
            "equip = pd.read_csv(processed / 'equipment_energy_anomaly_scores.csv')\n"
            "causes = pd.read_csv(processed / 'loss_root_cause_score.csv')\n"
            "line.sort_values('line_criticality_score', ascending=False).head(10)"
        ),
        _code_cell("equip.sort_values('equipment_energy_anomaly_score', ascending=False).head(10)"),
        _code_cell("causes.sort_values('loss_root_cause_score', ascending=False).head(10)"),
        _md_cell(
            "## Interpretación\n"
            "1. Validar concentración de pérdidas por línea.\n"
            "2. Priorizar equipos críticos con anomalía persistente.\n"
            "3. Contrastarlo contra cartera de iniciativas."
        ),
    ]
    _write_notebook(NOTEBOOKS_DIR / "01_notebook_principal.ipynb", nb_main)

    nb_scenarios = [
        _md_cell(
            "# Notebook de Escenarios e Inversiones\n"
            "Evaluación de trade-offs entre quick wins, mantenimiento, optimización y CAPEX."
        ),
        _code_cell(
            "import pandas as pd\n"
            "from pathlib import Path\n\n"
            "root = Path.cwd().resolve().parents[0]\n"
            "processed = root / 'data' / 'processed'\n\n"
            "scenario = pd.read_csv(processed / 'scenario_ranking.csv')\n"
            "invest = pd.read_csv(processed / 'investment_prioritization_final.csv')\n"
            "scenario[['iniciativa_id','escenario_label','ahorro_economico_anual_proxy','payback_meses','score_tradeoff']].head(15)"
        ),
        _code_cell("invest[['iniciativa_id','decision_rule','improvement_priority_index','main_business_case']].head(20)"),
        _code_cell(
            "invest.groupby('decision_rule')[['annual_saving_proxy','capex_estimado']].sum().sort_values('annual_saving_proxy', ascending=False)"
        ),
    ]
    _write_notebook(NOTEBOOKS_DIR / "02_notebook_escenarios.ipynb", nb_scenarios)


if __name__ == "__main__":
    build_notebooks()
