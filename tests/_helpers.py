from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
OUTPUTS = PROJECT_ROOT / "outputs"
REPORTS = OUTPUTS / "reports"
DASHBOARD_HTML = OUTPUTS / "dashboard" / "dashboard_inteligencia_operativa.html"
MANIFEST_LATEST = PROJECT_ROOT / "manifests" / "build_manifest_latest.json"


def read_csv(name: str) -> pd.DataFrame:
    return pd.read_csv(DATA_PROCESSED / name)


def file_sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()
