from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class GenerationConfig:
    seed: int = 20260328
    start_ts: str = "2024-01-01 00:00:00"
    end_ts: str = "2025-12-31 23:00:00"
    output_dir: Path = Path(__file__).resolve().parents[2] / "data" / "raw"


SHIFT_WINDOWS = {
    "A": (6, 14),
    "B": (14, 22),
    "C": (22, 30),
}


SOURCE_PRICE_MULTIPLIER = {
    "Electricidad": 1.00,
    "GasNatural": 0.74,
    "Vapor": 0.81,
}


SOURCE_EMISSION_FACTOR = {
    "Electricidad": 0.22,
    "GasNatural": 0.19,
    "Vapor": 0.24,
}
