from __future__ import annotations

from .synthetic_industrial.config import GenerationConfig
from .synthetic_industrial.pipeline import run_generation_pipeline


def generate_synthetic_data() -> None:
    """Genera todo el ecosistema sintético industrial en data/raw/."""
    run_generation_pipeline(GenerationConfig())


if __name__ == "__main__":
    generate_synthetic_data()
