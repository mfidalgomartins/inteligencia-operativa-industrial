from __future__ import annotations

from src.synthetic_industrial.config import GenerationConfig
from src.synthetic_industrial.pipeline import run_generation_pipeline


def main() -> None:
    config = GenerationConfig()
    tables = run_generation_pipeline(config)

    print("Generacion sintetica completada.")
    print(f"Output directory: {config.output_dir}")
    print("Tablas generadas y cardinalidades:")
    for name, df in tables.items():
        print(f"- {name}: {len(df):,} filas")


if __name__ == "__main__":
    main()
