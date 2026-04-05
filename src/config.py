from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
DATA_SAMPLES_DIR = PROJECT_ROOT / "data" / "samples"
SQL_DIR = PROJECT_ROOT / "sql"
OUTPUT_CHARTS_DIR = PROJECT_ROOT / "outputs" / "charts"
OUTPUT_DASHBOARD_DIR = PROJECT_ROOT / "outputs" / "dashboard"
OUTPUT_REPORTS_DIR = PROJECT_ROOT / "outputs" / "reports"
OUTPUT_RELEASE_PACKS_DIR = PROJECT_ROOT / "outputs" / "release_packs"
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"
DOCS_DIR = PROJECT_ROOT / "docs"

RANDOM_SEED = 20260328
START_DATE = "2024-01-01"
END_DATE = "2025-12-31"
DUCKDB_PATH = DATA_PROCESSED_DIR / "planta_industrial.duckdb"

MARGEN_CONTRIBUCION_EUR_TON = 480.0
COSTE_SCRAP_EUR_TON = 210.0
COSTE_MANTENIMIENTO_EUR_HORA_PARADA = 950.0
COSTE_EMISION_EUR_TON_CO2 = 85.0

SQL_ARCHITECTURE_FILES = [
    "01_staging_energy.sql",
    "02_staging_production.sql",
    "03_staging_events.sql",
    "04_integrated_line_hour.sql",
    "05_integrated_equipment_day.sql",
    "06_integrated_line_shift.sql",
    "07_analytical_mart_line_day.sql",
    "08_analytical_mart_equipment_day.sql",
    "09_analytical_mart_plant_week.sql",
    "10_kpi_queries.sql",
    "11_validation_queries.sql",
]
