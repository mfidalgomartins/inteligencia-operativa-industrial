from __future__ import annotations

from pathlib import Path

import duckdb

from .config import DATA_PROCESSED_DIR, DATA_RAW_DIR, DUCKDB_PATH, SQL_ARCHITECTURE_FILES, SQL_DIR


EXPORT_OBJECTS = [
    "int_line_hour",
    "int_equipment_day",
    "int_line_shift",
    "mart_line_day",
    "mart_equipment_day",
    "mart_plant_week",
    "vw_line_energy_performance",
    "vw_equipment_efficiency_risk",
    "vw_shift_operational_variance",
    "vw_loss_root_cause_summary",
    "vw_improvement_value_pool",
    "kpi_top_lineas_perdidas",
    "kpi_top_equipos_sobreconsumo",
    "kpi_top_turnos_ineficiencia",
    "kpi_top_causas_raiz",
    "kpi_lineas_peor_oee",
    "kpi_lineas_peor_sec",
    "kpi_iniciativas_mayor_valor",
    "kpi_capex_mejor_payback",
    "kpi_emisiones_evitables_proxy",
    "kpi_criticidad_global",
    "validation_sql_checks",
]


def _read_sql(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def run_sql_pipeline() -> None:
    """Ejecuta la arquitectura SQL (DuckDB) y exporta tablas/vistas analíticas."""
    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DUCKDB_PATH))
    raw_dir = str(DATA_RAW_DIR.resolve()).replace("\\", "/")

    execution_rows: list[dict[str, str]] = []
    for script_name in SQL_ARCHITECTURE_FILES:
        script_path = SQL_DIR / script_name
        sql = _read_sql(script_path).format(raw_dir=raw_dir)
        con.execute(sql)
        execution_rows.append({"orden": script_name[:2], "script": script_name, "estado": "OK"})

    for obj_name in EXPORT_OBJECTS:
        output_path = DATA_PROCESSED_DIR / f"{obj_name}.csv"
        output_sql = (
            f"COPY (SELECT * FROM {obj_name}) "
            f"TO '{str(output_path).replace('\\', '/')}' WITH (HEADER, DELIMITER ',')"
        )
        con.execute(output_sql)

    con.execute(
        """
        CREATE OR REPLACE TABLE sql_execution_log AS
        SELECT *
        FROM (
            VALUES
        """
        + ",\n".join([f"('{row['orden']}', '{row['script']}', '{row['estado']}')" for row in execution_rows])
        + "\n) AS t(orden, script, estado)"
    )
    con.execute(
        f"COPY (SELECT * FROM sql_execution_log ORDER BY orden) TO '{str((DATA_PROCESSED_DIR / 'sql_execution_log.csv')).replace('\\', '/')}' WITH (HEADER, DELIMITER ',')"
    )

    con.close()


if __name__ == "__main__":
    run_sql_pipeline()
