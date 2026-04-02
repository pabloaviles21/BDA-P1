import argparse
import os
import re
from datetime import datetime

import duckdb


DATASETS = {
    "uber_trips": {
        "table_name": "uber_data",
        "date_columns": {
            "pickup_start_date": "%m/%d/%Y",
            "pickup_end_date": "%m/%d/%Y",
        },
        "integer_columns": [
            "wave_number",
            "years",
            "week_number",
            "total_dispatched_trips",
            "unique_dispatched_vehicle",
        ],
    },
    "weather_nyc": {
        "table_name": "weather_data",
        "date_columns": {
            "date": "%m-%d-%Y",
        },
        "double_columns": [
            "maximum_temperature",
            "minimum_temperature",
            "average_temperature",
            "precipitation",
            "snow_fall",
            "snow_depth",
        ],
    },
    "accidents_nyc": {
        "table_name": "accidents_data",
        "date_columns": {
            "date": "%m/%d/%Y",
        },
        "time_columns": {
            "time": "%H:%M",
        },
        "integer_columns": [
            "persons_injured",
            "persons_killed",
            "pedestrians_injured",
            "pedestrians_killed",
            "cyclists_injured",
            "cyclists_killed",
            "motorists_injured",
            "motorists_killed",
        ],
        "double_columns": [
            "latitude",
            "longitude",
        ],
    },
}


def sanitize_column_name(column_name):
    normalized = column_name.strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized)
    return normalized.strip("_")


def list_available_dates(dataset_name):
    dataset_root = os.path.join("landing_zone", dataset_name)
    available_dates = []

    if os.path.isdir(dataset_root):
        for entry in os.listdir(dataset_root):
            entry_path = os.path.join(dataset_root, entry)
            if os.path.isdir(entry_path):
                try:
                    datetime.strptime(entry, "%Y-%m-%d")
                    available_dates.append(entry)
                except ValueError:
                    continue

    legacy_root = os.path.join("landing_zone")
    if os.path.isdir(legacy_root):
        for entry in os.listdir(legacy_root):
            entry_path = os.path.join(legacy_root, entry, dataset_name)
            if os.path.isdir(entry_path):
                try:
                    datetime.strptime(entry, "%Y-%m-%d")
                    available_dates.append(entry)
                except ValueError:
                    continue

    return sorted(set(available_dates))


def resolve_dataset_path(dataset_name, execution_date=None):
    if execution_date:
        candidates = [
            os.path.join("landing_zone", dataset_name, execution_date),
            os.path.join("landing_zone", execution_date, dataset_name),
        ]
        for candidate in candidates:
            if os.path.isdir(candidate):
                return execution_date, candidate
        return execution_date, None

    available_dates = list_available_dates(dataset_name)
    if not available_dates:
        return None, None

    latest_date = available_dates[-1]
    return resolve_dataset_path(dataset_name, latest_date)


def list_parquet_files(dataset_path):
    return sorted(
        os.path.join(dataset_path, file_name)
        for file_name in os.listdir(dataset_path)
        if file_name.lower().endswith(".parquet")
    )


def infer_columns_from_parquet(parquet_files):
    relation = duckdb.read_parquet(parquet_files[0])
    return relation.columns


def build_select_clause(dataset_name, original_columns):
    dataset_config = DATASETS[dataset_name]
    select_parts = []
    sanitized_names = []

    for original_name in original_columns:
        sanitized_name = sanitize_column_name(original_name)
        sanitized_names.append(sanitized_name)

        source_expr = f'"{original_name}"'
        cleaned_expr = f"NULLIF(TRIM({source_expr}::VARCHAR), '')"

        if sanitized_name in dataset_config.get("date_columns", {}):
            expr = (
                f"TRY_STRPTIME({cleaned_expr}, "
                f"'{dataset_config['date_columns'][sanitized_name]}')::DATE AS {sanitized_name}"
            )
        elif sanitized_name in dataset_config.get("time_columns", {}):
            expr = (
                f"TRY_STRPTIME({cleaned_expr}, "
                f"'{dataset_config['time_columns'][sanitized_name]}')::TIME AS {sanitized_name}"
            )
        elif sanitized_name in dataset_config.get("integer_columns", []):
            expr = f"TRY_CAST({cleaned_expr} AS INTEGER) AS {sanitized_name}"
        elif sanitized_name in dataset_config.get("double_columns", []):
            expr = f"TRY_CAST({cleaned_expr} AS DOUBLE) AS {sanitized_name}"
        else:
            expr = f"{cleaned_expr} AS {sanitized_name}"

        select_parts.append(expr)

    if dataset_name == "accidents_nyc" and "date" in sanitized_names and "time" in sanitized_names:
        select_parts.append(
            "CAST("
            "TRY_STRPTIME(NULLIF(TRIM(\"DATE\"::VARCHAR), ''), '%m/%d/%Y')::DATE + "
            "TRY_STRPTIME(NULLIF(TRIM(\"TIME\"::VARCHAR), ''), '%H:%M')::TIME "
            "AS TIMESTAMP"
            ") AS collision_timestamp"
        )

    return ",\n    ".join(select_parts)


def create_or_replace_table(connection, dataset_name, parquet_files):
    dataset_config = DATASETS[dataset_name]
    original_columns = infer_columns_from_parquet(parquet_files)
    select_clause = build_select_clause(dataset_name, original_columns)
    escaped_parquet_files = [file_path.replace("\\", "\\\\") for file_path in parquet_files]
    parquet_list_sql = ", ".join(f"'{file_path}'" for file_path in escaped_parquet_files)

    query = f"""
        CREATE OR REPLACE TABLE {dataset_config['table_name']} AS
        SELECT
            {select_clause}
        FROM read_parquet([{parquet_list_sql}])
    """
    connection.execute(query)


def print_table_schema(connection, table_name):
    rows = connection.execute(f"DESCRIBE {table_name}").fetchall()
    print(f"Esquema de '{table_name}':")
    for column_name, column_type, *_ in rows:
        print(f" - {column_name}: {column_type}")


def run_data_formatting_pipeline(execution_date=None, db_path="formatted_zone.db"):
    print("Iniciando pipeline de Formatted Zone con DuckDB...")
    connection = duckdb.connect(db_path)

    processed = []
    failed = []

    try:
        for dataset_name, dataset_config in DATASETS.items():
            resolved_date, dataset_path = resolve_dataset_path(dataset_name, execution_date)
            print(f"--- Procesando dataset: {dataset_name} ---")

            if not dataset_path:
                failed.append((dataset_name, "No se encontro una carpeta valida en landing_zone."))
                print(f"ERROR: No se encontro una carpeta para {dataset_name}.")
                continue

            parquet_files = list_parquet_files(dataset_path)
            if not parquet_files:
                failed.append((dataset_name, f"No hay archivos Parquet en {dataset_path}."))
                print(f"ERROR: No hay archivos Parquet en {dataset_path}.")
                continue

            try:
                create_or_replace_table(connection, dataset_name, parquet_files)
                print_table_schema(connection, dataset_config["table_name"])
                processed.append((dataset_name, resolved_date, len(parquet_files)))
                print(
                    f"Tabla {dataset_config['table_name']} creada con exito para la fecha {resolved_date}.\n"
                )
            except Exception as exc:
                failed.append((dataset_name, str(exc)))
                print(f"ERROR procesando {dataset_name}: {exc}\n")
    finally:
        connection.close()

    print("Resumen de la Formatted Zone:")
    print(f"Datasets procesados correctamente: {len(processed)}")
    print(f"Datasets con error: {len(failed)}")

    if failed:
        for dataset_name, message in failed:
            print(f"- {dataset_name}: {message}")

    return {"processed": processed, "failed": failed}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Normaliza la landing zone y la carga en DuckDB."
    )
    parser.add_argument(
        "--date",
        dest="execution_date",
        help="Fecha a procesar en formato YYYY-MM-DD. Si no se indica, usa la ultima disponible por dataset.",
    )
    parser.add_argument(
        "--db-path",
        dest="db_path",
        default="formatted_zone.db",
        help="Ruta del fichero DuckDB de salida. Por defecto usa formatted_zone.db.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_data_formatting_pipeline(
        execution_date=args.execution_date,
        db_path=args.db_path,
    )
