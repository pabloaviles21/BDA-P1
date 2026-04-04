import os
import re
from datetime import datetime
import urllib.request

import duckdb
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

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

    return sorted(set(available_dates))


def resolve_dataset_path(dataset_name):
    available_dates = list_available_dates(dataset_name)
    if not available_dates:
        return None, None

    latest_date = available_dates[-1]
    return latest_date, os.path.join("landing_zone", dataset_name, latest_date)


def list_parquet_files(dataset_path):
    return sorted(
        os.path.join(dataset_path, file_name)
        for file_name in os.listdir(dataset_path)
        if file_name.lower().endswith(".parquet")
    )


def map_duckdb_format_to_spark(fmt):
    """Convierte formatos como "%m/%d/%Y" al estilo Spark "M/d/yyyy"."""
    return (
        fmt.replace("%m", "M")
        .replace("%d", "d")
        .replace("%Y", "yyyy")
        .replace("%H", "H")
        .replace("%M", "mm")
    )


def format_dataframe(df, dataset_name):
    dataset_config = DATASETS[dataset_name]

    # Homogeneizar los nombres de las columnas
    for old_col in df.columns:
        new_col = sanitize_column_name(old_col)
        df = df.withColumnRenamed(old_col, new_col)

    columns = df.columns

    # Aplicar transformaciones y types especificadas
    for col_name in columns:
        if col_name in dataset_config.get("date_columns", {}):
            fmt = map_duckdb_format_to_spark(dataset_config["date_columns"][col_name])
            df = df.withColumn(col_name, expr(f"try_to_date(`{col_name}`, '{fmt}')"))

        elif col_name in dataset_config.get("time_columns", {}):
            if dataset_name == "accidents_nyc" and col_name == "time":
                df = df.withColumn(col_name, expr(f"NULLIF(TRIM(`{col_name}`), '')"))
            else:
                fmt = map_duckdb_format_to_spark(dataset_config["time_columns"][col_name])
                df = df.withColumn(col_name, expr(f"try_to_timestamp(`{col_name}`, '{fmt}')"))

        elif col_name in dataset_config.get("integer_columns", []):
            df = df.withColumn(col_name, expr(f"try_cast(`{col_name}` as int)"))

        elif col_name in dataset_config.get("double_columns", []):
            df = df.withColumn(col_name, expr(f"try_cast(`{col_name}` as double)"))

    # Columna calculada personalizada (solo para accidents_nyc)
    if dataset_name == "accidents_nyc" and "date" in columns and "time" in columns:
        df = df.withColumn(
            "collision_timestamp",
            expr("try_to_timestamp(concat(date_format(date, 'yyyy-MM-dd'), ' ', `time`))"),
        )

    return df




def run_data_formatting_pipeline(db_path="formatted_zone.db"):
    print("Iniciando pipeline de Formatted Zone con Apache Spark y escritura en DuckDB...")

    # Descargar el driver JDBC si no existe (nombrado duckdb.jar como en el ejemplo)
    jar_path = "duckdb.jar"
    if not os.path.exists(jar_path):
        print("Descargando DuckDB JDBC driver...")
        urllib.request.urlretrieve("https://repo1.maven.org/maven2/org/duckdb/duckdb_jdbc/0.10.1/duckdb_jdbc-0.10.1.jar", jar_path)

    # 1. Inicializar la base de datos (mismo estilo que el notebook)
    if os.path.isfile(db_path):
        os.remove(db_path)
    conn = duckdb.connect(db_path)
    conn.close()

    # 2. Inicializar sesión de Spark
    # Usamos driver.extraClassPath en Windows para evitar el error de winutils.exe 
    # que daba al utilizar 'spark.jars'
    spark = (
        SparkSession.builder
        .appName("FormattedZonePipeline")
        .config("spark.driver.extraClassPath", jar_path)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

    try:
        for dataset_name, dataset_config in DATASETS.items():
            resolved_date, dataset_path = resolve_dataset_path(dataset_name)
            print(f"--- Procesando dataset: {dataset_name} ---")

            if not dataset_path:
                print(f"ERROR: No se encontro una carpeta para {dataset_name}.")
                continue

            parquet_files = list_parquet_files(dataset_path)
            # 1. Leer los datos Parquet
            df = spark.read.parquet(*parquet_files)

            # 2. Formatear usando PySpark DataFrame API
            df_formatted = format_dataframe(df, dataset_name)

            # 3. Cargar en DuckDB desde pandas para evitar conflictos JDBC en Windows
            table_name = dataset_config["table_name"]
            pdf = df_formatted.toPandas()
            with duckdb.connect(db_path) as conn:
                conn.register("tmp_df", pdf)
                conn.execute(
                    f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM tmp_df"
                )
                conn.unregister("tmp_df")

            print(
                f"Tabla {table_name} creada con exito en DuckDB para la fecha {resolved_date}.\n"
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    run_data_formatting_pipeline()
    
    # Forzar el cierre inmediato para evitar que Spark/Java se queden colgados en Windows
    import os
    os._exit(0)
