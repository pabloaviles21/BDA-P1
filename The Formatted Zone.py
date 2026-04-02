import os
from datetime import datetime
from pyspark.sql import SparkSession

def run_data_formatting_pipeline():
    # 1. Iniciar la sesión de Spark configurando el driver de DuckDB
    print("Iniciando Spark Session...")
    spark = SparkSession.builder \
        .appName("FormattedZonePipeline") \
        .config("spark.jars", "duckdb.jar") \
        .getOrCreate()

    # 2. Definir rutas
    # Suponiendo que procesamos los datos bajados hoy
    today = datetime.now().strftime("%Y-%m-%d")
    landing_base_path = os.path.join('landing_zone', today)
    
    # Base de datos relacional de la Formatted Zone
    db_path = "formatted_zone.db"
    db_url = f"jdbc:duckdb:{db_path}"

    # Diccionario de carpetas a nombre de tabla SQL
    datasets = {
        'uber_trips': 'uber_data',
        'weather_nyc': 'weather_data',
        'accidents_nyc': 'accidents_data'
    }

    # 3. Procesar cada dataset
    for folder_name, table_name in datasets.items():
        dataset_path = os.path.join(landing_base_path, folder_name)
        print(f"--- Procesando dataset: {folder_name} ---")
        
        # Leer todos los CSV de esa carpeta con Spark
        # inferSchema=True le dice a Spark que adivine si es texto, número, fecha, etc.
        # header=True le dice que la primera fila tiene los nombres de las columnas
        df = spark.read \
            .option("delimiter", ",") \
            .option("header", True) \
            .option("inferSchema", True) \
            .csv(f"{dataset_path}/*.csv")
        
        # Mostrar el esquema (columnas y tipos de datos) por consola para verificar
        df.printSchema()

        # 4. Escribir los datos en DuckDB (Nuestra base de datos relacional)
        print(f"Guardando como tabla '{table_name}' en DuckDB...")
        df.write \
            .format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", table_name) \
            .option("driver", "org.duckdb.DuckDBDriver") \
            .mode("overwrite") \
            .save()
            
        print(f"Tabla {table_name} creada con éxito.\n")

    print("PROCESO FINALIZADO. La Formatted Zone está lista.")
    
    # Cerrar la sesión para liberar memoria
    spark.stop()

if __name__ == "__main__":
    run_data_formatting_pipeline()