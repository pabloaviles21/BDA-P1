import argparse
import os
from datetime import datetime
import pandas as pd

from kaggle.api.kaggle_api_extended import KaggleApi


DATASETS = {
    "danvargg/uber-nyc-2016": "uber_trips",
    "mathijs/weather-data-in-new-york-city-2016": "weather_nyc",
    "nypd/vehicle-collisions": "accidents_nyc",
}


def build_output_path(folder_name, execution_date):
    return os.path.join("landing_zone", folder_name, execution_date)


def run_data_collector(execution_date=None):
    execution_date = execution_date or datetime.now().strftime("%Y-%m-%d")

    api = KaggleApi()
    api.authenticate()
    print("Autenticacion completada.")

    processed = []
    failed = []

    for slug, folder_name in DATASETS.items():
        print(f"--- Procesando: {folder_name} ({execution_date}) ---")
        final_path = build_output_path(folder_name, execution_date)
        os.makedirs(final_path, exist_ok=True)

        try:
            api.dataset_download_files(slug, path=final_path, unzip=True)
            
            # Convertir archivos CSV descargados a Parquet
            for file in os.listdir(final_path):
                if file.endswith('.csv'):
                    csv_file_path = os.path.join(final_path, file)
                    parquet_file_path = os.path.join(final_path, file.replace('.csv', '.parquet'))
                    
                    print(f"Convirtiendo a Parquet: {file}...")
                    try:
                        df = pd.read_csv(csv_file_path, low_memory=False)
                        df.to_parquet(parquet_file_path, engine='pyarrow')
                        os.remove(csv_file_path) # Borramos el CSV original
                    except Exception as e:
                        print(f"Error al convertir {file}: {e}")

            processed.append(folder_name)
            print(f"Guardado en formato Parquet en: {final_path}")
        except Exception as exc:
            failed.append(folder_name)
            print(f"ERROR descargando {folder_name}: {exc}")

    print("\nResumen de la ejecucion:")
    print(f"Datasets descargados correctamente: {len(processed)}")
    print(f"Datasets con error: {len(failed)}")

    if failed:
        print("Fallaron:", ", ".join(failed))

    return {"execution_date": execution_date, "processed": processed, "failed": failed}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Descarga datasets de Kaggle en landing_zone/<dataset>/<fecha>."
    )
    parser.add_argument(
        "--date",
        dest="execution_date",
        help="Fecha de ejecucion en formato YYYY-MM-DD. Si no se indica, usa la fecha actual.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_data_collector(execution_date=args.execution_date)
