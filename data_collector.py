import os
from kaggle.api.kaggle_api_extended import KaggleApi
from datetime import datetime

def run_data_collector():
    # 1. Autenticación
    api = KaggleApi()
    api.authenticate()
    print("Autenticación completada.")

    # 2. Diccionario de datasets (Slug de Kaggle : Nombre de carpeta)
    datasets = {
        'danvargg/uber-nyc-2016': 'uber_trips',
        'mathijs/weather-data-in-new-york-city-2016': 'weather_nyc',
        'nypd/vehicle-collisions': 'accidents_nyc'
    }

    # 3. Definir la ruta de la Landing Zone
    # Usamos la fecha de hoy para cumplir con la "ejecución periódica"
    today = datetime.now().strftime("%Y-%m-%d")
    for slug, folder_name in datasets.items():
        print(f"--- Procesando: {folder_name} ---")
        
        # Crear la carpeta específica para este dataset
        final_path = os.path.join('landing_zone', folder_name, today)
        os.makedirs(final_path, exist_ok=True)

        # Descarga y descompresión
        # unzip=True extrae los CSV del archivo .zip que envía Kaggle
        api.dataset_download_files(slug, path=final_path, unzip=True)
        
        print(f"Guardado en: {final_path}")

    print("\nPROCESO FINALIZADO. Todos los datos están en la Landing Zone.")

if __name__ == "__main__":
    run_data_collector()
