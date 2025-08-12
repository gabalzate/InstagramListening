import requests
import csv
from datetime import datetime
import os
from config import SCRAPE_API_KEY

# Variables de configuración
INSTAGRAM_USERNAME = "vickydavilah"
BASE_URL_PROFILE = "https://api.scrapecreators.com/v1/instagram/profile"
CSV_FILE_NAME = "perfil_vickydavilah.csv"

HEADERS = {
    "x-api-key": SCRAPE_API_KEY,
    "accept": "application/json"
}
PARAMETERS = {
    "handle": INSTAGRAM_USERNAME
}

def fetch_and_log_profile_stats():
    """
    Realiza una petición a la API para obtener los datos del perfil y
    los registra en un CSV para seguimiento en el tiempo.
    """
    print(f"Buscando estadísticas de perfil para {INSTAGRAM_USERNAME}...")
    try:
        response = requests.get(BASE_URL_PROFILE, headers=HEADERS, params=PARAMETERS)
        response.raise_for_status()
        data = response.json()

        # Extraer información del perfil
        profile_data = data.get('data', {}).get('user', {})
        if not profile_data:
            print("Error: No se encontraron datos del perfil en la respuesta.")
            return

        # Extraer métricas clave
        followers_count = profile_data.get('edge_followed_by', {}).get('count', 0)
        posts_count = profile_data.get('edge_owner_to_timeline_media', {}).get('count', 0)
        following_count = profile_data.get('edge_follow', {}).get('count', 0)

        # Preparar los datos para el CSV
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        new_row = [
            current_timestamp,
            INSTAGRAM_USERNAME,
            followers_count,
            posts_count,
            following_count
        ]

        # Verificar si el archivo CSV existe para escribir el encabezado
        file_exists = os.path.exists(CSV_FILE_NAME)

        with open(CSV_FILE_NAME, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)

            if not file_exists:
                writer.writerow(['timestamp', 'username', 'followers_count', 'posts_count', 'following_count'])
                print(f"Se creó el archivo {CSV_FILE_NAME} con el encabezado.")

            writer.writerow(new_row)

        print(f"✅ Estadísticas del perfil registradas exitosamente en {CSV_FILE_NAME} en la marca de tiempo: {current_timestamp}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Ocurrió un error en la petición a la API: {e}")
    except ValueError:
        print("❌ Error al decodificar la respuesta JSON.")

if __name__ == "__main__":
    fetch_and_log_profile_stats()