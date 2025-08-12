import requests
import json
import csv
from datetime import datetime
import os
from config import SCRAPE_API_KEY

# Variables de configuración actualizadas
INSTAGRAM_USERNAME = "vickydavilah"
# URL del nuevo endpoint de la API
BASE_URL_POSTS = "https://api.scrapecreators.com/v2/instagram/user/posts"
CSV_FILE_NAME = f"posts_{INSTAGRAM_USERNAME}.csv"

HEADERS = {
    "x-api-key": SCRAPE_API_KEY,
    "accept": "application/json"
}

def fetch_and_log_posts():
    """
    Realiza una petición a la nueva API para obtener las publicaciones,
    extrae datos clave de cada una y los registra en un CSV.
    """
    print(f"Buscando publicaciones para el perfil de {INSTAGRAM_USERNAME}...")
    try:
        # Los parámetros han cambiado, ahora se usa 'handle'
        parameters = {"handle": INSTAGRAM_USERNAME}
        response = requests.get(BASE_URL_POSTS, headers=HEADERS, params=parameters)
        response.raise_for_status()
        data = response.json()

        # El path a las publicaciones ahora es 'data.items'
        posts = data.get('items', [])
        if not posts:
            print("Error: No se encontraron publicaciones en la respuesta.")
            return

        file_exists = os.path.exists(CSV_FILE_NAME)

        with open(CSV_FILE_NAME, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)

            if not file_exists:
                writer.writerow([
                    'timestamp_registro',
                    'post_id',
                    'shortcode',
                    'post_url',
                    'likes_count',
                    'comments_count',
                    'caption'
                ])
                print(f"Se creó el archivo {CSV_FILE_NAME} con el encabezado.")

            current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            for post in posts:
                # El campo 'post_id' es 'pk' o 'id' en la nueva estructura
                post_id = post.get('pk')
                # El 'shortcode' ahora se llama 'code'
                shortcode = post.get('code')
                post_url = f"https://www.instagram.com/p/{shortcode}/"
                # El campo de likes ahora se llama 'like_count'
                likes_count = post.get('like_count', 0)
                # El campo de comentarios ahora es 'comment_count'
                comments_count = post.get('comment_count', 0)
                # El path para el 'caption' también ha cambiado
                caption = post.get('caption', {}).get('text', '')

                # Escribir la nueva fila de datos por cada post
                writer.writerow([
                    current_timestamp,
                    post_id,
                    shortcode,
                    post_url,
                    likes_count,
                    comments_count,
                    caption
                ])

        print(f"✅ Se registraron {len(posts)} publicaciones en {CSV_FILE_NAME}.")

    except requests.exceptions.RequestException as e:
        print(f"❌ Ocurrió un error en la petición a la API: {e}")
    except ValueError:
        print("❌ Error al decodificar la respuesta JSON.")

if __name__ == "__main__":
    fetch_and_log_posts()