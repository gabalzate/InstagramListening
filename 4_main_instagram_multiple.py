import requests
import csv
from datetime import datetime
import os
import time

# Asumimos que config.py contiene: SCRAPE_API_KEY = "tu_clave"
from config import SCRAPE_API_KEY

# Rutas a los archivos
PERFILES_FILE = "perfiles_instagram.txt"
OUTPUT_CSV_FILE = "base_de_datos_instagram.csv"

# Endpoints de la API
BASE_URL_PROFILE = "https://api.scrapecreators.com/v1/instagram/profile"
BASE_URL_POSTS = "https://api.scrapecreators.com/v2/instagram/user/posts"

HEADERS = {
    "x-api-key": SCRAPE_API_KEY,
    "accept": "application/json"
}

def get_profile_data(username):
    """Obtiene las estadísticas generales de un perfil usando el endpoint v1."""
    print(f"  > Obteniendo datos del perfil...")
    params = {"handle": username}
    try:
        response = requests.get(BASE_URL_PROFILE, headers=HEADERS, params=params)
        response.raise_for_status()
        profile_data = response.json().get('data', {}).get('user', {})
        if not profile_data:
            print(f"  ❌ No se encontraron datos del perfil en la respuesta para {username}.")
        return profile_data
    except requests.exceptions.RequestException as e:
        print(f"  ❌ Error de API para el perfil de {username}: {e}")
        return {}

def get_posts_data(username):
    """Obtiene los datos de las publicaciones de un perfil."""
    print(f"  > Obteniendo datos de las publicaciones...")
    params = {"handle": username}
    try:
        response = requests.get(BASE_URL_POSTS, headers=HEADERS, params=params)
        response.raise_for_status()
        posts_data = response.json().get('items', [])
        if not posts_data:
            print(f"  ❌ No se encontraron publicaciones en la respuesta para {username}.")
        return posts_data
    except requests.exceptions.RequestException as e:
        print(f"  ❌ Error de API para los posts de {username}: {e}")
        return []

def load_existing_timestamps(filename):
    """Carga las marcas de tiempo de creación de posts existentes del archivo CSV."""
    existing_timestamps = set()
    if os.path.exists(filename):
        with open(filename, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None)  # Saltar el encabezado
            for row in reader:
                # La marca de tiempo está en la columna 'post_created_at_str', índice 6
                if len(row) > 6 and row[6] not in ('N/A', ''):
                    existing_timestamps.add(row[6])
    return existing_timestamps

def save_data_to_csv(data_rows, filename, header):
    """Guarda o añade datos a un archivo CSV."""
    file_exists = os.path.exists(filename)
    with open(filename, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(header)
        writer.writerows(data_rows)
    print(f"  ✅ Datos guardados en {filename}.")

def main():
    if not os.path.exists(PERFILES_FILE):
        print(f"Error: No se encontró el archivo {PERFILES_FILE}. Crea el archivo y añade los nombres de usuario.")
        return

    with open(PERFILES_FILE, 'r') as f:
        perfiles = [line.strip() for line in f if line.strip()]

    if not perfiles:
        print(f"Error: El archivo {PERFILES_FILE} está vacío. Añade nombres de usuario para continuar.")
        return

    all_data_to_save = []
    current_timestamp_registro = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Cargar las marcas de tiempo de posts existentes
    existing_timestamps = load_existing_timestamps(OUTPUT_CSV_FILE)

    for username in perfiles:
        print(f"\n--- ⏳ Procesando perfil: {username} ---")

        profile = get_profile_data(username)
        posts = get_posts_data(username)

        # Pausa para evitar saturar la API
        time.sleep(5)

        # Si no hay posts, registrar solo la información del perfil
        if not posts:
            row = [
                current_timestamp_registro, username,
                profile.get('edge_followed_by', {}).get('count', 0),
                profile.get('edge_owner_to_timeline_media', {}).get('count', 0),
                profile.get('edge_follow', {}).get('count', 0),
                'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A'
            ]
            all_data_to_save.append(row)
            continue

        # Para cada post, crear una fila de datos si no existe una con el mismo timestamp
        for post in posts:
            post_created_at_unix = post.get('taken_at')
            if post_created_at_unix:
                post_created_at_str = datetime.fromtimestamp(post_created_at_unix).strftime('%Y-%m-%d %H:%M:%S')
            else:
                post_created_at_str = 'N/A'

            # Verificar si la marca de tiempo ya existe en nuestra base de datos
            if post_created_at_str not in existing_timestamps:
                post_id = post.get('pk')
                shortcode = post.get('code')
                post_url = f"https://www.instagram.com/p/{shortcode}/"
                likes = post.get('like_count', 0)
                comments = post.get('comment_count', 0)

                # Solución para el AttributeError: verificar si 'caption' no es None antes de llamar a '.get()'
                caption_obj = post.get('caption')
                if caption_obj is not None:
                    caption = caption_obj.get('text', '')
                else:
                    caption = '' # Asignar un valor predeterminado si no hay descripción

                # Se añade 'N/A' para la transcripción, sin llamar a la API
                row = [
                    current_timestamp_registro, username,
                    profile.get('edge_followed_by', {}).get('count', 0),
                    profile.get('edge_owner_to_timeline_media', {}).get('count', 0),
                    profile.get('edge_follow', {}).get('count', 0),
                    post_id, post_created_at_str, shortcode, post_url, likes, comments, caption, 'N/A'
                ]
                all_data_to_save.append(row)
            else:
                print(f"  > Post con fecha de creación '{post_created_at_str}' ya existe. Saltando para evitar duplicados.")

    # Encabezado del CSV consolidado
    header = [
        'timestamp_registro', 'username', 'followers_count', 'posts_count_total',
        'following_count', 'post_id', 'post_created_at_str', 'post_shortcode', 'post_url',
        'likes_count', 'comments_count', 'post_caption', 'post_transcript'
    ]

    save_data_to_csv(all_data_to_save, OUTPUT_CSV_FILE, header)
    print("\n🎉 Proceso de recolección diaria completado. Todos los datos han sido guardados en 'base_de_datos_instagram.csv'.")
    print("El campo 'post_transcript' ha sido dejado como 'N/A' para ser llenado por el script semanal.")

if __name__ == "__main__":
    main()