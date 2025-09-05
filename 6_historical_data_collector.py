import requests
import csv
from datetime import datetime, timedelta
import os
import time

# Asumimos que config.py contiene: SCRAPE_API_KEY = "tu_clave"
from config import SCRAPE_API_KEY

# Rutas a los archivos
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

def get_posts_page(username, next_max_id=None):
    """
    Obtiene una página de publicaciones de un perfil.
    Retorna los posts y el cursor para la siguiente página.
    """
    params = {"handle": username}
    if next_max_id:
        params["next_max_id"] = next_max_id
        
    try:
        response = requests.get(BASE_URL_POSTS, headers=HEADERS, params=params)
        response.raise_for_status()
        data = response.json()
        return data.get('items', []), data.get('next_max_id')
    except requests.exceptions.RequestException as e:
        print(f"  ❌ Error de API al obtener posts: {e}")
        return [], None

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
    username = input("Ingresa el nombre de usuario de Instagram para recolectar datos históricos: ")
    num_pages = 40
    
    print(f"\n--- ⏳ Recolectando datos históricos para: {username} ---")

    profile = get_profile_data(username)
    if not profile:
        return

    all_historical_data = []
    next_max_id = None
    
    # Cargar las marcas de tiempo de posts existentes para evitar duplicados
    existing_timestamps = load_existing_timestamps(OUTPUT_CSV_FILE)
    
    # Definir el límite de tiempo de 40 días
    forty_days_ago = datetime.now() - timedelta(days=40)

    for i in range(num_pages):
        print(f"--- Procesando página {i+1} de publicaciones... ---")
        posts, next_max_id = get_posts_page(username, next_max_id)
        
        if not posts:
            print("No hay más publicaciones disponibles o ocurrió un error.")
            break
        
        for post in posts:
            post_created_at_unix = post.get('taken_at')
            if post_created_at_unix:
                post_created_at_str = datetime.fromtimestamp(post_created_at_unix).strftime('%Y-%m-%d %H:%M:%S')
                post_date = datetime.fromtimestamp(post_created_at_unix)
                
                # Verificar si el post ya está en el CSV o si es demasiado antiguo
                if post_created_at_str in existing_timestamps:
                    print(f"  > Post con fecha de creación '{post_created_at_str}' ya existe. Saltando para evitar duplicados.")
                    continue
                if post_date < forty_days_ago:
                    print(f"  > Post de '{post_created_at_str}' es más antiguo que 40 días. Deteniendo la recolección.")
                    break
            else:
                post_created_at_str = 'N/A'
                
            post_id = post.get('pk')
            shortcode = post.get('code')
            post_url = f"https://www.instagram.com/p/{shortcode}/"
            likes = post.get('like_count', 0)
            comments = post.get('comment_count', 0)
            
            caption_obj = post.get('caption')
            caption = caption_obj.get('text', '') if caption_obj is not None else ''

            media_type = post.get('media_type', 'N/A')
            play_count = post.get('play_count', 'N/A')
            
            usertags_list = post.get('usertags', {}).get('in', [])
            usertags = ','.join([user.get('user', {}).get('username', '') for user in usertags_list]) if usertags_list else 'N/A'
            
            row = [
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'), username,
                profile.get('edge_followed_by', {}).get('count', 0),
                profile.get('edge_owner_to_timeline_media', {}).get('count', 0),
                profile.get('edge_follow', {}).get('count', 0),
                post_id, post_created_at_str, shortcode, post_url, likes, comments, caption, 
                media_type, play_count, usertags, 'N/A'
            ]
            all_historical_data.append(row)
        
        # Si no hay más posts en la API o encontramos posts muy antiguos, detener el bucle
        if post_date < forty_days_ago or not next_max_id:
            break
        
        time.sleep(15)  # Pausa entre peticiones de paginación para evitar bloqueos

    # Encabezado del CSV consolidado
    header = [
        'timestamp_registro', 'username', 'followers_count', 'posts_count_total', 
        'following_count', 'post_id', 'post_created_at_str', 'post_shortcode', 'post_url', 
        'likes_count', 'comments_count', 'post_caption', 'media_type', 'play_count', 'usertags', 'post_transcript'
    ]
    
    save_data_to_csv(all_historical_data, OUTPUT_CSV_FILE, header)
    print("\n🎉 Recolección de datos históricos completada. Los posts han sido guardados en 'base_de_datos_instagram.csv'.")
    print("Recuerda ejecutar el script de transcripción si es necesario.")

if __name__ == "__main__":
    main()
