# =============================================================================
# SCRIPT 17: ACTUALIZADOR DE DATOS EN VIVO (Versión 2 - A prueba de Pinned Posts)
#
# Objetivo:
# Actualiza la base de datos con las publicaciones más recientes, manejando
# correctamente los posts fijados que pueden alterar el orden cronológico.
#
# Lógica Mejorada:
# 1. El rango de fechas se calcula igual.
# 2. Al procesar una página de posts, no se detiene al encontrar el primer
#    post antiguo. En su lugar, revisa la página completa.
# 3. La búsqueda para un perfil se detiene solo si se encuentra un post ya
#    existente en la base de datos o si una página ENTERA contiene solo
#    posts más antiguos que el rango de búsqueda.
# =============================================================================

import requests
import csv
from datetime import datetime, timedelta
import os
import time
import pandas as pd

# Asumimos que config.py contiene: SCRAPE_API_KEY
try:
    from config import SCRAPE_API_KEY
except ImportError:
    print("❌ Error: Asegúrate de que tu archivo 'config.py' existe y contiene la variable SCRAPE_API_KEY.")
    exit()

# --- CONFIGURACIÓN ---
PROFILES_FILE = "perfiles_instagram.txt"
OUTPUT_CSV_FILE = "base_de_datos_instagram.csv"
BATCH_SIZE = 5

# --- Endpoints de la API ---
BASE_URL_PROFILE = "https://api.scrapecreators.com/v1/instagram/profile"
BASE_URL_POSTS = "https://api.scrapecreators.com/v2/instagram/user/posts"

HEADERS = {
    "x-api-key": SCRAPE_API_KEY,
    "accept": "application/json"
}

# --- FUNCIONES AUXILIARES (sin cambios) ---

def get_valid_date_range():
    today = datetime.now()
    end_date = today
    if today.day <= 5:
        first_day_of_current_month = today.replace(day=1)
        last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
        start_date = last_day_of_previous_month.replace(day=1)
        print(f"ℹ️  Ejecutando en los primeros 5 días del mes. Se buscarán posts desde el mes anterior ({start_date.strftime('%Y-%m')}).")
    else:
        start_date = today.replace(day=1)
        print(f"ℹ️  Buscando posts en el mes actual ({start_date.strftime('%Y-%m')}).")
    return start_date.date(), end_date.date()

def get_profile_data(username):
    params = {"handle": username}
    try:
        response = requests.get(BASE_URL_PROFILE, headers=HEADERS, params=params)
        response.raise_for_status()
        return response.json().get('data', {}).get('user', {})
    except requests.exceptions.RequestException as e:
        print(f"  ❌ Error de API para el perfil de {username}: {e}")
        return {}

def get_posts_page(username, next_max_id=None):
    params = {"handle": username}
    if next_max_id:
        params["next_max_id"] = next_max_id
    try:
        response = requests.get(BASE_URL_POSTS, headers=HEADERS, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()
        return data.get("items", []), data.get("next_max_id")
    except requests.exceptions.RequestException as e:
        print(f"  ❌ Error de API obteniendo posts para {username}: {e}")
        return [], None

def save_batch_to_csv(data_batch, filename):
    header = [
        'timestamp_registro', 'username', 'followers_count', 'posts_count_total', 
        'following_count', 'post_id', 'post_created_at_str', 'post_shortcode', 'post_url', 
        'likes_count', 'comments_count', 'post_caption', 'media_type', 'play_count', 'usertags', 'post_transcript'
    ]
    file_exists = os.path.isfile(filename)
    try:
        with open(filename, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(header)
            writer.writerows(data_batch)
        print(f"  💾 Lote de {len(data_batch)} registros guardado exitosamente.")
    except IOError as e:
        print(f"  ❌ Error al guardar el lote en el archivo CSV: {e}")

# --- FUNCIÓN PRINCIPAL ---
def main():
    print("🚀 Iniciando el script de actualización de datos...")
    
    start_date, end_date = get_valid_date_range()
    
    existing_post_ids = set()
    if os.path.exists(OUTPUT_CSV_FILE):
        try:
            df_existing = pd.read_csv(OUTPUT_CSV_FILE)
            existing_post_ids = set(df_existing['post_id'].astype(str))
            print(f"✅ Se cargaron {len(existing_post_ids)} IDs de posts existentes.")
        except Exception as e:
            print(f"⚠️  Advertencia: No se pudo leer el archivo CSV existente. Error: {e}")

    try:
        with open(PROFILES_FILE, 'r', encoding='utf-8') as f:
            profiles = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"❌ Error: No se encontró el archivo '{PROFILES_FILE}'.")
        return

    total_new_posts_added = 0
    
    for username in profiles:
        print(f"\n--- Procesando perfil: {username} ---")
        profile_data = get_profile_data(username)
        if not profile_data:
            continue

        new_data_batch = []
        next_max_id = None
        
        while True:
            posts, next_max_id_from_api = get_posts_page(username, next_max_id)
            if not posts:
                print("  > No se encontraron más posts para este perfil.")
                break

            print(f"  > Procesando un lote de {len(posts)} posts de la API...")
            
            # --- 👇 LÓGICA MEJORADA PARA MANEJAR POSTS FIJADOS 👇 ---
            stop_fetching_for_profile = False
            page_contained_only_old_posts = True # Asumimos que la página es vieja hasta demostrar lo contrario

            for post in posts:
                post_id = str(post.get('pk'))
                
                # Comprobación 1: Si el post ya existe, nos detenemos. Esta es la señal de parada más fuerte.
                if post_id in existing_post_ids:
                    print(f"  > Se encontró un post ya existente (ID: {post_id}). Deteniendo la búsqueda para {username}.")
                    stop_fetching_for_profile = True
                    break # Salimos del bucle 'for post in posts'
                
                post_date = datetime.fromtimestamp(post.get('taken_at', 0)).date()
                
                # Comprobación 2: Verificamos si CUALQUIER post de la página es reciente.
                if post_date >= start_date:
                    page_contained_only_old_posts = False

                # Comprobación 3: Añadimos el post a la base de datos si está en el rango de fechas.
                if start_date <= post_date <= end_date:
                    # Formatear la fila de datos
                    post_created_at_str = datetime.fromtimestamp(post.get('taken_at', 0)).strftime('%Y-%m-%d %H:%M:%S')
                    shortcode = post.get('code', 'N/A')
                    post_url = f"https://www.instagram.com/p/{shortcode}/"
                    caption = post.get('caption', {}).get('text', '') if post.get('caption') else ''
                    media_type = post.get('media_type', 1)
                    usertags_list = [tag['user']['username'] for tag in post.get('usertags', {}).get('in', [])]

                    row = [
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'), username,
                        profile_data.get('follower_count', 0),
                        profile_data.get('media_count', 0),
                        profile_data.get('following_count', 0),
                        post_id, post_created_at_str, shortcode, post_url,
                        post.get('like_count', 0), post.get('comment_count', 0), caption, 
                        media_type, post.get('play_count', 0) if media_type == 2 else 'N/A',
                        ', '.join(usertags_list) if usertags_list else 'N/A', 'N/A'
                    ]
                    new_data_batch.append(row)
                    total_new_posts_added += 1
                    
                    if len(new_data_batch) >= BATCH_SIZE:
                        save_batch_to_csv(new_data_batch, OUTPUT_CSV_FILE)
                        new_data_batch = []
            
            # Comprobación final después de revisar toda la página
            if stop_fetching_for_profile or page_contained_only_old_posts:
                if page_contained_only_old_posts and not stop_fetching_for_profile:
                    print(f"  > La página completa contenía posts antiguos. Deteniendo búsqueda para {username}.")
                break # Salimos del bucle 'while True'

            if not next_max_id_from_api:
                break # Si no hay más páginas, salimos
            
            next_max_id = next_max_id_from_api
            time.sleep(5)

        if new_data_batch:
            save_batch_to_csv(new_data_batch, OUTPUT_CSV_FILE)
        
        print(f"  > Finalizado el procesamiento para {username}.")
        time.sleep(10)

    print(f"\n🎉 ¡Proceso de actualización completado! Se añadieron un total de {total_new_posts_added} nuevos posts.")

if __name__ == "__main__":
    main()
