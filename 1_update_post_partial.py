# =============================================================================
# SCRIPT 17: ACTUALIZADOR DE DATOS EN VIVO (Versión 6.1 - CORRECCIÓN POR SHORTCODE)
#
# Lógica de Parada: El script se detiene si encuentra 5 o más posts existentes (duplicados)
# en la misma página, basándose en el post_shortcode.
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
DUPLICATE_THRESHOLD = 5 # Si se encuentran 5 shortcodes duplicados, la búsqueda se detiene.

# --- Endpoints de la API ---
BASE_URL_PROFILE = "https://api.scrapecreators.com/v1/instagram/profile"
BASE_URL_POSTS = "https://api.scrapecreators.com/v2/instagram/user/posts"

HEADERS = {
    "x-api-key": SCRAPE_API_KEY,
    "accept": "application/json"
}

# --- FUNCIONES AUXILIARES ---

def get_valid_date_range():
    today = datetime.now()
    end_date = today
    if today.day <= 5:
        first_day_of_current_month = today.replace(day=1)
        last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
        start_date = last_day_of_previous_month.replace(day=1)
        print(f"ℹ️ Ejecutando en los primeros 5 días del mes. Se buscarán posts desde el mes anterior ({start_date.strftime('%Y-%m')}).")
    else:
        start_date = today.replace(day=1)
        print(f"ℹ️ Buscando posts en el mes actual ({start_date.strftime('%Y-%m')}).")
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
    
    # Conjunto para almacenar todos los shortcodes existentes para la validación
    existing_shortcodes = set() 
    if os.path.exists(OUTPUT_CSV_FILE):
        try:
            # --- CORRECCIÓN CLAVE: Forzar la lectura de post_shortcode como string ---
            df_existing = pd.read_csv(OUTPUT_CSV_FILE, dtype={'post_shortcode': str})
            
            # Limpiar NaNs y cargar los shortcodes en el conjunto.
            existing_shortcodes = set(df_existing['post_shortcode'].dropna()) 
            print(f"✅ Se cargaron {len(existing_shortcodes)} shortcodes existentes para deduplicación.")
            
        except Exception as e:
            print(f"⚠️ Advertencia: No se pudo leer el archivo CSV existente o las columnas. Error: {e}")

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

        # Extracción de Métricas del Perfil
        followers_count = profile_data.get('edge_followed_by', {}).get('count', 0)
        posts_count_total = profile_data.get('edge_owner_to_timeline_media', {}).get('count', 0)
        following_count = profile_data.get('edge_follow', {}).get('count', 0)

        new_data_batch = []
        next_max_id = None
        
        while True:
            posts, next_max_id_from_api = get_posts_page(username, next_max_id)
            if not posts:
                print("  > No se encontraron más posts para este perfil.")
                break

            print(f"  > Procesando un lote de {len(posts)} posts de la API...")
            
            duplicate_count_in_page = 0 
            
            for post in posts:
                # --- VERIFICACIÓN DE DUPLICADO POR SHORTCODE ---
                shortcode = post.get('code', '')
                
                if shortcode in existing_shortcodes:
                    duplicate_count_in_page += 1
                    continue # Lo contamos y saltamos
                
                # --- SI LLEGA AQUÍ, EL POST ES COMPLETAMENTE NUEVO ---
                
                # Manejo de la fecha del post
                taken_at = post.get('taken_at', 0)
                if taken_at == 0:
                    print(f"  ⚠️ Advertencia: Post con shortcode {shortcode} no tiene fecha de creación. Saltando.")
                    continue
                
                post_date = datetime.fromtimestamp(taken_at).date()
                
                # Comprobación de Rango (sigue siendo relevante para el rango de la API)
                if start_date <= post_date <= end_date:
                    
                    post_created_at_str = datetime.fromtimestamp(taken_at).strftime('%Y-%m-%d %H:%M:%S')
                    post_url = f"https://www.instagram.com/p/{shortcode}/"
                    post_id = str(post.get('pk', 'N/A')) # Mantenemos el post_id en la fila
                    caption_obj = post.get('caption')
                    caption = caption_obj.get('text', '') if caption_obj else ''
                    media_type = post.get('media_type', 1)
                    
                    play_count = post.get('play_count', 0) if media_type == 2 else 0 
                    usertags_list = [tag['user']['username'] for tag in post.get('usertags', {}).get('in', [])]

                    row = [
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'), username,
                        followers_count, 
                        posts_count_total, 
                        following_count,
                        post_id, post_created_at_str, shortcode, post_url,
                        post.get('like_count', 0), post.get('comment_count', 0), caption, 
                        media_type, play_count,
                        ', '.join(usertags_list) if usertags_list else 'N/A', 'N/A'
                    ]
                    new_data_batch.append(row)
                    total_new_posts_added += 1
                    existing_shortcodes.add(shortcode) # Marcarlo como existente inmediatamente
                    
                    if len(new_data_batch) >= BATCH_SIZE:
                        save_batch_to_csv(new_data_batch, OUTPUT_CSV_FILE)
                        new_data_batch = []
                
            # --- VERIFICACIÓN DE PARADA POR DENSIDAD ---
            if duplicate_count_in_page >= DUPLICATE_THRESHOLD:
                print(f"  🛑 ALERTA DE PARADA: Se encontraron {duplicate_count_in_page} posts duplicados. Deteniendo la búsqueda para {username}.")
                break
            
            if not next_max_id_from_api:
                break # Fin de la paginación
                
            next_max_id = next_max_id_from_api
            time.sleep(5)

        if new_data_batch:
            save_batch_to_csv(new_data_batch, OUTPUT_CSV_FILE)
            
        print(f"  > Finalizado el procesamiento para {username}.")
        time.sleep(10)

    print(f"\n🎉 ¡Proceso de actualización completado! Se añadieron un total de {total_new_posts_added} nuevos posts.")

if __name__ == "__main__":
    main()
