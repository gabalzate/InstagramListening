import requests
import csv
import os
import time
from datetime import datetime, timedelta

# Asumimos que config.py contiene: SCRAPE_API_KEY = "tu_clave"
from config import SCRAPE_API_KEY

# --- CONFIGURACIÓN ---
INPUT_PROFILES_FILE = "perfiles_instagram.txt"
OUTPUT_FOLDER = "menciones"
CSV_HEADER = [
    'timestamp_registro', 'username', 'followers_count', 'posts_count_total',
    'following_count', 'post_id', 'post_created_at_str', 'post_shortcode', 'post_url',
    'likes_count', 'comments_count', 'post_caption', 'media_type', 'play_count',
    'usertags', 'post_transcript'
]

# --- ENDPOINTS DE LA API ---
URL_GOOGLE_SEARCH = "https://api.scrapecreators.com/v1/google/search"
URL_INSTAGRAM_POST = "https://api.scrapecreators.com/v1/instagram/post"

HEADERS = {
    "x-api-key": SCRAPE_API_KEY,
    "accept": "application/json"
}

# --- FUNCIONES AUXILIARES ---

def search_google_for_mentions(username):
    """Construye la consulta y busca menciones en Google."""
    print(f"  🔍 Buscando menciones para '{username}' en Google...")

    one_month_ago = datetime.now() - timedelta(days=30)
    date_str = one_month_ago.strftime("%Y-%m-%d")

    query = f'site:instagram.com "@{username}" -inurl:"https://www.instagram.com/{username}/" after:{date_str}'
    params = {"query": query, "region": "CO"}

    try:
        response = requests.get(URL_GOOGLE_SEARCH, headers=HEADERS, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        post_urls = [
            result['url'] for result in data.get('results', [])
            if 'url' in result and ('/p/' in result['url'] or '/reel/' in result['url'])
        ]
        print(f"  ✅ Se encontraron {len(post_urls)} menciones potenciales.")
        return post_urls
    except requests.exceptions.RequestException as e:
        print(f"  ❌ Error al buscar en Google: {e}")
        return []

def get_post_details(post_url):
    """Obtiene los detalles de un post de Instagram a partir de su URL."""
    params = {"url": post_url}
    try:
        response = requests.get(URL_INSTAGRAM_POST, headers=HEADERS, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"    ❌ Error al obtener detalles del post {post_url}: {e}")
        return None

def extract_data_from_post(api_data, original_url):
    """Extrae y mapea los datos de la respuesta de la API al formato del CSV."""
    post_data = api_data.get("data", {}).get("xdt_shortcode_media", {})
    if not post_data:
        return None

    owner = post_data.get("owner", {})

    tagged_users_list = [
        edge['node']['user']['username']
        for edge in post_data.get('edge_media_to_tagged_user', {}).get('edges', [])
        if 'node' in edge and 'user' in edge['node'] and 'username' in edge['node']['user']
    ]
    usertags_str = ", ".join(tagged_users_list) if tagged_users_list else "N/A"

    # --- 👇 LÍNEA CORREGIDA PARA MANEJAR CAPTIONS VACÍOS 👇 ---
    caption_edges = post_data.get("edge_media_to_caption", {}).get("edges", [])
    post_caption = caption_edges[0].get("node", {}).get("text", "") if caption_edges else ""

    return {
        'timestamp_registro': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'username': owner.get("username", "N/A"),
        'followers_count': owner.get("edge_followed_by", {}).get("count"),
        'posts_count_total': owner.get("edge_owner_to_timeline_media", {}).get("count"),
        'following_count': None,
        'post_id': post_data.get("id"),
        'post_created_at_str': datetime.fromtimestamp(post_data.get("taken_at_timestamp", 0)).strftime("%Y-%m-%d %H:%M:%S"),
        'post_shortcode': post_data.get("shortcode"),
        'post_url': original_url,
        'likes_count': post_data.get("edge_media_preview_like", {}).get("count"),
        'comments_count': post_data.get("edge_media_to_parent_comment", {}).get("count"),
        'post_caption': post_caption,
        'media_type': 2 if post_data.get("is_video") else 1,
        'play_count': post_data.get("video_play_count"),
        'usertags': usertags_str,
        'post_transcript': ""
    }

# --- FUNCIÓN PRINCIPAL ---

def main():
    """
    Orquesta todo el proceso: leer perfiles, buscar menciones,
    extraer datos y guardar en CSV.
    """
    print("🚀 Iniciando el script para encontrar menciones...")

    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
        print(f"📁 Carpeta '{OUTPUT_FOLDER}' creada.")

    try:
        with open(INPUT_PROFILES_FILE, 'r', encoding='utf-8') as f:
            profiles = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"❌ Error: No se encontró el archivo '{INPUT_PROFILES_FILE}'.")
        return

    print(f"👥 Se procesarán {len(profiles)} perfiles.")

    for profile in profiles:
        print(f"\n--- Procesando perfil: {profile} ---")

        mention_urls = search_google_for_mentions(profile)

        if not mention_urls:
            print(f"  ℹ️ No se encontraron menciones para '{profile}' o hubo un error. Pasando al siguiente.")
            continue

        all_mentions_data = []

        for i, url in enumerate(mention_urls):
            print(f"    ({i+1}/{len(mention_urls)}) Obteniendo datos de: {url}")
            post_details_json = get_post_details(url)

            if post_details_json:
                extracted_data = extract_data_from_post(post_details_json, url)
                if extracted_data:
                    all_mentions_data.append(extracted_data)

            time.sleep(1.5)

        if all_mentions_data:
            today_str = datetime.now().strftime("%Y%m%d")
            filename = f"{today_str}_{profile}.csv"
            filepath = os.path.join(OUTPUT_FOLDER, filename)

            try:
                with open(filepath, 'w', newline='', encoding='utf-8') as f_out:
                    writer = csv.DictWriter(f_out, fieldnames=CSV_HEADER)
                    writer.writeheader()
                    writer.writerows(all_mentions_data)
                print(f"  💾 ¡Éxito! Resultados guardados en '{filepath}'")
            except IOError as e:
                print(f"  ❌ Error al guardar el archivo CSV: {e}")
        else:
            print(f"  ℹ️ No se pudo extraer información detallada de ninguna mención para '{profile}'.")

    print("\n🎉 ¡Proceso completado para todos los perfiles!")

if __name__ == "__main__":
    main()