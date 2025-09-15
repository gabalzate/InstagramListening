import pandas as pd
import requests
import os
import time
from datetime import datetime, timedelta # Se importa datetime para manejar las fechas

# Asumimos que config.py contiene: SCRAPE_API_KEY
try:
    from config import SCRAPE_API_KEY
except ImportError:
    print("❌ Error: Asegúrate de que tu archivo 'config.py' existe y contiene la variable SCRAPE_API_KEY.")
    exit()

# --- CONFIGURACIÓN ---
DATA_FILE = "base_de_datos_instagram.csv"
API_URL = "https://api.scrapecreators.com/v1/instagram/post"
HEADERS = {
    "x-api-key": SCRAPE_API_KEY,
    "accept": "application/json"
}
BATCH_SIZE = 10 # Procesar y guardar en lotes de 10

# --- FUNCIÓN DE API ---
def get_live_post_data(post_url):
    """Llama al API para obtener los datos más recientes de un post."""
    params = {"url": post_url}
    try:
        response = requests.get(API_URL, headers=HEADERS, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"  ⚠️ Error de API para {post_url}: {e}")
        return None

# --- FUNCIÓN PRINCIPAL ---
def update_instagram_data():
    """Script principal para actualizar los datos del CSV."""
    print("🚀 Iniciando el script de actualización de datos...")

    try:
        df = pd.read_csv(DATA_FILE)
    except FileNotFoundError:
        print(f"❌ Error: No se encontró el archivo '{DATA_FILE}'.")
        return

    # --- 👇 LÓGICA DE FILTRADO POR FECHAS AÑADIDA AQUÍ 👇 ---

    # 1. Asegurarse de que la columna de fecha esté en formato datetime
    #    'coerce' convertirá fechas no válidas en NaT (Not a Time)
    df['post_created_at_str'] = pd.to_datetime(df['post_created_at_str'], errors='coerce')

    # 2. Calcular el rango de fechas
    today = datetime.now()
    one_week_ago = today - timedelta(weeks=1)
    three_weeks_ago = today - timedelta(weeks=3)

    # 3. Filtrar el DataFrame para obtener solo las publicaciones en ese rango
    df_to_update = df[
        (df['post_created_at_str'] >= three_weeks_ago) &
        (df['post_created_at_str'] <= one_week_ago)
    ].copy() # .copy() para evitar advertencias de Pandas

    # -----------------------------------------------------------

    total_rows_to_update = len(df_to_update)
    if total_rows_to_update == 0:
        print("✅ No se encontraron publicaciones en el rango de fechas especificado (hace 1-3 semanas). No se necesita actualizar nada.")
        return

    print(f"📄 Se procesarán {total_rows_to_update} publicaciones que están en el rango de fechas.")

    # Iterar por el DataFrame FILTRADO en lotes
    for start_index in range(0, total_rows_to_update, BATCH_SIZE):
        end_index = min(start_index + BATCH_SIZE, total_rows_to_update)
        print(f"\n--- Procesando lote: {start_index + 1} a {end_index} de las publicaciones filtradas ---")

        batch_df = df_to_update.iloc[start_index:end_index]

        for index, row in batch_df.iterrows():
            post_url = row['post_url']
            print(f"  🔗 Verificando: {post_url}")

            live_data = get_live_post_data(post_url)

            if live_data and "data" in live_data and "xdt_shortcode_media" in live_data["data"]:
                post_details = live_data["data"]["xdt_shortcode_media"]

                new_likes = post_details.get("edge_media_preview_like", {}).get("count")
                new_comments = post_details.get("edge_media_to_parent_comment", {}).get("count")
                new_plays = post_details.get("video_play_count")

                # Actualizar el DataFrame ORIGINAL en la fila correcta
                if new_likes is not None:
                    df.loc[index, 'likes_count'] = new_likes
                if new_comments is not None:
                    df.loc[index, 'comments_count'] = new_comments
                if row['media_type'] == 2 and new_plays is not None:
                    df.loc[index, 'play_count'] = new_plays

                print(f"    ✅ Actualizado: Likes={new_likes}, Comentarios={new_comments}, Reproducciones={new_plays or 'N/A'}")
            else:
                print("    ❌ No se pudo obtener la información.")

            time.sleep(1.5)

        # Guardar el progreso
        try:
            df.to_csv(DATA_FILE, index=False)
            print(f"💾 Progreso guardado. El archivo '{DATA_FILE}' ha sido actualizado.")
        except IOError as e:
            print(f"❌ Error al guardar el archivo: {e}")

    print("\n🎉 ¡Proceso de actualización completado!")

if __name__ == "__main__":
    update_instagram_data()