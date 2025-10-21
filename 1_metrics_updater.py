import requests
import csv
import os
import time
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from io import StringIO

# Asumimos que config.py contiene: SCRAPE_API_KEY
try:
    from config import SCRAPE_API_KEY
except ImportError:
    print("❌ Error: Asegúrate de que tu archivo 'config.py' existe y contiene la variable SCRAPE_API_KEY.")
    exit()

# --- CONFIGURACIÓN ---
MAIN_DATA_FILE = "base_de_datos_instagram.csv"
OUTPUT_CSV_TEMP = "base_de_datos_instagram_temp.csv"
BATCH_SIZE = 5  # Número de posts a actualizar antes de guardar el progreso

# --- Endpoints de la API ---
BASE_URL_POST = "https://api.scrapecreators.com/v1/instagram/post"

HEADERS = {
    "x-api-key": SCRAPE_API_KEY,
    "accept": "application/json"
}

def get_post_metrics(post_url):
    """Obtiene likes, comentarios y plays para una URL específica."""
    params = {"url": post_url}
    try:
        response = requests.get(BASE_URL_POST, headers=HEADERS, params=params)
        response.raise_for_status()
        data = response.json().get('data', {}).get('xdt_shortcode_media', {})
        
        # Extracción robusta de las métricas
        likes = data.get('edge_media_preview_like', {}).get('count', 0)
        comments = data.get('edge_media_to_parent_comment', {}).get('count', 0)
        
        # El campo puede ser 'video_play_count' o 'video_view_count'
        plays = data.get('video_play_count') or data.get('video_view_count', 0)
        
        # Si no es video, plays debe ser 0
        if not data.get('is_video'):
            plays = 0
            
        return int(likes), int(comments), int(plays)
        
    except requests.exceptions.RequestException as e:
        print(f"  ❌ Error de API al obtener métricas para {post_url}: {e}")
        return None
    except Exception as e:
        print(f"  ❌ Error al procesar la respuesta para {post_url}: {e}")
        return None

def update_metrics_in_csv():
    """Orquesta la actualización de métricas para los posts más recientes."""
    
    print("🚀 Iniciando el proceso de actualización de métricas...")

    # --- 1. Definición del Rango de Actualización ---
    if not os.path.exists(MAIN_DATA_FILE):
        print(f"❌ Error: No se encontró el archivo de datos principal: {MAIN_DATA_FILE}.")
        return

    try:
        # Carga del DataFrame y forzar la fecha a datetime
        df_full = pd.read_csv(MAIN_DATA_FILE, dtype={'post_url': str})
        df_full['post_created_at_str'] = pd.to_datetime(df_full['post_created_at_str'], errors='coerce')
    except Exception as e:
        print(f"❌ Error al cargar o preparar el DataFrame: {e}")
        return

    # Quitar filas sin fecha válida
    df_valid = df_full.dropna(subset=['post_created_at_str']).copy()
    
    if df_valid.empty:
        print("⚠️ No hay posts válidos con fecha de creación. Finalizando.")
        return

    # Paso 1.2 & 1.3: Determinar la fecha de inicio y el umbral de 7 días
    max_creation_date = df_valid['post_created_at_str'].max()
    start_update_date = max_creation_date - timedelta(days=7)
    
    print(f"📅 Fecha de creación más reciente en la base de datos: {max_creation_date.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📅 Se actualizarán posts creados desde: {start_update_date.strftime('%Y-%m-%d %H:%M:%S')} (últimos 7 días de creación)")

    # Paso 1.4: Filtrar el Subconjunto a Actualizar
    df_to_update = df_valid[df_valid['post_created_at_str'] >= start_update_date].copy()
    
    # 2. Mapeo de URLs Únicas para la API
    # Obtener URLs únicas para evitar llamadas repetidas por el mismo post
    urls_to_update = df_to_update['post_url'].unique()
    total_unique_posts = len(urls_to_update)
    
    if total_unique_posts == 0:
        print("✅ No se encontraron posts nuevos en el rango para actualizar. Finalizando.")
        return

    print(f"\nTotal de posts únicos a actualizar: {total_unique_posts}")

    # 3. Bucle de Actualización (Petición por Post)
    updates_map = {}
    
    # Leer el CSV completo de forma nativa para la escritura (más seguro que Pandas para series de tiempo)
    with open(MAIN_DATA_FILE, 'r', newline='', encoding='utf-8') as f:
        csv_content = f.read()
    all_csv_rows = list(csv.reader(StringIO(csv_content)))
    header = all_csv_rows[0]

    # Identificar índices de las columnas a actualizar
    try:
        # En tu estructura: likes_count(9), comments_count(10), play_count(13)
        IDX_URL = header.index('post_url')
        IDX_LIKES = header.index('likes_count')
        IDX_COMMENTS = header.index('comments_count')
        IDX_PLAYS = header.index('play_count')
    except ValueError:
        print("❌ Error: Una o más columnas clave (post_url, likes_count, etc.) faltan en el CSV.")
        return

    print("\n--- Iniciando Peticiones a la API y Mapeo ---")
    
    for i, url in enumerate(urls_to_update):
        print(f"[{i+1}/{total_unique_posts}] 🔎 Obteniendo métricas para: {url}")
        metrics = get_post_metrics(url)
        
        if metrics is not None:
            likes, comments, plays = metrics
            updates_map[url] = {'likes': likes, 'comments': comments, 'plays': plays}
            print(f"  ✅ Actualizado en mapa: L={likes:,}, C={comments:,}, P={plays:,}")
        else:
             print("  ⚠️ No se pudieron obtener métricas. Post omitido.")
        
        time.sleep(5) # Pausa entre peticiones

    # 4. Reemplazo de Datos y Guardado Final (CSV)
    print("\n--- Aplicando actualizaciones al archivo CSV ---")

    updates_applied = 0
    # Iterar sobre las filas del CSV, comenzando después del encabezado
    for i in range(1, len(all_csv_rows)):
        row = all_csv_rows[i]
        
        # Si la fila tiene suficientes columnas
        if len(row) > IDX_URL:
            row_url = row[IDX_URL]
            
            if row_url in updates_map:
                updates = updates_map[row_url]
                
                # Solo actualizar si la métrica es diferente (opcional, pero limpio)
                if row[IDX_LIKES] != str(updates['likes']) or \
                   row[IDX_COMMENTS] != str(updates['comments']) or \
                   row[IDX_PLAYS] != str(updates['plays']):
                    
                    row[IDX_LIKES] = updates['likes']
                    row[IDX_COMMENTS] = updates['comments']
                    row[IDX_PLAYS] = updates['plays']
                    
                    updates_applied += 1
                
                # Lógica de guardado por lotes (cada 5 posts únicos actualizados)
                if updates_applied % BATCH_SIZE == 0 and updates_applied > 0:
                    print(f"  💾 Guardando progreso... Lote de {BATCH_SIZE} filas actualizado.")
                    
                    # Escribir todas las filas actualizadas hasta el momento en un archivo temporal
                    with open(OUTPUT_CSV_TEMP, 'w', newline='', encoding='utf-8') as f_out:
                        writer = csv.writer(f_out)
                        writer.writerows(all_csv_rows)
                    
                    # Reemplazar el archivo original con el temporal
                    os.replace(OUTPUT_CSV_TEMP, MAIN_DATA_FILE)
                    
                    # Recargar el contenido del CSV para continuar (necesario si se usa un iterador)
                    # Pero en este caso, como estamos trabajando con la lista 'all_csv_rows' en memoria, solo actualizamos el disco.
                    
                    print(f"  ✅ Progreso guardado. Total actualizado: {updates_applied}")

    # Guardar el lote final si queda algo sin guardar
    if updates_applied % BATCH_SIZE != 0 or updates_applied == 0:
         with open(OUTPUT_CSV_TEMP, 'w', newline='', encoding='utf-8') as f_out:
             writer = csv.writer(f_out)
             writer.writerows(all_csv_rows)
         os.replace(OUTPUT_CSV_TEMP, MAIN_DATA_FILE)

    print(f"\n🎉 ¡Proceso de actualización completado! Se actualizaron un total de {updates_applied} filas.")

if __name__ == '__main__':
    update_metrics_in_csv()
