import requests
import csv
import os
import time
import re # ¡Importante añadir esta librería!

# Asumimos que config.py contiene: SCRAPE_API_KEY = "tu_clave"
from config import SCRAPE_API_KEY

# Rutas a los archivos
INPUT_CSV_FILE = "base_de_datos_instagram.csv"
OUTPUT_CSV_FILE_TEMP = "base_de_datos_instagram_temp.csv"

# Endpoint de la API
BASE_URL_TRANSCRIPT = "https://api.scrapecreators.com/v2/instagram/media/transcript"

HEADERS = {
    "x-api-key": SCRAPE_API_KEY,
    "accept": "application/json"
}

BATCH_SIZE = 10  # Guardar el progreso cada 10 posts

def limpiar_transcripcion_srt(texto_srt: str) -> str:
    """
    Toma un texto en formato de subtítulos (SRT), lo limpia y extrae únicamente el diálogo.
    """
    if not isinstance(texto_srt, str):
        return ""
    # Elimina las líneas de tiempo (ej. 00:00:00,000 --> 00:00:04,581)
    texto_sin_tiempos = re.sub(r'\d{2}:\d{2}:\d{2},\d{3} --> \d{2}:\d{2}:\d{2},\d{3}', '', texto_srt)
    # Elimina los números de secuencia que están en su propia línea
    texto_sin_numeros = re.sub(r'^\d+\s*$', '', texto_sin_tiempos, flags=re.MULTILINE)
    # Une todo en un texto limpio y plano
    texto_limpio = ' '.join(texto_sin_numeros.strip().split())
    return texto_limpio

def get_transcript(post_url):
    """
    Realiza una llamada a la API, maneja múltiples formatos de respuesta y procesa carruseles.
    """
    params = {"url": post_url}
    try:
        response = requests.get(BASE_URL_TRANSCRIPT, headers=HEADERS, params=params, timeout=45) # Añadido timeout
        response.raise_for_status()
        data = response.json()
        
        found_transcripts = []
        
        # Itera sobre TODOS los items en la lista de transcripciones (para manejar carruseles)
        for item in data.get('transcripts', []):
            if 'text' in item and item['text']:
                found_transcripts.append(item['text'])
            elif 'transcript' in item and item['transcript']:
                texto_limpio = limpiar_transcripcion_srt(item['transcript'])
                found_transcripts.append(texto_limpio)
        
        # Si encontramos alguna transcripción, las unimos. Si no, devolvemos el mensaje.
        if found_transcripts:
            return " ".join(found_transcripts)
        else:
            return "No se encontró transcripción."
            
    except requests.exceptions.RequestException as e:
        print(f"  ❌ Error en la API de transcripción para {post_url}: {e}")
        return "Error en la transcripción."
    except ValueError:
        print(f"  ❌ Error al decodificar JSON para {post_url}. Respuesta no válida.")
        return "Error en la transcripción."

def process_transcriptions():
    """
    Función principal que lee el CSV, procesa las transcripciones pendientes y guarda el resultado.
    """
    if not os.path.exists(INPUT_CSV_FILE):
        print(f"Error: El archivo de entrada '{INPUT_CSV_FILE}' no existe.")
        return

    rows_to_process = []
    with open(INPUT_CSV_FILE, 'r', newline='', encoding='utf-8') as f_in:
        reader = csv.reader(f_in)
        header = next(reader)
        # Asegurarse de que las columnas existen
        if 'post_url' not in header or 'post_transcript' not in header or 'media_type' not in header:
            print("Error: El CSV debe contener las columnas 'post_url', 'post_transcript' y 'media_type'.")
            return
        
        url_index = header.index('post_url')
        transcript_index = header.index('post_transcript')
        media_type_index = header.index('media_type') # <-- Obtenemos el índice de media_type

        for row in reader:
            rows_to_process.append(row)

    transcriptions_processed_count = 0
    total_rows = len(rows_to_process)
    
    print("🚀 Iniciando proceso de transcripción...")

    for i, row in enumerate(rows_to_process):
        # --- 👇 LÓGICA DE FILTRADO MEJORADA 👇 ---
        
        is_video = row[media_type_index].strip() == '2'
        current_transcript = row[transcript_index]
        needs_processing = not current_transcript or current_transcript.strip() in ["N/A", "No se encontró transcripción.", ""]

        # Solo procesar si es un video Y necesita transcripción
        if is_video and needs_processing:
            post_url = row[url_index]
            print(f"({i+1}/{total_rows}) 💬 Procesando video: {post_url}")
            
            new_transcript = get_transcript(post_url)
            row[transcript_index] = new_transcript
            
            print(f"  ➡️ Resultado: {new_transcript[:80]}...")
            transcriptions_processed_count += 1
            
            # Pausa para no saturar la API
            time.sleep(1)

            # Guardar progreso cada BATCH_SIZE
            if transcriptions_processed_count > 0 and transcriptions_processed_count % BATCH_SIZE == 0:
                print(f"\n✅ Lote de {BATCH_SIZE} posts completado. Guardando progreso...\n")
                
                with open(OUTPUT_CSV_FILE_TEMP, 'w', newline='', encoding='utf-8') as f_out:
                    writer = csv.writer(f_out)
                    writer.writerow(header)
                    writer.writerows(rows_to_process)
                
                os.replace(OUTPUT_CSV_FILE_TEMP, INPUT_CSV_FILE)

    # Guardado final
    if transcriptions_processed_count > 0:
        print("\n✅ Proceso finalizado. Guardando los últimos cambios...")
        with open(OUTPUT_CSV_FILE_TEMP, 'w', newline='', encoding='utf-8') as f_out:
            writer = csv.writer(f_out)
            writer.writerow(header)
            writer.writerows(rows_to_process)
        
        os.replace(OUTPUT_CSV_FILE_TEMP, INPUT_CSV_FILE)
        print(f"🎉 ¡Éxito! Se actualizaron {transcriptions_processed_count} posts en '{INPUT_CSV_FILE}'.")
    else:
        print("\n✅ No se encontraron videos pendientes de transcripción. ¡Todo está actualizado!")


if __name__ == "__main__":
    process_transcriptions()
