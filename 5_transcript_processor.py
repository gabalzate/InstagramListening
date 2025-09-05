import requests
import csv
import os
import time

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

BATCH_SIZE =10  # Guardar el progreso cada 100 posts

def get_transcript(post_url):
    """Realiza una llamada a la API para obtener la transcripción de un post."""
    params = {"url": post_url}
    try:
        response = requests.get(BASE_URL_TRANSCRIPT, headers=HEADERS, params=params)
        response.raise_for_status()
        data = response.json()
        
        transcripts = data.get('transcripts')
        if transcripts and isinstance(transcripts, list) and len(transcripts) > 0 and 'text' in transcripts[0]:
            return transcripts[0]['text']
        else:
            return "No se encontró transcripción."
            
    except requests.exceptions.RequestException as e:
        print(f"  ❌ Error en la API de transcripción para {post_url}: {e}")
        return "Error en la transcripción."
    except ValueError:
        print("  ❌ Error al decodificar la respuesta JSON.")
        return "Error al decodificar JSON."

def process_transcripts():
    """
    Lee el archivo CSV, procesa las transcripciones pendientes en lotes y
    guarda el resultado.
    """
    if not os.path.exists(INPUT_CSV_FILE):
        print(f"❌ Error: No se encontró el archivo {INPUT_CSV_FILE}.")
        return

    rows_to_process = []
    header = []

    with open(INPUT_CSV_FILE, 'r', newline='', encoding='utf-8') as f_in:
        reader = csv.reader(f_in)
        header = next(reader)
        for row in reader:
            rows_to_process.append(row)
    
    transcriptions_processed_count = 0
    total_rows = len(rows_to_process)
    
    print(f"Total de filas para procesar: {total_rows}")
    
    for i, row in enumerate(rows_to_process):
        # La columna de transcripción está en el índice 15
        if len(row) > 15 and row[15] == 'N/A':
            post_url = row[8] # La URL está en el índice 8
            print(f"🔎 Procesando post {i+1}/{total_rows} - {row[1]} ({row[7]})...")
            
            transcript = get_transcript(post_url)
            row[15] = transcript
            transcriptions_processed_count += 1
            
            # Pausa para evitar saturar la API
            time.sleep(15)

            # Guardar el progreso cada BATCH_SIZE
            if transcriptions_processed_count % BATCH_SIZE == 0:
                print(f"\n✅ Lote de {BATCH_SIZE} posts completado. Guardando progreso...")
                
                with open(OUTPUT_CSV_FILE_TEMP, 'w', newline='', encoding='utf-8') as f_out:
                    writer = csv.writer(f_out)
                    writer.writerow(header)
                    writer.writerows(rows_to_process)
                
                os.replace(OUTPUT_CSV_FILE_TEMP, INPUT_CSV_FILE)
                print(f"🎉 Progreso guardado. El archivo '{INPUT_CSV_FILE}' ha sido actualizado.")

    # Guardar el último lote si no se ha guardado ya
    if transcriptions_processed_count > 0 and (transcriptions_processed_count % BATCH_SIZE != 0 or total_rows < BATCH_SIZE):
        print("\n✅ Último lote completado. Guardando progreso final...")
        with open(OUTPUT_CSV_FILE_TEMP, 'w', newline='', encoding='utf-8') as f_out:
            writer = csv.writer(f_out)
            writer.writerow(header)
            writer.writerows(rows_to_process)
        
        os.replace(OUTPUT_CSV_FILE_TEMP, INPUT_CSV_FILE)
        
    if transcriptions_processed_count == 0:
        print("\n✅ No se encontraron transcripciones pendientes. ¡Todo está actualizado!")
    else:
        print(f"\n🎉 Proceso de transcripción completado. Se actualizaron {transcriptions_processed_count} posts y el archivo '{INPUT_CSV_FILE}' se ha renovado.")

if __name__ == "__main__":
    process_transcripts()
