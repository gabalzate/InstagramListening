import requests
import csv
import os
import time

# Asumimos que config.py contiene: SCRAPE_API_KEY = "tu_clave"
from config import SCRAPE_API_KEY

# Rutas a los archivos
INPUT_CSV_FILE = "base_de_datos_instagram.csv"
OUTPUT_CSV_FILE = "base_de_datos_instagram_temp.csv"

# Endpoint de la API
BASE_URL_TRANSCRIPT = "https://api.scrapecreators.com/v2/instagram/media/transcript"

HEADERS = {
    "x-api-key": SCRAPE_API_KEY,
    "accept": "application/json"
}

def get_transcript(post_url):
    """Realiza una llamada a la API para obtener la transcripción de un post."""
    params = {"url": post_url}
    try:
        response = requests.get(BASE_URL_TRANSCRIPT, headers=HEADERS, params=params)
        response.raise_for_status()
        transcripts = response.json().get('transcripts', [])
        if transcripts and 'text' in transcripts[0]:
            return transcripts[0]['text']
        else:
            return "No se encontró transcripción."
    except requests.exceptions.RequestException as e:
        print(f"  ❌ Error en la API de transcripción para {post_url}: {e}")
        return "Error en la transcripción."

def process_transcripts():
    """
    Lee el archivo CSV, procesa las transcripciones pendientes y
    guarda el resultado en un nuevo archivo CSV.
    """
    if not os.path.exists(INPUT_CSV_FILE):
        print(f"❌ Error: No se encontró el archivo {INPUT_CSV_FILE}.")
        return

    updated_rows = []
    transcriptions_to_process = 0

    with open(INPUT_CSV_FILE, 'r', newline='', encoding='utf-8') as f_in:
        reader = csv.reader(f_in)
        header = next(reader)
        updated_rows.append(header)

        for row in reader:
            # La columna de transcripción está en el último índice de tu encabezado
            if len(row) > 12 and row[12] == 'N/A':
                transcriptions_to_process += 1
                post_url = row[8] # La URL está en el índice 8
                print(f"🔎 Transcribiendo post pendiente para {row[1]} ({row[7]})...")
                transcript = get_transcript(post_url)
                row[12] = transcript
                print(f"  ✅ Transcripción obtenida. Pausando 15 segundos...")
                time.sleep(15) # Pausa entre llamadas a la API de transcripción

            updated_rows.append(row)

    if transcriptions_to_process == 0:
        print("\n✅ No se encontraron transcripciones pendientes. ¡Todo está actualizado!")
        return

    with open(OUTPUT_CSV_FILE, 'w', newline='', encoding='utf-8') as f_out:
        writer = csv.writer(f_out)
        writer.writerows(updated_rows)

    # Reemplazar el archivo original con el nuevo
    os.replace(OUTPUT_CSV_FILE, INPUT_CSV_FILE)
    print(f"\n🎉 Proceso de transcripción completado. Se actualizaron {transcriptions_to_process} posts y el archivo '{INPUT_CSV_FILE}' se ha renovado.")

if __name__ == "__main__":
    process_transcripts()