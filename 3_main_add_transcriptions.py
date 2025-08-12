import requests
import csv
import os
import time  # Importamos la librería para agregar un delay
from config import SCRAPE_API_KEY

# Variables de configuración
INSTAGRAM_USERNAME = "vickydavilah"
BASE_URL_TRANSCRIPT = "https://api.scrapecreators.com/v2/instagram/media/transcript"
INPUT_CSV_FILE = f"posts_{INSTAGRAM_USERNAME}.csv"
OUTPUT_CSV_FILE = f"posts_{INSTAGRAM_USERNAME}_with_transcripts.csv"

HEADERS = {
    "x-api-key": SCRAPE_API_KEY,
    "accept": "application/json"
}

def get_transcript(post_url):
    """
    Realiza una llamada a la API para obtener la transcripción de un post.
    """
    print(f"  > Obteniendo transcripción para: {post_url}")
    parameters = {"url": post_url}
    try:
        response = requests.get(BASE_URL_TRANSCRIPT, headers=HEADERS, params=parameters)
        response.raise_for_status()
        data = response.json()

        # La transcripción está en 'transcripts', que es un array
        transcripts = data.get('transcripts', [])
        if transcripts and 'text' in transcripts[0]:
            return transcripts[0]['text']
        else:
            return "No se encontró transcripción."

    except requests.exceptions.RequestException as e:
        print(f"  ❌ Error en la API: {e}")
        return "Error en la transcripción."
    except json.JSONDecodeError:
        print("  ❌ Error al decodificar la respuesta JSON.")
        return "Error al decodificar JSON."

def process_posts_for_transcripts():
    """
    Lee el archivo CSV de posts, obtiene las transcripciones y
    guarda la nueva información en un archivo CSV de salida.
    """
    # Usamos listas para almacenar las filas del CSV
    posts_with_transcripts = []

    try:
        # Abrir el archivo CSV original para lectura
        with open(INPUT_CSV_FILE, 'r', newline='', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            header = next(reader)  # Leer el encabezado

            # Añadir la nueva columna al encabezado
            if 'transcript' not in header:
                header.append('transcript')

            posts_with_transcripts.append(header)

            # Iterar sobre cada fila del archivo
            for row in reader:
                # La URL del post es la cuarta columna (índice 3)
                post_url = row[3]
                transcript_text = get_transcript(post_url)

                # Agregar la transcripción como una nueva columna
                row.append(transcript_text)
                posts_with_transcripts.append(row)

                # Esperar 15 segundos para evitar sobrecargar la API
                # Este valor se puede ajustar si es necesario
                time.sleep(15)

    except FileNotFoundError:
        print(f"❌ Error: No se encontró el archivo {INPUT_CSV_FILE}.")
        return
    except IndexError:
        print("❌ Error: Formato de archivo CSV incorrecto. No se pudo encontrar la URL del post.")
        return

    # Escribir los datos actualizados en un nuevo archivo CSV
    with open(OUTPUT_CSV_FILE, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        writer.writerows(posts_with_transcripts)

    print(f"\n✅ Proceso completado. El nuevo archivo con transcripciones es: {OUTPUT_CSV_FILE}")

if __name__ == "__main__":
    process_posts_for_transcripts()