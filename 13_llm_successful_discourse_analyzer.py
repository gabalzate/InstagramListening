import pandas as pd
import google.generativeai as genai
import os
import time

# Asumimos que config.py contiene: GEMINI_API_KEY
try:
    from config import GEMINI_API_KEY
except ImportError:
    print("❌ Error: Asegúrate de que tu archivo 'config.py' existe y contiene la variable GEMINI_API_KEY.")
    exit()

# --- CONFIGURACIÓN ---
INPUT_FILE = "output/b_top10_videos_likes.csv"
OUTPUT_FOLDER = "analisis_discurso_exitoso"

# --- INSTRUCCIÓN MAESTRA (MASTER PROMPT) PARA ANÁLISIS ESTRATÉGICO ---
# Esta instrucción está diseñada para que el LLM actúe como un estratega y
# destile las claves de la comunicación efectiva.
MASTER_PROMPT = """
Actúa como un estratega de comunicación política de élite. Te proporcionaré el texto de los 10 videos de Instagram más exitosos (con más "me engagement") de un candidato presidencial de Colombia.

Tu misión es analizar este "corpus del éxito" para destilar las claves de su comunicación más potente y efectiva. Genera un reporte de una página en Español con la siguiente estructura:

**1. Resumen del Patrón de Éxito:**
En un párrafo, describe el patrón o fórmula general que define a sus videos más exitosos. ¿Se basan en la emoción, en datos duros, en la confrontación, en la cercanía?

**2. Tácticas de Comunicación Clave:**
Identifica de 3 a 4 tácticas o técnicas de comunicación específicas que se repiten en este contenido. Por ejemplo:
    * **Narrativa Personal:** ¿Usa historias personales o anécdotas para conectar?
    * **Contraste y Confrontación:** ¿Define claramente a un "enemigo" o una idea opuesta?
    * **Apelación a la Esperanza/Miedo:** ¿Qué emoción principal busca evocar?
    * **Llamados a la Acción Claros:** ¿Pide a su audiencia que haga algo específico?

**3. Temas de Mayor Resonancia:**
Basado en este contenido, ¿cuáles son los temas específicos que, en la voz de este candidato, generan la mayor interacción con su audiencia?

**4. Perfil de la Audiencia Implícita:**
A juzgar por el lenguaje y los temas de su contenido más exitoso, ¿a qué tipo de audiencia le está hablando principalmente? ¿Son jóvenes, indecisos, su base más leal?

**5. Conclusión Estratégica:**
¿Cuál es la clave fundamental del poder de su discurso? ¿Qué lo hace diferente o potente en comparación con un discurso político tradicional?

---
Aquí está el texto de los videos más exitosos del candidato:

{corpus_text}
"""

# --- FUNCIÓN DE ANÁLISIS ---

def analyze_text_with_gemini(text_corpus, model_name='gemini-2.5-flash'):
    """Envía el texto del corpus a Gemini y devuelve el reporte generado."""

    prompt = MASTER_PROMPT.format(corpus_text=text_corpus)

    try:
        model = genai.GenerativeModel(model_name)
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        print(f"  ❌ Error al contactar la API de Gemini: {e}")
        return "No se pudo generar el reporte debido a un error en la API."

# --- FUNCIÓN PRINCIPAL ---

def main():
    """Orquesta el análisis estratégico de los videos más exitosos."""

    print("🚀 Iniciando el análisis estratégico del discurso de alto impacto...")

    # Configurar el API de Gemini
    try:
        genai.configure(api_key=GEMINI_API_KEY)
    except Exception as e:
        print(f"❌ Error configurando la API de Gemini. Revisa tu API Key. Error: {e}")
        return

    # Crear carpeta de salida
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
        print(f"📁 Carpeta '{OUTPUT_FOLDER}' creada.")

    # Cargar los datos de los videos más exitosos
    try:
        df = pd.read_csv(INPUT_FILE)
    except FileNotFoundError:
        print(f"❌ No se encontró el archivo '{INPUT_FILE}'. Asegúrate de que existe en la carpeta 'output'.")
        return

    # Agrupar por candidato
    grouped = df.groupby('username')

    print(f"💬 Se analizará el contenido exitoso de {len(grouped)} perfiles.")

    # Bucle de análisis
    for candidate_name, group in grouped:
        print(f"\n--- Analizando a: {candidate_name} ---")

        # Crear el "corpus del éxito"
        group['full_text'] = group['post_caption'].fillna('') + "\n\n" + group['post_transcript'].fillna('')
        corpus_of_success = "\n\n--- NUEVO VIDEO ---\n\n".join(group['full_text'])

        if not corpus_of_success.strip():
            print("  ⚠️  No hay texto en las publicaciones de este candidato. Saltando...")
            continue

        # Llamar a Gemini para el análisis estratégico
        print("  🤖 Enviando corpus del éxito a Gemini para análisis estratégico...")
        reporte_texto = analyze_text_with_gemini(corpus_of_success)

        # Guardar el reporte
        report_filename = f"analisis_exitoso_{candidate_name}.txt"
        report_filepath = os.path.join(OUTPUT_FOLDER, report_filename)
        with open(report_filepath, 'w', encoding='utf-8') as f_out:
            f_out.write(reporte_texto)

        print(f"  📄 Reporte estratégico guardado en '{report_filepath}'")

        # Pausa para no exceder los límites de la API
        time.sleep(2)

    print(f"\n🎉 ¡Proceso completado! Todos los análisis están en la carpeta '{OUTPUT_FOLDER}'.")

if __name__ == "__main__":
    main()