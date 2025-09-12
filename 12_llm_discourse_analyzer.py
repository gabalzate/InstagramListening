import google.generativeai as genai
import os
import glob
import time

# Asumimos que config.py contiene: GEMINI_API_KEY
try:
    from config import GEMINI_API_KEY
except ImportError:
    print("❌ Error: Asegúrate de que tu archivo 'config.py' existe y contiene la variable GEMINI_API_KEY.")
    exit()

# --- CONFIGURACIÓN ---
INPUT_FOLDER = "reportes_discurso"
OUTPUT_FOLDER = "analisis_llm"

# --- INSTRUCCIÓN MAESTRA (MASTER PROMPT) PARA GEMINI ---
# Este es el cerebro del análisis. Guía al LLM para que actúe como un experto
# y nos dé un reporte estructurado y de alta calidad.
MASTER_PROMPT = """
Actúa como un analista político y de comunicación experto. A continuación, te proporcionaré el corpus de texto completo de todas las publicaciones de un candidato presidencial de Colombia en Instagram.

Tu tarea es leer y analizar profundamente este texto y generar un reporte conciso, de máximo una página en Español, que contenga las siguientes secciones claramente definidas:

**1. Perfil de Comunicación:**
Describe en uno o dos párrafos el estilo general de comunicación del candidato. ¿Es formal o informal? ¿Cercano o distante? ¿Usa un lenguaje técnico o popular?

**2. Temas Principales:**
Identifica y enumera los 3 a 5 temas más recurrentes en su discurso (ej. Seguridad, Economía, Educación, Corrupción, Medio Ambiente). Proporciona un breve ejemplo de cómo aborda cada tema.

**3. Tono y Sentimiento Dominante:**
¿Cuál es el tono general del discurso? ¿Es optimista, confrontacional, esperanzador, crítico, propositivo?

**4. Palabras Clave de Poder:**
Lista las palabras o frases cortas que el candidato repite estratégicamente para enmarcar su mensaje (ej. "cambio real", "mano dura", "justicia social", "futuro", "potencia de la vida").

**5. Conclusión Estratégica:**
En un párrafo final, resume la estrategia de comunicación general del candidato. ¿A qué audiencia parece estar hablándole y qué busca evocar con su discurso?

---
Aquí está el texto del candidato:

{corpus_text}
"""

# --- FUNCIÓN DE ANÁLISIS ---

def analyze_text_with_gemini(text_corpus):
    """Envía el texto del corpus a Gemini y devuelve el reporte generado."""

    # Construye el prompt final uniendo la instrucción maestra con el texto
    prompt = MASTER_PROMPT.format(corpus_text=text_corpus)

    try:
        model = genai.GenerativeModel('gemini-2.5-flash')
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        print(f"  ❌ Error al contactar la API de Gemini: {e}")
        return "No se pudo generar el reporte debido a un error en la API."

# --- FUNCIÓN PRINCIPAL ---

def main():
    """Orquesta todo el proceso de análisis con el LLM."""

    print("🚀 Iniciando el análisis de discurso con Gemini...")

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

    # Encontrar todos los archivos de corpus
    corpus_files = glob.glob(os.path.join(INPUT_FOLDER, 'corpus_texto_*.txt'))

    if not corpus_files:
        print(f"❌ No se encontraron archivos de corpus en la carpeta '{INPUT_FOLDER}'. Asegúrate de haber ejecutado el script anterior.")
        return

    print(f"💬 Se analizarán {len(corpus_files)} perfiles.")

    # Bucle de análisis
    for file_path in corpus_files:
        # Extraer el nombre del candidato del nombre del archivo
        filename = os.path.basename(file_path)
        candidate_name = filename.replace('corpus_texto_', '').replace('.txt', '')

        print(f"\n--- Analizando a: {candidate_name} ---")

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                corpus_text = f.read()

            if not corpus_text.strip():
                print("  ⚠️  El archivo de corpus está vacío. Saltando...")
                continue

            # Llamar a Gemini para el análisis
            print("  🤖 Enviando texto a Gemini para análisis... (esto puede tardar un momento)")
            reporte_texto = analyze_text_with_gemini(corpus_text)

            # Guardar el reporte
            report_filename = f"analisis_llm_{candidate_name}.txt"
            report_filepath = os.path.join(OUTPUT_FOLDER, report_filename)
            with open(report_filepath, 'w', encoding='utf-8') as f_out:
                f_out.write(reporte_texto)

            print(f"  📄 Reporte de análisis guardado en '{report_filepath}'")

            # Pausa para no exceder los límites de la API
            time.sleep(2)

        except Exception as e:
            print(f"  ❌ Error procesando el archivo {filename}: {e}")

    print(f"\n🎉 ¡Proceso completado! Todos los análisis están en la carpeta '{OUTPUT_FOLDER}'.")


if __name__ == "__main__":
    main()