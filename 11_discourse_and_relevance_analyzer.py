import pandas as pd
import os
import re
from wordcloud import WordCloud
import matplotlib.pyplot as plt

# --- CONFIGURACIÓN ---
MAIN_DATA_FILE = "base_de_datos_instagram.csv"
PROFILES_FILE = "perfiles_instagram.txt"
OUTPUT_FOLDER = "reportes_discurso"

STOPWORDS = set([
    'de', 'la', 'que', 'el', 'en', 'y', 'a', 'los', 'del', 'se', 'las', 'por', 'un',
    'para', 'con', 'no', 'una', 'su', 'al', 'lo', 'como', 'más', 'pero', 'sus',
    'le', 'ya', 'o', 'este', 'ha', 'me', 'si', 'sin', 'sobre', 'este', 'entre',
    'cuando', 'también', 'fue', 'ser', 'son', 'dos', 'así', 'desde', 'muy', 'hasta',
    'nos', 'mi', 'eso', 'qué', 'todo', 'todos', 'eres', 'soy', 'es', 'está', 'están'
])

# --- FUNCIONES AUXILIARES ---

def clean_text(text):
    """Limpia el texto eliminando URLs, menciones y caracteres no alfanuméricos."""
    text = re.sub(r'http\S+', '', text)
    text = re.sub(r'@\w+', '', text)
    text = re.sub(r'#\w+', '', text)
    text = re.sub(r'[^a-zA-ZáéíóúÁÉÍÓÚñÑ\s]', '', text)
    text = text.lower()
    return ' '.join([word for word in text.split() if word not in STOPWORDS and len(word) > 2])

def generate_wordcloud(text, candidate_name, output_folder):
    """Genera y guarda una nube de palabras a partir de un texto."""
    wordcloud = WordCloud(width=800, height=400, background_color='white', collocations=False).generate(text)

    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")

    filename = f"wordcloud_discurso_{candidate_name}.png"
    filepath = os.path.join(output_folder, filename)
    plt.savefig(filepath)
    plt.close()
    print(f"  🖼️  Nube de palabras guardada en '{filepath}'")

def add_labels_to_bars(ax, is_percentage=False):
    """Añade el valor como texto encima de cada barra en un gráfico."""
    for bar in ax.patches:
        y_value = bar.get_height()
        x_value = bar.get_x() + bar.get_width() / 2

        if is_percentage:
            label = f"{y_value:.2%}"
        else:
            label = f"{int(y_value):,}"

        ax.text(x_value, y_value, label, ha='center', va='bottom', fontsize=8, rotation=90)


# --- FUNCIÓN PRINCIPAL ---

def analyze_discourse_and_relevance():
    """Script principal que orquesta todo el análisis."""

    print("🚀 Iniciando el análisis de discurso, alcance y relevancia...")

    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
        print(f"📁 Carpeta '{OUTPUT_FOLDER}' creada.")

    try:
        df = pd.read_csv(MAIN_DATA_FILE)
        with open(PROFILES_FILE, 'r', encoding='utf-8') as f:
            candidates = [line.strip() for line in f if line.strip()]
    except FileNotFoundError as e:
        print(f"❌ Error: No se pudo encontrar un archivo necesario: {e}")
        return

    comparative_results = {}

    for candidate in candidates:
        print(f"\n--- Analizando a: {candidate} ---")

        df_candidate = df[df['username'] == candidate].copy()

        if df_candidate.empty:
            print(f"  ⚠️  No se encontraron publicaciones para {candidate}. Saltando...")
            continue

        df_candidate['full_text'] = df_candidate['post_caption'].fillna('') + " " + df_candidate['post_transcript'].fillna('')
        corpus_text = " ".join(df_candidate['full_text'])
        cleaned_corpus = clean_text(corpus_text)

        corpus_filename = f"corpus_texto_{candidate}.txt"
        corpus_filepath = os.path.join(OUTPUT_FOLDER, corpus_filename)
        with open(corpus_filepath, 'w', encoding='utf-8') as f_out:
            f_out.write(corpus_text)
        print(f"  📝 Corpus de texto guardado en '{corpus_filepath}'")

        if cleaned_corpus:
            generate_wordcloud(cleaned_corpus, candidate, OUTPUT_FOLDER)
        else:
            print("  ⚠️  No hay suficiente texto para generar una nube de palabras.")

        df_videos = df_candidate[df_candidate['media_type'] == 2].copy()
        total_video_reach = df_videos['play_count'].sum()

        if not df_videos.empty:
            df_videos['interaction_rate'] = (df_videos['likes_count'] + df_videos['comments_count']) / df_videos['play_count']
            avg_interaction_rate = df_videos['interaction_rate'].mean()
        else:
            avg_interaction_rate = 0

        # --- 👇 CÁLCULO DE LA NUEVA MÉTRICA 👇 ---
        total_interactions = total_video_reach * avg_interaction_rate

        df_candidate['impact_score'] = 1 + (df_candidate['likes_count'] * 0.1) + (df_candidate['comments_count'] * 0.25)
        top_5_posts = df_candidate.nlargest(5, 'impact_score')

        comparative_results[candidate] = {
            'reach': total_video_reach,
            'rate': avg_interaction_rate,
            'total_interactions': total_interactions # Guardar para el gráfico
        }

        report_filename = f"reporte_{candidate}.txt"
        report_filepath = os.path.join(OUTPUT_FOLDER, report_filename)
        with open(report_filepath, 'w', encoding='utf-8') as f_report:
            f_report.write(f"--- Reporte de Análisis de Discurso y Relevancia: {candidate} ---\n\n")
            f_report.write("== MÉTRICAS CLAVE DE VIDEO ==\n")
            f_report.write(f"Alcance Real Total de Videos (Reproducciones): {total_video_reach:,.0f}\n")
            f_report.write(f"Tasa de Interacción por Reproducción Promedio: {avg_interaction_rate:.2%}\n")
            f_report.write(f"Interacciones Totales Estimadas en Videos: {total_interactions:,.0f}\n\n")
            f_report.write("== TOP 5 PUBLICACIONES DE MAYOR IMPACTO ==\n")
            for i, (_, post) in enumerate(top_5_posts.iterrows()):
                f_report.write(f"\n--- {i+1}. Post con Impacto: {post['impact_score']:.2f} ---\n")
                f_report.write(f"URL: {post['post_url']}\n")
                f_report.write(f"Texto: {post['post_caption']}\n")
        print(f"  📄 Reporte individual guardado en '{report_filepath}'")

    # --- 👇 SECCIÓN DE GRÁFICOS MODIFICADA 👇 ---
    df_comp = pd.DataFrame.from_dict(comparative_results, orient='index')

    # Gráfico 1: Alcance Real
    ax_reach = df_comp.sort_values('reach', ascending=False).plot(kind='bar', y='reach', figsize=(12, 7), legend=None,
                                                                 title='Alcance Real Total de Videos (Suma de Reproducciones)')
    plt.ylabel("Total de Reproducciones")
    add_labels_to_bars(ax_reach)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_FOLDER, "comparativo_alcance_videos.png"))

    # Gráfico 2: Tasa de Interacción (Eficiencia)
    ax_rate = df_comp.sort_values('rate', ascending=False).plot(kind='bar', y='rate', figsize=(12, 7), legend=None,
                                                               title='Tasa de Interacción por Reproducción (Eficiencia)')
    plt.ylabel("Interacciones por Reproducción (%)")
    plt.gca().yaxis.set_major_formatter(plt.FuncFormatter('{:.2%}'.format))
    add_labels_to_bars(ax_rate, is_percentage=True)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_FOLDER, "comparativo_eficiencia_videos.png"))

    # --- 👇 NUEVO GRÁFICO 3: INTERACCIONES TOTALES 👇 ---
    ax_interactions = df_comp.sort_values('total_interactions', ascending=False).plot(kind='bar', y='total_interactions', figsize=(12, 7), legend=None,
                                                                                    title='Interacciones Totales Estimadas en Videos')
    plt.ylabel("Número Total de Interacciones (Likes + Comentarios)")
    add_labels_to_bars(ax_interactions)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_FOLDER, "comparativo_interacciones_totales_videos.png"))

    print("\n📊 Gráficos comparativos guardados.")
    print(f"\n🎉 ¡Proceso completado! Todos los reportes están en la carpeta '{OUTPUT_FOLDER}'.")

if __name__ == "__main__":
    analyze_discourse_and_relevance()