import pandas as pd
import os
import re
import numpy as np
from datetime import datetime, timedelta
from wordcloud import WordCloud
import matplotlib.pyplot as plt

# --- CONFIGURACIÓN ---
MAIN_DATA_FILE = "base_de_datos_instagram.csv"
PROFILES_FILE = "perfiles_instagram.txt"
# Carpeta base para análisis completo
OUTPUT_FOLDER_FULL = "reportes_discurso" 

STOPWORDS = set([
    'de', 'la', 'que', 'el', 'en', 'y', 'a', 'los', 'del', 'se', 'las', 'por', 'un',
    'para', 'con', 'no', 'una', 'su', 'al', 'lo', 'como', 'más', 'pero', 'sus',
    'le', 'ya', 'o', 'este', 'ha', 'me', 'si', 'sin', 'sobre', 'este', 'entre',
    'cuando', ' también', 'fue', 'ser', 'son', 'dos', 'así', 'desde', 'muy', 'hasta',
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
    print(f"  🖼️ Nube de palabras guardada en '{filepath}'")

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


# --- FUNCIÓN REUTILIZABLE DE ANÁLISIS ---

def run_discourse_analysis(df, candidates, output_folder):
    """Ejecuta el análisis de discurso y relevancia sobre un DataFrame específico."""

    print(f"\n--- Iniciando análisis para la carpeta: '{output_folder}' ---")

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"📁 Carpeta '{output_folder}' creada.")

    comparative_results = {}

    for candidate in candidates:
        print(f"\n--- Analizando a: {candidate} ---")

        df_candidate = df[df['username'] == candidate].copy()

        if df_candidate.empty:
            print(f"  ⚠️ No se encontraron publicaciones para {candidate} en este período. Saltando...")
            continue
            
        # Asegurarse de que el conteo de seguidores sea válido (tomamos el max para ignorar NaNs de post individuales)
        latest_followers = df_candidate['followers_count'].max() 
        
        # --- SOLUCIÓN AL VALUERROR: Manejar NaN antes de la conversión a int ---
        if pd.isna(latest_followers) or latest_followers == 0:
             print(f"  ⚠️ Conteo de seguidores no válido (0 o NaN) para {candidate}. No se puede calcular el engagement. Se usa N/A en el reporte.")
             latest_followers_report_str = "N/A"
             # Usar 1 como denominador temporal solo para permitir que la columna de engagement
             # se cree y se llene con ceros más adelante. La lógica de engagement ya maneja NaNs.
             latest_followers = 1 
        else:
            latest_followers_report_str = f"{int(latest_followers):,}"
        # -----------------------------------------------------------------------


        # 1. Preparación del Corpus de Texto
        df_candidate['full_text'] = df_candidate['post_caption'].fillna('') + " " + \
                                    df_candidate['post_transcript'].replace('N/A', '').fillna('')
        
        corpus_text = " ".join(df_candidate['full_text'])
        cleaned_corpus = clean_text(corpus_text)

        # 1.1. Guardar corpus y generar nube de palabras
        corpus_filename = f"corpus_texto_{candidate}.txt"
        corpus_filepath = os.path.join(output_folder, corpus_filename)
        with open(corpus_filepath, 'w', encoding='utf-8') as f_out:
            f_out.write(corpus_text)
        print(f"  📝 Corpus de texto guardado en '{corpus_filepath}'")

        if cleaned_corpus:
            generate_wordcloud(cleaned_corpus, candidate, output_folder)
        else:
            print("  ⚠️ No hay suficiente texto limpio para generar una nube de palabras.")


        # 2. Cálculo de Métricas de Relevancia (Engagement)
        df_videos = df_candidate[df_candidate['media_type'] == 2].copy()
        
        # Asegurar que play_count es numérico y no NaN
        df_videos['play_count'] = pd.to_numeric(df_videos['play_count'], errors='coerce').fillna(0)
        
        total_video_reach = df_videos['play_count'].sum()
        
        # Calcular tasa de interacción por reproducción promedio
        if not df_videos.empty and total_video_reach > 0:
            df_videos['interaction_rate'] = (df_videos['likes_count'] + df_videos['comments_count']) / df_videos['play_count']
            avg_interaction_rate = df_videos['interaction_rate'].replace([np.inf, -np.inf], np.nan).mean()
            total_interactions = total_video_reach * avg_interaction_rate
        else:
            avg_interaction_rate = 0
            total_interactions = 0


        # 3. Puntuación de Impacto por Post (para Top 5)
        df_candidate['impact_score'] = 1 + (df_candidate['likes_count'].fillna(0) * 0.1) + (df_candidate['comments_count'].fillna(0) * 0.25)
        top_5_posts = df_candidate.nlargest(5, 'impact_score')

        comparative_results[candidate] = {
            'reach': total_video_reach,
            'rate': avg_interaction_rate if not pd.isna(avg_interaction_rate) else 0,
            'total_interactions': total_interactions
        }

        # 4. Generación de Reporte Individual
        report_filename = f"reporte_{candidate}.txt"
        report_filepath = os.path.join(output_folder, report_filename)
        with open(report_filepath, 'w', encoding='utf-8') as f_report:
            f_report.write(f"--- Reporte de Análisis de Discurso y Relevancia: {candidate} ---\n\n")
            f_report.write(f"Conteo de Seguidores Utilizado: {latest_followers_report_str}\n\n") # Uso la variable formateada
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

    # --- 5. SECCIÓN DE GRÁFICOS COMPARATIVOS ---
    if not comparative_results:
        print("\n⚠️ No hay datos comparables para generar gráficos.")
        return
        
    df_comp = pd.DataFrame.from_dict(comparative_results, orient='index')
    df_comp = df_comp.fillna(0) # Asegurar 0s para gráficos

    # Gráfico 1: Alcance Real
    ax_reach = df_comp.sort_values('reach', ascending=False).plot(kind='bar', y='reach', figsize=(12, 7), legend=None,
                                                                   title='Alcance Real Total de Videos (Suma de Reproducciones)')
    plt.ylabel("Total de Reproducciones")
    add_labels_to_bars(ax_reach)
    plt.tight_layout()
    plt.savefig(os.path.join(output_folder, "comparativo_alcance_videos.png"))
    plt.close()

    # Gráfico 2: Tasa de Interacción (Eficiencia)
    ax_rate = df_comp.sort_values('rate', ascending=False).plot(kind='bar', y='rate', figsize=(12, 7), legend=None,
                                                                title='Tasa de Interacción por Reproducción (Eficiencia)')
    plt.ylabel("Interacciones por Reproducción (%)")
    plt.gca().yaxis.set_major_formatter(plt.FuncFormatter('{:.2%}'.format))
    add_labels_to_bars(ax_rate, is_percentage=True)
    plt.tight_layout()
    plt.savefig(os.path.join(output_folder, "comparativo_eficiencia_videos.png"))
    plt.close()

    # Gráfico 3: Interacciones Totales Estimadas
    ax_interactions = df_comp.sort_values('total_interactions', ascending=False).plot(kind='bar', y='total_interactions', figsize=(12, 7), legend=None,
                                                                                      title='Interacciones Totales Estimadas en Videos')
    plt.ylabel("Número Total de Interacciones (Likes + Comentarios)")
    add_labels_to_bars(ax_interactions)
    plt.tight_layout()
    plt.savefig(os.path.join(output_folder, "comparativo_interacciones_totales_videos.png"))
    plt.close()

    print("\n📊 Gráficos comparativos guardados.")
    print(f"\n✅ Análisis para '{output_folder}' completado.")


# --- FUNCIÓN PRINCIPAL ORQUESTADORA ---

def main_discourse_analysis(input_filepath=MAIN_DATA_FILE):
    """
    Función principal que carga, limpia los datos y orquesta los dos tipos de análisis:
    completo y mensual.
    """
    print("Iniciando el proceso de análisis dual de discurso y métricas...")

    # --- Carga y Preparación Inicial de Datos ---
    try:
        # Forzar el post_id/shortcode a string para evitar errores de tipo en la deduplicación
        df_full = pd.read_csv(input_filepath, dtype={'post_id': str, 'post_shortcode': str})
    except FileNotFoundError:
        print(f"❌ Error: No se encontró el archivo de entrada en '{input_filepath}'.")
        return

    try:
        with open(PROFILES_FILE, 'r', encoding='utf-8') as f:
            candidates = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"❌ Error: No se encontró el archivo de perfiles en '{PROFILES_FILE}'.")
        return

    # 1. Limpieza de tipos de datos
    df_full['post_created_at_str'] = pd.to_datetime(df_full['post_created_at_str'], errors='coerce')
        
    numeric_cols = ['followers_count', 'likes_count', 'comments_count', 'play_count', 'media_type']
    for col in numeric_cols:
          if col in df_full.columns:
            df_full[col] = pd.to_numeric(df_full[col], errors='coerce')
          else:
            print(f"Advertencia: Falta la columna '{col}'. Se continuará con 0/NaN.")
            if col == 'play_count': df_full[col] = np.nan 
            elif col in ['likes_count', 'comments_count', 'followers_count']: df_full[col] = 0 
            
    # Reemplazar ceros con NaN en denominadores para evitar divisiones por cero
    df_full['followers_count'] = df_full['followers_count'].replace(0, np.nan)
    df_full['play_count'] = df_full['play_count'].replace(0, np.nan)

    # --- ANÁLISIS 1: COMPLETO (HISTORIAL) ---
    run_discourse_analysis(df_full.copy(), candidates, output_folder=OUTPUT_FOLDER_FULL)
    

    # --- ANÁLISIS 2: MENSUAL (MES PRESENTE) ---
    
    today = datetime.now()
    # Fecha de inicio del mes presente
    start_date = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    # Filtrar el DataFrame para obtener solo los posts creados en el mes presente
    df_monthly = df_full[df_full['post_created_at_str'].notna() & (df_full['post_created_at_str'] >= start_date)].copy()
    
    # Crear el nombre de la carpeta de salida mensual dinámicamente
    month_str = today.strftime('%m')
    year_str = today.strftime('%Y')
    output_folder_monthly = f'{OUTPUT_FOLDER_FULL}_{month_str}_{year_str}_month'
    
    # --- INYECCIÓN DE SEGUIDORES (Corregida de la última vez) ---
    # Esto asegura que el df_monthly (que solo tiene posts del mes) tenga el último conteo de seguidores
    if 'timestamp_registro' in df_full.columns:
        # 1. Obtener el último conteo de seguidores conocido para cada usuario del historial completo
        df_followers_latest = df_full.sort_values('timestamp_registro', ascending=True).drop_duplicates('username', keep='last')
        followers_map = df_followers_latest.set_index('username')['followers_count'].dropna().to_dict()
        
        # 2. Inyectar el último conteo de seguidores conocido en el DataFrame Mensual
        df_monthly['followers_count'] = df_monthly['username'].map(followers_map)
        # Reemplazar ceros con NaN nuevamente en el df_monthly
        df_monthly['followers_count'] = df_monthly['followers_count'].replace(0, np.nan) 
    else:
        print("Advertencia: No se pudo inyectar el conteo de seguidores más reciente. Se usará solo el conteo disponible.")


    # Ejecutar el análisis solo si hay datos en el rango mensual
    if not df_monthly.empty:
        run_discourse_analysis(df_monthly, candidates, output_folder=output_folder_monthly)
    else:
        print(f"\n--- No se encontraron publicaciones en el rango mensual ({start_date.strftime('%Y-%m-%d')} en adelante). Se omite el análisis para '{output_folder_monthly}'. ---")

    print("\n🎉 Proceso dual de análisis completado.")

# --- Ejecución del Análisis ---
if __name__ == '__main__':
    main_discourse_analysis()
