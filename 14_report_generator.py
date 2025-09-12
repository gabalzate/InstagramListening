import pandas as pd
import os
import glob
import json
from jinja2 import Environment, FileSystemLoader
import markdown # <-- LIBRERÍA AÑADIDA

# --- (El resto de la configuración se mantiene igual) ---
CANDIDATES_FILE = "perfiles_instagram.txt"
REPORTS_FOLDER = "reportes_discurso"
LLM_GENERAL_FOLDER = "analisis_llm"
LLM_EXITOSO_FOLDER = "analisis_discurso_exitoso"
NAME_MAP_FILE = "reemplazo_nombres_perfiles_visualizacion.json"
OUTPUT_FOLDER = "sitio_web"
BASE_DATA_FILE = "base_de_datos_instagram.csv"
SUMMARY_ENGAGEMENT_FILE = "output/a_resumen_candidatos.csv"
TOP_VIDEOS_FILE = "output/b_top10_videos_likes.csv"


def get_start_date(file_path):
    try:
        df = pd.read_csv(file_path, usecols=['post_created_at_str'])
        df['post_created_at_str'] = pd.to_datetime(df['post_created_at_str'])
        min_date = df['post_created_at_str'].min().strftime('%d de %B de %Y')
        return min_date
    except Exception as e:
        print(f"⚠️  Advertencia: No se pudo leer la fecha de inicio. {e}")
        return "N/A"

def format_summary_data(file_path, name_map):
    try:
        df = pd.read_csv(file_path)
        summary_list = []
        for _, row in df.iterrows():
            formatted_row = {
                'name': name_map.get(row['username'], row['username']),
                'followers': f"{int(row['seguidores_actualizados']):,}",
                'avg_video_likes': f"{row['avg_engagement_video_likes']:.2%}",
                'avg_video_comments': f"{row['avg_engagement_video_comments']:.2%}",
                'avg_image_likes': f"{row['avg_engagement_imagen_likes']:.2%}",
                'avg_image_comments': f"{row['avg_engagement_imagen_comments']:.2%}",
                'avg_carousel_likes': f"{row['avg_engagement_carrusel_likes']:.2%}",
                'avg_carousel_comments': f"{row['avg_engagement_carrusel_comments']:.2%}",
            }
            summary_list.append(formatted_row)
        return summary_list
    except Exception as e:
        print(f"⚠️  Advertencia: No se pudo leer el archivo de resumen de engagement. {e}")
        return []

def get_top_videos(file_path, username):
    try:
        df = pd.read_csv(file_path)
        df_candidate = df[df['username'] == username].head(3)
        videos = []
        for _, row in df_candidate.iterrows():
            video_data = {
                'caption': row.get('post_caption', 'Sin descripción.'),
                'likes': f"{int(row.get('likes_count', 0)):,}",
                'comments': f"{int(row.get('comments_count', 0)):,}",
                'plays': f"{int(row.get('play_count', 0)):,}",
                'url': row.get('post_url')
            }
            videos.append(video_data)
        return videos
    except Exception as e:
        print(f"⚠️  Advertencia: No se pudieron cargar los top videos para {username}. {e}")
        return []

def gather_all_candidate_data(candidate_username, name_map, summary_engagement_data):
    data = {"username": candidate_username, "name": name_map.get(candidate_username, candidate_username)}

    followers = "N/A"
    for summary_row in summary_engagement_data:
        if summary_row['name'] == data['name']:
            followers = summary_row['followers']
            break
    data['followers'] = followers

    try:
        with open(os.path.join(REPORTS_FOLDER, f"reporte_{candidate_username}.txt"), 'r', encoding='utf-8') as f:
            for line in f:
                if "Alcance Real Total de Videos" in line:
                    data['reach'] = line.split(':')[-1].strip()
                if "Tasa de Interacción por Reproducción Promedio" in line:
                    data['rate'] = line.split(':')[-1].strip()
    except FileNotFoundError:
        data['reach'], data['rate'] = "N/A", "N/A"

    # --- 👇 CAMBIO IMPORTANTE AQUÍ 👇 ---
    # Leer los análisis y convertirlos de Markdown a HTML
    try:
        with open(os.path.join(LLM_GENERAL_FOLDER, f"analisis_llm_{candidate_username}.txt"), 'r', encoding='utf-8') as f:
            markdown_text = f.read()
            data['llm_analysis_general'] = markdown.markdown(markdown_text) # Conversión a HTML
    except FileNotFoundError:
        data['llm_analysis_general'] = "<p>Análisis del LLM (General) no encontrado.</p>"

    try:
        with open(os.path.join(LLM_EXITOSO_FOLDER, f"analisis_exitoso_{candidate_username}.txt"), 'r', encoding='utf-8') as f:
            markdown_text = f.read()
            data['llm_analysis_exitoso'] = markdown.markdown(markdown_text) # Conversión a HTML
    except FileNotFoundError:
        data['llm_analysis_exitoso'] = "<p>Análisis del Discurso Exitoso (LLM) no encontrado.</p>"

    data['top_videos'] = get_top_videos(TOP_VIDEOS_FILE, candidate_username)
    data['report_url'] = f"reporte_{candidate_username}.html"
    data['wordcloud_path'] = f"../{REPORTS_FOLDER}/wordcloud_discurso_{candidate_username}.png"

    return data

def main():
    print("🚀 Iniciando la generación del sitio web de reportes...")
    env = Environment(loader=FileSystemLoader('.'))

    try:
        with open(CANDIDATES_FILE, 'r', encoding='utf-8') as f:
            candidates = [line.strip() for line in f if line.strip()]
        with open(NAME_MAP_FILE, 'r', encoding='utf-8') as f:
            name_map = json.load(f)
    except FileNotFoundError as e:
        print(f"❌ Error: No se pudo encontrar un archivo de configuración necesario: {e}")
        return

    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)

    start_date = get_start_date(BASE_DATA_FILE)
    summary_engagement_data = format_summary_data(SUMMARY_ENGAGEMENT_FILE, name_map)
    all_candidates_data = []

    print("📝 Generando reportes individuales...")
    template_individual = env.get_template("template_individual.html")
    for candidate in candidates:
        candidate_data = gather_all_candidate_data(candidate, name_map, summary_engagement_data)
        all_candidates_data.append(candidate_data)

        html_content = template_individual.render(
            candidate_name=candidate_data['name'],
            followers=candidate_data['followers'],
            llm_analysis_general=candidate_data['llm_analysis_general'],
            llm_analysis_exitoso=candidate_data['llm_analysis_exitoso'],
            wordcloud_path=candidate_data['wordcloud_path'],
            top_videos=candidate_data['top_videos']
        )

        output_path = os.path.join(OUTPUT_FOLDER, candidate_data['report_url'])
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
    print(f"  ✅ Se generaron {len(candidates)} reportes individuales.")

    print("📈 Generando el reporte general...")
    template_general = env.get_template("template_general.html")
    html_content_general = template_general.render(
        fecha_inicio_datos=start_date,
        summary_data=summary_engagement_data,
        candidates=all_candidates_data
    )
    with open(os.path.join(OUTPUT_FOLDER, "index.html"), 'w', encoding='utf-8') as f:
        f.write(html_content_general)
    print("  ✅ Se generó 'index.html'.")

    print(f"\n🎉 ¡Proceso completado! El sitio web está listo en la carpeta '{OUTPUT_FOLDER}'.")

if __name__ == "__main__":
    main()