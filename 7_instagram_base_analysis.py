import pandas as pd
import os
import numpy as np

def analyze_instagram_data(input_filepath='base_de_datos_instagram.csv', output_folder='output'):
    """
    Realiza un análisis completo de los datos de Instagram, calcula métricas de engagement,
    genera un resumen por candidato y extrae las 10 publicaciones más relevantes POR PERFIL.

    Args:
        input_filepath (str): La ruta al archivo CSV de entrada.
        output_folder (str): El nombre de la carpeta para guardar los archivos de salida.
    """
    print("Iniciando el análisis de datos de Instagram...")

    # --- Creación de la Carpeta de Salida ---
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"Carpeta '{output_folder}' creada.")

    # --- PASO 1: Carga y Preparación de Datos ---
    print("Paso 1: Cargando y preparando los datos...")
    try:
        df = pd.read_csv(input_filepath)
    except FileNotFoundError:
        print(f"Error: No se encontró el archivo de entrada en '{input_filepath}'.")
        return

    # Limpieza básica y conversión de tipos
    df['timestamp_registro'] = pd.to_datetime(df['timestamp_registro'])
    df['post_created_at_str'] = pd.to_datetime(df['post_created_at_str'])
    numeric_cols = ['followers_count', 'likes_count', 'comments_count', 'play_count']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Reemplazar followers_count igual a 0 con NaN para evitar divisiones por cero
    df['followers_count'] = df['followers_count'].replace(0, np.nan)
    print(" -> Datos cargados y limpios.")

    # --- PASO 2: Cálculo de Métricas de Engagement ---
    print("Paso 2: Calculando métricas de engagement...")
    
    df['engagement_likes'] = (df['likes_count'] / df['followers_count']) * 100
    df['engagement_comments'] = (df['comments_count'] / df['followers_count']) * 100

    df['engagement_likes'] = df['engagement_likes'].fillna(0)
    df['engagement_comments'] = df['engagement_comments'].fillna(0)
    
    print(" -> Métricas de engagement calculadas.")

    # --- PASO 3: Creación del Resumen de Candidatos ---
    print("Paso 3: Creando el resumen de candidatos...")

    resumen_candidatos = df.groupby('username').agg(
        seguidores_actualizados=('followers_count', 'first'),
        total_publicaciones=('post_id', 'count')
    ).reset_index()
    resumen_candidatos['seguidores_actualizados'] = resumen_candidatos['seguidores_actualizados'].fillna(0)

    avg_engagement = df.groupby(['username', 'media_type']).agg(
        avg_likes=('engagement_likes', 'mean'),
        avg_comments=('engagement_comments', 'mean')
    ).reset_index()

    media_type_map = {1: 'imagen', 2: 'video', 8: 'carrusel'}
    
    pivot_engagement = avg_engagement.pivot_table(
        index='username',
        columns='media_type',
        values=['avg_likes', 'avg_comments']
    ).reset_index()

    pivot_engagement.columns = [
        f'{col[0]}_{media_type_map[col[1]]}' if col[1] in media_type_map else col[0] 
        for col in pivot_engagement.columns
    ]
    
    pivot_engagement = pivot_engagement.rename(columns={
        'avg_likes_imagen': 'avg_engagement_imagen_likes',
        'avg_comments_imagen': 'avg_engagement_imagen_comments',
        'avg_likes_video': 'avg_engagement_video_likes',
        'avg_comments_video': 'avg_engagement_video_comments',
        'avg_likes_carrusel': 'avg_engagement_carrusel_likes',
        'avg_comments_carrusel': 'avg_engagement_carrusel_comments',
    })
    
    resumen_candidatos = pd.merge(resumen_candidatos, pivot_engagement, on='username', how='left')
    resumen_candidatos.fillna(0, inplace=True)
    
    resumen_candidatos.to_csv(os.path.join(output_folder, 'a_resumen_candidatos.csv'), index=False, float_format='%.6f')
    print(" -> Archivo 'a_resumen_candidatos.csv' generado correctamente.")

    # --- PASO 4: Generación de Tops 10 y Archivos de Evolución (LÓGICA MODIFICADA) ---
    print("Paso 4: Generando archivos de Top 10 por perfil y evolución...")

    def generate_top10_per_user(media_type, metric, filename):
        """
        Filtra por tipo de medio, luego agrupa por usuario y para cada uno,
        encuentra las 10 publicaciones principales según la métrica especificada.
        """
        # Filtra el DataFrame por el tipo de medio para eficiencia
        media_df = df[df['media_type'] == media_type]
        
        # Ordena por usuario y luego por la métrica de forma descendente
        # y agrupa por usuario para tomar los 10 primeros de cada grupo.
        top_10_df = media_df.sort_values(by=['username', metric], ascending=[True, False]) \
                            .groupby('username') \
                            .head(10)
        
        top_10_df.to_csv(os.path.join(output_folder, filename), index=False, float_format='%.6f')
        print(f" -> Archivo '{filename}' generado (Top 10 por perfil).")

    # Generar todos los Top 10 con la nueva lógica por perfil
    generate_top10_per_user(2, 'engagement_likes', 'b_top10_videos_likes.csv')
    generate_top10_per_user(2, 'engagement_comments', 'c_top10_videos_comments.csv')
    generate_top10_per_user(1, 'engagement_likes', 'd_top10_imagenes_likes.csv')
    generate_top10_per_user(1, 'engagement_comments', 'e_top10_imagenes_comments.csv')
    generate_top10_per_user(8, 'engagement_likes', 'f_top10_carruseles_likes.csv')
    generate_top10_per_user(8, 'engagement_comments', 'g_top10_carruseles_comments.csv')

    # Generar datos de evolución (esta parte no necesitaba cambios)
    df['dia_publicacion'] = df['post_created_at_str'].dt.date
    likes_evolution = df.groupby(['dia_publicacion', 'username'])['likes_count'].sum().reset_index()
    comments_evolution = df.groupby(['dia_publicacion', 'username'])['comments_count'].sum().reset_index()

    likes_evolution.to_csv(os.path.join(output_folder, 'h_datos_evolucion_likes.csv'), index=False)
    print(" -> Archivo 'h_datos_evolucion_likes.csv' generado.")
    comments_evolution.to_csv(os.path.join(output_folder, 'i_datos_evolucion_comentarios.csv'), index=False)
    print(" -> Archivo 'i_datos_evolucion_comentarios.csv' generado.")

    print("\nAnálisis completado. Los archivos se han guardado en la carpeta 'output'.")

# --- Ejecución del Análisis ---
if __name__ == '__main__':
    analyze_instagram_data()
