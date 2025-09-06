import pandas as pd
import os
import numpy as np

def analyze_instagram_data(input_filepath='base_de_datos_instagram.csv', output_folder='output'):
    """
    Realiza un análisis completo de los datos de Instagram, calcula métricas de engagement
    y genera varios archivos CSV con los resultados.

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
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Asignar seguidores actualizados a cada publicación
    df_sorted = df.sort_values('timestamp_registro', ascending=True)
    latest_followers = df_sorted.drop_duplicates('username', keep='last')[['username', 'followers_count']]
    latest_followers = latest_followers.rename(columns={'followers_count': 'seguidores_actualizados'})
    df = pd.merge(df, latest_followers, on='username', how='left')
    print("Datos cargados y seguidores actualizados asignados.")

    # --- PASO 2: Cálculo de Métricas de Engagement Específicas ---
    print("Paso 2: Calculando métricas de engagement...")

    # Evitar división por cero
    df['engagement_video_likes'] = np.where((df['media_type'] == 2) & (df['play_count'] > 0), df['likes_count'] / df['play_count'], 0)
    df['engagement_video_comments'] = np.where((df['media_type'] == 2) & (df['play_count'] > 0), df['comments_count'] / df['play_count'], 0)
    df['engagement_imagen_likes'] = np.where((df['media_type'] == 1) & (df['seguidores_actualizados'] > 0), df['likes_count'] / df['seguidores_actualizados'], 0)
    df['engagement_imagen_comments'] = np.where((df['media_type'] == 1) & (df['seguidores_actualizados'] > 0), df['comments_count'] / df['seguidores_actualizados'], 0)
    df['engagement_carrusel_likes'] = np.where((df['media_type'] == 8) & (df['seguidores_actualizados'] > 0), df['likes_count'] / df['seguidores_actualizados'], 0)
    df['engagement_carrusel_comments'] = np.where((df['media_type'] == 8) & (df['seguidores_actualizados'] > 0), df['comments_count'] / df['seguidores_actualizados'], 0)
    print("Métricas calculadas exitosamente.")

    # --- PASO 3: Generación de Archivos de Salida Ordenados ---
    print("Paso 3: Generando archivos de salida...")

    # a_resumen_candidatos.csv
    summary = df.groupby('username').agg(
        seguidores_actualizados=('seguidores_actualizados', 'max'),
        total_publicaciones=('post_id', 'count'),
        avg_engagement_video_likes=('engagement_video_likes', lambda x: x[x > 0].mean()),
        avg_engagement_video_comments=('engagement_video_comments', lambda x: x[x > 0].mean()),
        avg_engagement_imagen_likes=('engagement_imagen_likes', lambda x: x[x > 0].mean()),
        avg_engagement_imagen_comments=('engagement_imagen_comments', lambda x: x[x > 0].mean()),
        avg_engagement_carrusel_likes=('engagement_carrusel_likes', lambda x: x[x > 0].mean()),
        avg_engagement_carrusel_comments=('engagement_carrusel_comments', lambda x: x[x > 0].mean())
    ).reset_index().fillna(0)
    summary.to_csv(os.path.join(output_folder, 'a_resumen_candidatos.csv'), index=False, float_format='%.6f')
    print(f" -> Archivo 'a_resumen_candidatos.csv' generado.")

    # Función auxiliar para generar los Top 10 (MODIFICADA)
    def generate_top10(media_type, metric, filename):
        df_filtered = df[df['media_type'] == media_type]
        top10 = df_filtered.groupby('username').apply(lambda x: x.nlargest(10, metric)).reset_index(drop=True)

        # Lista de columnas actualizada con los campos que solicitaste
        cols_to_keep = [
            'username',
            metric, # La métrica de engagement específica
            'likes_count',
            'comments_count',
            'seguidores_actualizados',
            'post_created_at_str',
            'play_count',
            'usertags',
            'post_transcript',
            'post_caption',
            'post_url'
        ]

        # Asegurarse de que las columnas existan en el dataframe para evitar errores
        existing_cols_to_keep = [col for col in cols_to_keep if col in top10.columns]

        top10[existing_cols_to_keep].to_csv(os.path.join(output_folder, filename), index=False, float_format='%.6f')
        print(f" -> Archivo '{filename}' generado.")

    # Generar todos los Top 10
    generate_top10(2, 'engagement_video_likes', 'b_top10_videos_likes.csv')
    generate_top10(2, 'engagement_video_comments', 'c_top10_videos_comments.csv')
    generate_top10(1, 'engagement_imagen_likes', 'd_top10_imagenes_likes.csv')
    generate_top10(1, 'engagement_imagen_comments', 'e_top10_imagenes_comments.csv')
    generate_top10(8, 'engagement_carrusel_likes', 'f_top10_carruseles_likes.csv')
    generate_top10(8, 'engagement_carrusel_comments', 'g_top10_carruseles_comments.csv')

    # h_ y i_ datos de evolución
    df['dia_publicacion'] = df['post_created_at_str'].dt.date
    likes_evolution = df.groupby(['dia_publicacion', 'username'])['likes_count'].sum().reset_index()
    comments_evolution = df.groupby(['dia_publicacion', 'username'])['comments_count'].sum().reset_index()

    likes_evolution.to_csv(os.path.join(output_folder, 'h_datos_evolucion_likes.csv'), index=False)
    print(f" -> Archivo 'h_datos_evolucion_likes.csv' generado.")
    comments_evolution.to_csv(os.path.join(output_folder, 'i_datos_evolucion_comentarios.csv'), index=False)
    print(f" -> Archivo 'i_datos_evolucion_comentarios.csv' generado.")

    print("\n¡Análisis completado! Todos los archivos han sido guardados en la carpeta 'output'.")


# --- Ejecutar el análisis ---
if __name__ == "__main__":
    analyze_instagram_data()