import pandas as pd
import os
import numpy as np
from datetime import datetime, timedelta

def run_analysis(df, output_folder):
    """
    Toma un DataFrame de datos de Instagram y ejecuta todo el proceso de análisis,
    guardando los resultados en la carpeta de salida especificada.

    Args:
        df (pd.DataFrame): El DataFrame con los datos a analizar.
        output_folder (str): La ruta a la carpeta donde se guardarán los resultados.
    """
    print(f"\n--- Iniciando análisis para la carpeta: '{output_folder}' ---")
    
    # --- Creación de la Carpeta de Salida ---
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"Carpeta '{output_folder}' creada.")

    # --- PASO 2: Cálculo de Métricas de Engagement ---
    print("Paso 2: Calculando métricas de engagement...")
    
    # Copia segura para evitar SettingWithCopyWarning
    df = df.copy()

    # Calcular engagement basado en seguidores (para no videos)
    engagement_likes_followers = (df['likes_count'] / df['followers_count']) * 100
    engagement_comments_followers = (df['comments_count'] / df['followers_count']) * 100

    # Calcular engagement basado en reproducciones (solo para videos)
    engagement_likes_plays = (df['likes_count'] / df['play_count']) * 100
    engagement_comments_plays = (df['comments_count'] / df['play_count']) * 100

    # Aplicar la lógica condicional usando np.where
    # Si media_type es 2 (video), usa el cálculo por plays. Si no (imagen o carrusel), usa el cálculo por followers.
    df['engagement_likes'] = np.where(df['media_type'] == 2, engagement_likes_plays, engagement_likes_followers)
    df['engagement_comments'] = np.where(df['media_type'] == 2, engagement_comments_plays, engagement_comments_followers)

    # Reemplazar posibles valores infinitos o NaN con 0
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df['engagement_likes'] = df['engagement_likes'].fillna(0)
    df['engagement_comments'] = df['engagement_comments'].fillna(0)
    
    print(" -> Métricas de engagement calculadas.")

    # --- PASO 3: Creación del Resumen de Candidatos ---
    print("Paso 3: Creando el resumen de candidatos...")
    
    # CORRECCIÓN AQUÍ: Se usa 'max' en lugar de 'first' para asegurar que se tome
    # el valor más alto (el conteo real) y se ignoren los NaNs generados por la limpieza.
    resumen_candidatos = df.groupby('username').agg(
        seguidores_actualizados=('followers_count', 'max'), 
        total_publicaciones=('post_id', 'count')
    ).reset_index()
    
    # Asegurarse de que no haya NaNs en seguidores si el 'max' falló (si el df está vacío, aunque la premisa lo descarte)
    resumen_candidatos['seguidores_actualizados'] = resumen_candidatos['seguidores_actualizados'].fillna(0)


    # Calcular promedios de engagement por tipo de medio
    avg_engagement = df.groupby(['username', 'media_type']).agg(
        avg_likes=('engagement_likes', 'mean'),
        avg_comments=('engagement_comments', 'mean')
    ).reset_index()

    # Mapeo de media_type a nombres para las columnas
    media_type_map = {1: 'imagen', 2: 'video', 8: 'carrusel'}
    
    # Reorganizar la tabla para tener columnas por tipo de medio y métrica
    pivot_engagement = avg_engagement.pivot_table(
        index='username',
        columns='media_type',
        values=['avg_likes', 'avg_comments']
    ).reset_index()

    # Renombrar las columnas de forma más descriptiva
    pivot_engagement.columns = [
        f'{col[0]}_{media_type_map[int(col[1])]}' if isinstance(col[1], (int, float)) and int(col[1]) in media_type_map else col[0] 
        for col in pivot_engagement.columns
    ]
    
    # Renombrar columnas finales al formato deseado
    pivot_engagement = pivot_engagement.rename(columns={
        'avg_likes_imagen': 'avg_engagement_imagen_likes',
        'avg_comments_imagen': 'avg_engagement_imagen_comments',
        'avg_likes_video': 'avg_engagement_video_likes',
        'avg_comments_video': 'avg_engagement_video_comments',
        'avg_likes_carrusel': 'avg_engagement_carrusel_likes',
        'avg_comments_carrusel': 'avg_engagement_carrusel_comments',
    })
    
    # Unir el resumen general con los promedios de engagement
    resumen_candidatos = pd.merge(resumen_candidatos, pivot_engagement, on='username', how='left')
    # Rellenar con 0 si un candidato no tiene cierto tipo de contenido
    resumen_candidatos.fillna(0, inplace=True)
    
    # Guardar el archivo de resumen
    resumen_candidatos.to_csv(os.path.join(output_folder, 'a_resumen_candidatos.csv'), index=False, float_format='%.6f')
    print(" -> Archivo 'a_resumen_candidatos.csv' generado.")

    # --- PASO 4: Generación de Tops 10 y Archivos de Evolución ---
    print("Paso 4: Generando archivos de Top 10 por perfil y evolución...")

    def generate_top10_per_user(media_type, metric, filename):
        """Genera el archivo Top 10 para un tipo de medio y métrica específicos."""
        # Filtrar por tipo de medio
        media_df = df[df['media_type'] == media_type]
        if media_df.empty:
            print(f" -> No hay datos para media_type {media_type}, se omite '{filename}'.")
            return
            
        # Ordenar por usuario y métrica, luego tomar los 10 primeros por usuario
        top_10_df = media_df.sort_values(by=['username', metric], ascending=[True, False]) \
                             .groupby('username') \
                             .head(10)
        
        # Guardar el archivo Top 10
        top_10_df.to_csv(os.path.join(output_folder, filename), index=False, float_format='%.6f')
        print(f" -> Archivo '{filename}' generado.")

    # Generar todos los archivos Top 10
    generate_top10_per_user(2, 'engagement_likes', 'b_top10_videos_likes.csv')
    generate_top10_per_user(2, 'engagement_comments', 'c_top10_videos_comments.csv')
    generate_top10_per_user(1, 'engagement_likes', 'd_top10_imagenes_likes.csv')
    generate_top10_per_user(1, 'engagement_comments', 'e_top10_imagenes_comments.csv')
    generate_top10_per_user(8, 'engagement_likes', 'f_top10_carruseles_likes.csv')
    generate_top10_per_user(8, 'engagement_comments', 'g_top10_carruseles_comments.csv')

    # Generar datos de evolución diaria
    if 'post_created_at_str' in df.columns and not df['post_created_at_str'].isnull().all():
        df['dia_publicacion'] = df['post_created_at_str'].dt.date
        likes_evolution = df.groupby(['dia_publicacion', 'username'])['likes_count'].sum().reset_index()
        comments_evolution = df.groupby(['dia_publicacion', 'username'])['comments_count'].sum().reset_index()

        likes_evolution.to_csv(os.path.join(output_folder, 'h_datos_evolucion_likes.csv'), index=False)
        print(" -> Archivo 'h_datos_evolucion_likes.csv' generado.")
        comments_evolution.to_csv(os.path.join(output_folder, 'i_datos_evolucion_comentarios.csv'), index=False)
        print(" -> Archivo 'i_datos_evolucion_comentarios.csv' generado.")
    else:
        print(" -> No se pudieron generar los archivos de evolución (columna de fecha ausente o vacía).")


    print(f"\n✅ Análisis para '{output_folder}' completado.")


def main(input_filepath='base_de_datos_instagram.csv'):
    """
    Función principal que carga los datos y orquesta los dos tipos de análisis:
    completo y mensual.
    """
    print("Iniciando el proceso de análisis dual...")

    # --- Carga y Preparación Inicial de Datos ---
    try:
        df_full = pd.read_csv(input_filepath)
    except FileNotFoundError:
        print(f"Error: No se encontró el archivo de entrada en '{input_filepath}'.")
        return

    # Limpieza básica inicial
    df_full['post_created_at_str'] = pd.to_datetime(df_full['post_created_at_str'], errors='coerce')
    # Asegurarse de que 'media_type' existe antes de convertirla
    if 'media_type' not in df_full.columns:
        print("Error: Falta la columna 'media_type' en el archivo CSV.")
        return
        
    numeric_cols = ['followers_count', 'likes_count', 'comments_count', 'play_count', 'media_type']
    for col in numeric_cols:
          # Verificar si la columna existe antes de intentar convertirla
          if col in df_full.columns:
            df_full[col] = pd.to_numeric(df_full[col], errors='coerce')
          else:
            print(f"Advertencia: Falta la columna '{col}'. Se continuará sin ella.")
            # Crear la columna con NaNs si no existe para evitar errores posteriores
            if col == 'play_count': df_full[col] = np.nan 
            # Para otras columnas numéricas, podrías asignar 0 o NaN según tu lógica
            elif col in ['likes_count', 'comments_count', 'followers_count']: df_full[col] = 0 


    # Reemplazar ceros con NaN para evitar divisiones por cero
    if 'followers_count' in df_full.columns:
        df_full['followers_count'] = df_full['followers_count'].replace(0, np.nan)
    if 'play_count' in df_full.columns:
          df_full['play_count'] = df_full['play_count'].replace(0, np.nan)
    
    # --- ANÁLISIS 1: COMPLETO ---
    # Se pasa una copia del DataFrame completo a la función de análisis
    run_analysis(df_full.copy(), output_folder='output')
    
    # --- ANÁLISIS 2: MENSUAL ---
    today = datetime.now()
    # Determinar el rango de fechas para el análisis mensual
    if today.day <= 5:
        # Incluir mes actual y anterior si estamos a principios de mes
        first_day_current_month = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        last_day_previous_month = first_day_current_month - timedelta(days=1)
        start_date = last_day_previous_month.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        print(f"Análisis mensual incluirá datos desde: {start_date.strftime('%Y-%m-%d')}")
    else:
        # Incluir solo el mes actual
        start_date = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        print(f"Análisis mensual incluirá datos desde: {start_date.strftime('%Y-%m-%d')}")
    
    # Filtrar el DataFrame para obtener solo los datos del rango mensual
    # Asegurarse de que la columna de fecha no tenga NaNs antes de filtrar
    df_monthly = df_full[df_full['post_created_at_str'].notna() & (df_full['post_created_at_str'] >= start_date)].copy()
    
    # Crear el nombre de la carpeta de salida mensual dinámicamente
    today_str = today.strftime('%Y%m%d')
    output_folder_monthly = f'output_{today_str}_month' # Corregido el nombre
    
    # Ejecutar el análisis solo si hay datos en el rango mensual
    if not df_monthly.empty:
        run_analysis(df_monthly, output_folder=output_folder_monthly)
    else:
        print(f"\n--- No se encontraron publicaciones en el rango mensual ({start_date.strftime('%Y-%m-%d')} en adelante). Se omite el análisis para '{output_folder_monthly}'. ---")

    print("\n🎉 Proceso dual completado.")

# --- Ejecución del Análisis ---
if __name__ == '__main__':
    main()
