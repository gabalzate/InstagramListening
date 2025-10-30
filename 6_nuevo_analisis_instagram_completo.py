import pandas as pd
import numpy as np
import os
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import warnings

# Configuración inicial para evitar warnings de visualización
warnings.filterwarnings("ignore")

# =========================================================================
# 1. CONFIGURACIÓN INICIAL Y PREPARACIÓN
# =========================================================================

# --- Variables de Entorno ---
INPUT_FILE = "base_de_datos_instagram.csv"
TODAY = datetime.now()

# La carpeta de salida incluye la fecha de ejecución 
FOLDER_NAME = f"analisis_{TODAY.strftime('%Y%m%d')}"

# Determinar el mes y año actual para el filtrado
CURRENT_MONTH_YEAR = TODAY.strftime('%Y-%m')


def setup_environment(input_file: str) -> pd.DataFrame or None:
    """Crea la carpeta de salida, carga el DataFrame y limpia tipos de datos."""
    
    if not os.path.exists(FOLDER_NAME):
        os.makedirs(FOLDER_NAME)
        print(f"Carpeta de salida creada: '{FOLDER_NAME}'")

    try:
        df = pd.read_csv(input_file)
    except FileNotFoundError:
        print(f"❌ Error: El archivo '{input_file}' no se encontró. Asegúrate de que está en la misma carpeta.")
        return None

    # Limpieza: Convertir fecha y columnas de conteo a tipos adecuados
    df['post_created_at_dt'] = pd.to_datetime(df['post_created_at_str'], errors='coerce')
    
    # Columnas de conteo a tipo numérico (incluyendo play_count y media_type si es numérico)
    count_columns = ['followers_count', 'likes_count', 'comments_count', 'play_count', 'media_type']
    for col in count_columns:
        # Rellenar NaNs con 0 y convertir a entero (necesario para cálculos)
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    # Filtrar registros que no tienen una fecha válida (NaT)
    df = df[df['post_created_at_dt'].notna()].copy()
    
    return df

def step_1_data_preparation(df: pd.DataFrame) -> pd.DataFrame:
    """Filtra el DataFrame por el mes en curso y añade columnas de tiempo."""
    
    print(f"\n--- PASO 1: Filtrando datos para el mes/año: {CURRENT_MONTH_YEAR} ---")
    
    # 1. Filtrado de datos
    df['analysis_month'] = df['post_created_at_dt'].dt.strftime('%Y-%m')
    df_filtered = df[df['analysis_month'] == CURRENT_MONTH_YEAR].copy()
    
    # 2. Adición de columnas de tiempo (necesarias para los Pasos 5 y 7)
    df_filtered['date_only'] = df_filtered['post_created_at_dt'].dt.date
    df_filtered['hour'] = df_filtered['post_created_at_dt'].dt.hour
    df_filtered['day_of_week'] = df_filtered['post_created_at_dt'].dt.dayofweek # 0=Lunes, 6=Domingo
    
    # 3. Guardar el DataFrame filtrado
    output_path = os.path.join(FOLDER_NAME, "01_data_filtrada_mensual_ig.csv")
    df_filtered['date_only'] = df_filtered['date_only'].astype(str) # Convertir a string para CSV
    df_filtered.to_csv(output_path, index=False)
    
    print(f"Filas después del filtrado: {len(df_filtered)}")
    print(f"Datos filtrados guardados en: {output_path}")
    
    return df_filtered

# =========================================================================
# 2. PASO 2: MÉTRICAS BÁSICAS Y TASA DE ENGAGEMENT POR SEGUIDORES (ERF)
# =========================================================================

def step_2_monthly_summary(df: pd.DataFrame):
    """Genera la tabla de resumen de actividad mensual por perfil, incluyendo ERF."""
    
    print("\n--- PASO 2: Métricas Básicas y Tasa de Engagement por Seguidores (ERF) ---")

    # 1. Definir agregaciones
    aggregation_functions = {
        'post_id': 'count',                 
        'likes_count': 'sum',              
        'comments_count': 'sum',           
        'play_count': 'sum',               
        'followers_count': 'max'            
    }

    # 2. Aplicar la agregación
    df_summary = df.groupby('username').agg(aggregation_functions).reset_index()
    df_summary = df_summary.rename(columns={'post_id': 'posts_publicados_mes', 'followers_count': 'max_followers_mes'})
    
    # 3. Calcular la Tasa de Engagement por Seguidores (ERF)
    # FÓRMULA: ERF = (Likes + Comentarios) / Max Followers * 100
    df_summary['total_interactions'] = df_summary['likes_count'] + df_summary['comments_count']
    df_summary['ERF_total'] = np.where(
        df_summary['max_followers_mes'] > 0,
        (df_summary['total_interactions'] / df_summary['max_followers_mes']) * 100,
        0
    )

    # 4. Ordenar y guardar
    df_summary = df_summary.sort_values(by='posts_publicados_mes', ascending=False)
    output_path = os.path.join(FOLDER_NAME, "02_monthly_summary_ig.csv")
    df_summary.to_csv(output_path, index=False)
    
    print(f"Tabla de resumen mensual (incluyendo ERF) guardada en: {output_path}")

# =========================================================================
# 3. PASO 3 y 4: CÁLCULO DE ENGAGEMENT PONDERADO (IC-P) Y TOP POSTS
# =========================================================================

def step_3_4_icp_top_posts(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula el Índice de Compromiso Ponderado (IC-P) por post y genera el Top 3 y Ratios Promedio."""
    
    print("\n--- PASOS 3 y 4: Índice de Compromiso Ponderado (IC-P) y Top Posts ---")
    
    # 1. Cálculo de las interacciones ponderadas (Numerador del IC-P)
    # FÓRMULA: Numerador Ponderado = (3 * Comentarios) + (1 * Likes)
    df['weighted_interactions'] = (3 * df['comments_count']) + (1 * df['likes_count'])
    
    # 2. Determinar el Denominador Lógico (Vistas o Seguidores)
    # 🛑 FIX: Usamos la comparación numérica directa para media_type. 
    # media_type == 2 es el código para VIDEO en la mayoría de implementaciones de Instagram.
    df['denominator'] = np.where(
        # Condición: Si es un Video (media_type == 2) Y tiene vistas (>0)
        (df['media_type'] == 2) & (df['play_count'] > 0),
        df['play_count'],
        # Falso: Es imagen, carrusel o video sin vistas -> Usar Followers
        df['followers_count']
    )
    
    # 3. Cálculo del Índice de Compromiso Ponderado (IC-P)
    # FÓRMULA: IC-P = (Interacciones Ponderadas / Denominador Lógico) * 100
    df['IC_P'] = np.where(
        df['denominator'] > 0,
        (df['weighted_interactions'] / df['denominator']) * 100,
        0
    )
    
    # --- PASO 4.1: Generar el Top 3 por IC-P ---
    df_top_3 = df.sort_values(by='IC_P', ascending=False).head(3)

    df_top_3 = df_top_3[[
        'username', 'post_caption', 'post_url', 'post_shortcode', 'IC_P', 'media_type'
    ]].copy()
    df_top_3['IC_P'] = df_top_3['IC_P'].round(2)

    output_path = os.path.join(FOLDER_NAME, "04_top_3_posts_ig.csv")
    df_top_3.to_csv(output_path, index=False)
    print(f"Tabla de Top 3 posts por IC-P guardada en: {output_path}")

    # --- PASO 4.2: Generar Ratios Promedio por Perfil ---
    
    df['ERV_Comments'] = np.where(df['play_count'] > 0, (df['comments_count'] / df['play_count']) * 100, 0)
    df['ERF_Likes'] = np.where(df['followers_count'] > 0, (df['likes_count'] / df['followers_count']) * 100, 0)
    
    df_ratios = df.groupby('username')[['IC_P', 'ERV_Comments', 'ERF_Likes']].mean().reset_index()
    df_ratios = df_ratios.sort_values(by='IC_P', ascending=False)
    
    output_path_ratios = os.path.join(FOLDER_NAME, "04_profile_engagement_ratios_ig.csv")
    df_ratios.to_csv(output_path_ratios, index=False)
    print(f"Tabla de Ratios Promedio por Perfil guardada en: {output_path_ratios}")
    
    return df

# =========================================================================
# 4. PASO 5: FRECUENCIA DIARIA (TENDENCIA)
# =========================================================================

def step_5_daily_frequency(df: pd.DataFrame):
    """Calcula y grafica la cantidad de posts diarios por perfil."""
    
    print("\n--- PASO 5: Frecuencia de Publicación Diaria (Tendencia) ---")
    
    # 1. Contar posts diarios
    df_daily_posts = df.groupby(['date_only', 'username']).agg(
        posts_count=('post_id', 'count')
    ).reset_index()

    # 2. Guardar la tabla de datos diarios (CSV)
    df_daily_posts['date_only'] = df_daily_posts['date_only'].astype(str)
    output_path_csv = os.path.join(FOLDER_NAME, "05_daily_post_count_ig.csv")
    df_daily_posts.to_csv(output_path_csv, index=False)
    print(f"Tabla de posts diarios guardada en: {output_path_csv}")

    # 3. Generar Gráfico de Líneas (Tendencia)
    df_plot = df_daily_posts.copy()
    df_plot['date_only'] = pd.to_datetime(df_plot['date_only'])
    
    plt.figure(figsize=(14, 6))
    sns.lineplot(
        data=df_plot, x='date_only', y='posts_count', hue='username',
        marker='o', dashes=False, palette='Spectral'
    )
    plt.title('Posts Diarios por Perfil (Tendencia Mensual - Instagram)', fontsize=16)
    plt.xlabel('Fecha', fontsize=14)
    plt.ylabel('Cantidad de Posts', fontsize=14)
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Perfil', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()

    output_path_png = os.path.join(FOLDER_NAME, "05_daily_post_line_chart_ig.png")
    plt.savefig(output_path_png)
    plt.close()
    print(f"Gráfico de Líneas guardado en: {output_path_png}")

# =========================================================================
# 5. PASO 6: LONGITUD DE CONTENIDO
# =========================================================================

def step_6_content_length(df: pd.DataFrame):
    """Calcula el promedio de longitud de la descripción (caption) y transcripción."""
    
    print("\n--- PASO 6: Análisis de Longitud de Contenido ---")
    
    # 1. Calcular la longitud del contenido (en caracteres)
    df['caption_length'] = df['post_caption'].fillna('').astype(str).apply(len)
    df['transcript_length'] = df['post_transcript'].fillna('').astype(str).apply(len)

    # 2. Agrupar por perfil y calcular el promedio
    df_content_length = df.groupby('username')[['caption_length', 'transcript_length']].mean().reset_index()
    df_content_length.columns = ['username', 'avg_caption_length', 'avg_transcript_length']

    # 3. Guardar la tabla (CSV)
    output_path = os.path.join(FOLDER_NAME, "06_content_length_ig.csv")
    df_content_length.to_csv(output_path, index=False)
    print(f"Tabla de longitud de contenido guardada en: {output_path}")

    # 4. Generar Gráfico de Barras
    df_length_plot = df_content_length.melt(
        id_vars='username', var_name='Type', value_name='Average_Length'
    )

    plt.figure(figsize=(14, 8))
    sns.barplot(
        data=df_length_plot, x='username', y='Average_Length', hue='Type', palette='Pastel1'
    )
    plt.title('Longitud Promedio de Contenido por Perfil (Instagram)', fontsize=16)
    plt.xlabel('Perfil', fontsize=14)
    plt.ylabel('Longitud Promedio de Caracteres', fontsize=14)
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Tipo de Contenido', loc='upper right')
    plt.tight_layout()

    output_png = os.path.join(FOLDER_NAME, "06_content_length_bar_chart_ig.png")
    plt.savefig(output_png)
    plt.close()
    print(f"Gráfico de Barras de longitud guardado en: {output_png}")


# =========================================================================
# 6. PASO 7: OPORTUNIDAD (HORA Y DÍA ÓPTIMOS)
# =========================================================================

def step_7_optimal_time(df: pd.DataFrame):
    """Calcula y grafica la hora y día óptimos de publicación (usando play_count promedio)."""
    
    print("\n--- PASO 7: Análisis de Oportunidad (Hora y Día Óptimos) ---")

    # 1. Hora Óptima
    df_optimal_hour = df.groupby(['hour', 'username']).agg(
        avg_success_metric=('play_count', 'mean') # Métrica de éxito: Vistas promedio (Play Count)
    ).reset_index()

    output_path_hour = os.path.join(FOLDER_NAME, "07_optimal_time_hour_ig.csv")
    df_optimal_hour.to_csv(output_path_hour, index=False)
    print(f"Tabla de hora óptima guardada en: {output_path_hour}")

    # Gráfico de Líneas para Hora Óptima
    plt.figure(figsize=(14, 6))
    sns.lineplot(
        data=df_optimal_hour, x='hour', y='avg_success_metric', hue='username',
        marker='o', dashes=False, palette='Spectral'
    )
    plt.title('Vistas Promedio por Hora de Publicación (Instagram)', fontsize=16)
    plt.xlabel('Hora del Día (0-23)', fontsize=14)
    plt.ylabel('Vistas Promedio (Play Count)', fontsize=14)
    plt.xticks(range(0, 24))
    plt.legend(title='Perfil', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    output_path_png = os.path.join(FOLDER_NAME, "07_optimal_time_hour_line_chart_ig.png")
    plt.savefig(output_path_png)
    plt.close()
    print(f"Gráfico de Líneas de hora óptima guardado en: {output_path_png}")
    
    # 2. Día Óptimo
    df_optimal_day = df.groupby(['day_of_week', 'username']).agg(
        avg_success_metric=('play_count', 'mean')
    ).reset_index()
    
    day_map = {0: 'Lunes', 1: 'Martes', 2: 'Miércoles', 3: 'Jueves', 4: 'Viernes', 5: 'Sábado', 6: 'Domingo'}
    df_optimal_day['day_name'] = df_optimal_day['day_of_week'].map(day_map)
    
    output_path_day = os.path.join(FOLDER_NAME, "07_optimal_time_day_ig.csv")
    df_optimal_day.to_csv(output_path_day, index=False)
    print(f"Tabla de día óptimo guardada en: {output_path_day}")

    # Gráfico de Barras para Día Óptimo
    plt.figure(figsize=(14, 6))
    sns.barplot(
        data=df_optimal_day, x='day_name', y='avg_success_metric', hue='username',
        palette='Spectral', order=day_map.values()
    )
    plt.title('Vistas Promedio por Día de la Semana de Publicación (Instagram)', fontsize=16)
    plt.xlabel('Día de la Semana', fontsize=14)
    plt.ylabel('Vistas Promedio (Play Count)', fontsize=14)
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Perfil', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()

    output_path_png = os.path.join(FOLDER_NAME, "07_optimal_time_day_bar_chart_ig.png")
    plt.savefig(output_path_png)
    plt.close()
    print(f"Gráfico de Barras de día óptimo guardado en: {output_path_png}")


# =========================================================================
# 7. PASO 8: DESEMPEÑO POR FORMATO (MEDIA TYPE)
# =========================================================================

def step_8_media_type_analysis(df: pd.DataFrame):
    """Calcula y grafica el IC-P promedio por tipo de contenido (media_type)."""
    
    print("\n--- PASO 8: Desempeño por Formato (Media Type) ---")

    # 🛑 FIX: Mapeo de códigos numéricos a nombres legibles
    # Basado en la inspección de datos y estándares comunes (1=Photo, 2=Video/Reel, 8=Carousel)
    media_map = {
        1: 'Photo/Image',
        2: 'Video (Reel)',
        8: 'Carousel'
    }
    
    # 1. Normalizar los tipos de medio y crear la columna de limpieza
    df['media_type_clean'] = df['media_type'].map(media_map).fillna('Otro/Desconocido')
    
    # 2. Agrupar por perfil y tipo de medio, y calcular el IC-P promedio
    df_media_type = df.groupby(['username', 'media_type_clean']).agg(
        avg_icp=('IC_P', 'mean')
    ).reset_index()

    # 3. Guardar la tabla (CSV)
    output_path_csv = os.path.join(FOLDER_NAME, "08_media_type_ranking_ig.csv")
    df_media_type.to_csv(output_path_csv, index=False)
    print(f"Tabla de IC-P por formato guardada en: {output_path_csv}")

    # 4. Generar Gráfico de Barras Agrupadas
    plt.figure(figsize=(14, 8))
    sns.barplot(
        data=df_media_type, x='username', y='avg_icp', hue='media_type_clean',
        palette='magma' # Paleta distinguible para formatos
    )
    plt.title('IC-P Promedio por Perfil y Tipo de Formato (Instagram)', fontsize=16)
    plt.xlabel('Perfil', fontsize=14)
    plt.ylabel('Índice de Compromiso Ponderado Promedio (IC-P)', fontsize=14)
    plt.xticks(rotation=45, ha='right')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.legend(title='Tipo de Formato', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()

    output_path_png = os.path.join(FOLDER_NAME, "08_media_type_bar_chart_ig.png")
    plt.savefig(output_path_png)
    plt.close()
    print(f"Gráfico de Barras de formatos guardado en: {output_path_png}")


# =========================================================================
# FUNCIÓN PRINCIPAL DE EJECUCIÓN
# =========================================================================
def main():
    """Ejecuta todos los pasos del análisis de Instagram."""
    
    df = setup_environment(INPUT_FILE)
    
    if df is not None:
        df_filtered = step_1_data_preparation(df)
        
        if len(df_filtered) > 0:
            step_2_monthly_summary(df_filtered)
            df_with_icp = step_3_4_icp_top_posts(df_filtered)
            step_5_daily_frequency(df_with_icp)
            step_6_content_length(df_with_icp)
            step_7_optimal_time(df_with_icp)
            step_8_media_type_analysis(df_with_icp)
            
            print("\n🎉 Proceso de análisis de métricas de INSTAGRAM finalizado. Revisa la carpeta:", FOLDER_NAME)
        else:
            print("\n⚠️ No se encontraron datos para el mes en curso en la base de datos de Instagram. Finalizando el script.")

if __name__ == "__main__":
    main()
