import pandas as pd
import os
import re
import glob

# --- CONFIGURACIÓN ---
MAIN_DATA_FILE = "base_de_datos_instagram.csv"
PROFILES_FILE = "perfiles_instagram.txt"
MENTIONS_FOLDER = "menciones"
OUTPUT_CSV_RAW_FILE = "network_data_raw.csv"
OUTPUT_CSV_CONSOLIDATED_FILE = "network_data_consolidated.csv"

# --- FUNCIONES DE ANÁLISIS ---

def calculate_impact_weight(likes, comments):
    """Calcula el peso de una interacción basado en el engagement."""
    likes = float(likes) if pd.notna(likes) else 0
    comments = float(comments) if pd.notna(comments) else 0
    return 1 + (likes * 0.1) + (comments * 0.25)

def analyze_direct_interactions(main_df, candidates):
    """Parte 1: Extrae interacciones directas del archivo principal."""
    print("🔎 Parte 1: Analizando interacciones directas entre candidatos...")
    connections = []
    for _, row in main_df.iterrows():
        author = row['username']
        if author not in candidates:
            continue

        text_to_scan = f"{row.get('post_caption', '')} {row.get('usertags', '')}"

        for target_candidate in candidates:
            if author == target_candidate:
                continue

            if re.search(f'@{re.escape(target_candidate)}\\b|\\b{re.escape(target_candidate)}\\b', str(text_to_scan), re.IGNORECASE):
                weight = calculate_impact_weight(row['likes_count'], row['comments_count'])
                connections.append({'source': author, 'target': target_candidate, 'weight': weight})

    print(f"  ✅ Se encontraron {len(connections)} interacciones directas.")
    return connections

def analyze_external_mentions(mentions_folder, candidates):
    """Parte 2: Extrae co-menciones de los archivos de la carpeta 'menciones'."""
    print("\n🔎 Parte 2: Analizando conversaciones externas (co-menciones)...")
    connections = []
    mention_files = glob.glob(os.path.join(mentions_folder, '*.csv'))

    for file_path in mention_files:
        try:
            df_mention = pd.read_csv(file_path)
            main_candidate_mentioned = os.path.basename(file_path).split('_')[-1].replace('.csv', '')

            df_mention = df_mention[df_mention['username'] != main_candidate_mentioned]

            for _, row in df_mention.iterrows():
                author = row['username']
                text_to_scan = f"{row.get('post_caption', '')} {row.get('usertags', '')}"

                mentioned_in_post = set()
                for candidate in candidates:
                    if re.search(f'@{re.escape(candidate)}\\b|\\b{re.escape(candidate)}\\b', str(text_to_scan), re.IGNORECASE):
                        mentioned_in_post.add(candidate)

                if main_candidate_mentioned in mentioned_in_post and len(mentioned_in_post) > 1:
                    weight = calculate_impact_weight(row['likes_count'], row['comments_count'])
                    for mentioned_candidate in mentioned_in_post:
                        connections.append({'source': author, 'target': mentioned_candidate, 'weight': weight})
        except Exception as e:
            print(f"  ⚠️  Error procesando el archivo {file_path}: {e}")

    print(f"  ✅ Se encontraron {len(connections)} conexiones en menciones de terceros.")
    return connections

# --- FUNCIÓN PRINCIPAL ---

def main():
    """Orquesta el proceso para generar los archivos de datos de la red."""
    print("🚀 Iniciando la generación de datos de red...")

    try:
        main_df = pd.read_csv(MAIN_DATA_FILE)
        with open(PROFILES_FILE, 'r', encoding='utf-8') as f:
            candidates = {line.strip() for line in f if line.strip()}
    except FileNotFoundError as e:
        print(f"❌ Error: No se pudo encontrar un archivo necesario: {e}")
        return

    direct_connections = analyze_direct_interactions(main_df, candidates)
    external_connections = analyze_external_mentions(MENTIONS_FOLDER, candidates)
    all_connections = direct_connections + external_connections

    if not all_connections:
        print("❌ No se encontraron suficientes conexiones para generar los archivos.")
        return

    print("\n📊 Parte 3: Consolidando conexiones y guardando archivos...")
    df_net = pd.DataFrame(all_connections)

    # Guardar el archivo de conexiones RAW (df_net)
    try:
        df_net.to_csv(OUTPUT_CSV_RAW_FILE, index=False, float_format='%.2f')
        print(f"  💾 Datos de red sin procesar guardados en '{OUTPUT_CSV_RAW_FILE}'.")
    except IOError as e:
        print(f"  ⚠️  No se pudo guardar el archivo CSV de red sin procesar: {e}")

    # Consolidar los datos
    df_final_net = df_net.groupby(['source', 'target']).sum().reset_index()

    # Guardar el archivo de conexiones CONSOLIDADO (df_final_net)
    try:
        df_final_net.to_csv(OUTPUT_CSV_CONSOLIDATED_FILE, index=False, float_format='%.2f')
        print(f"  💾 Datos de red consolidados guardados en '{OUTPUT_CSV_CONSOLIDATED_FILE}'.")
    except IOError as e:
        print(f"  ⚠️  No se pudo guardar el archivo CSV de red consolidado: {e}")

    print("\n🎉 ¡Proceso completado! Los archivos de datos de la red han sido generados.")

if __name__ == "__main__":
    main()