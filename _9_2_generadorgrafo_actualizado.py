import pandas as pd
import os
import re
import glob
import json

# --- CONFIGURACIÓN ---
MAIN_DATA_FILE = "base_de_datos_instagram.csv"
PROFILES_FILE = "perfiles_instagram.txt"
NAMES_MAPPING_FILE = "reemplazo_nombres_perfiles_visualizacion.json" # Nuevo archivo
MENTIONS_FOLDER = "menciones"
OUTPUT_CSV_RAW_FILE = "network_data_raw.csv"
OUTPUT_CSV_CONSOLIDATED_FILE = "network_data_consolidated.csv"

# --- FUNCIONES DE UTILIDAD ---

def load_search_mapping(candidates, mapping_file):
    """
    Crea un diccionario de búsqueda expandido.
    Clave: Término a buscar (username o nombre real).
    Valor: ID canónico del candidato (username).
    """
    search_map = {}
    
    # 1. Cargar el JSON de nombres reales si existe
    real_names_map = {}
    if os.path.exists(mapping_file):
        try:
            with open(mapping_file, 'r', encoding='utf-8') as f:
                real_names_map = json.load(f)
            print(f"  ✅ Archivo de nombres reales cargado: {mapping_file}")
        except Exception as e:
            print(f"  ⚠️ Error leyendo JSON de nombres: {e}")
    else:
        print(f"  ⚠️ No se encontró el archivo JSON {mapping_file}. Se usarán solo usernames.")

    # 2. Construir el mapa de búsqueda para cada candidato
    for candidate in candidates:
        candidate = candidate.strip()
        # Mapear el username al username
        search_map[candidate] = candidate
        
        # Si el candidato tiene un nombre real en el JSON, mapear el nombre real al username
        # Nota: Asumimos que la clave en el JSON es el username (con o sin @)
        keys_to_check = [candidate, f"@{candidate}"]
        
        for key in keys_to_check:
            if key in real_names_map:
                real_name = real_names_map[key]
                if real_name:
                    # Agregamos el nombre real al mapa de búsqueda apuntando al candidato original
                    search_map[real_name] = candidate
                    
    return search_map

def calculate_impact_weight(likes, comments):
    """Calcula el peso de una interacción basado en el engagement."""
    likes = float(likes) if pd.notna(likes) else 0
    comments = float(comments) if pd.notna(comments) else 0
    return 1 + (likes * 0.1) + (comments * 0.25)

# --- FUNCIONES DE ANÁLISIS ---

def analyze_direct_interactions(main_df, search_map):
    """
    Parte 1: Extrae interacciones buscando en TODAS las columnas 
    coincidencias de username O nombre real.
    """
    print("🔎 Parte 1: Analizando interacciones directas (Búsqueda total en columnas)...")
    connections = []
    
    # Pre-compilar expresiones regulares para velocidad
    # Creamos una lista de (regex, target_candidate_id)
    search_patterns = []
    for search_term, target_id in search_map.items():
        # Escapamos el término para evitar errores de regex con caracteres especiales
        # \b asegura que coincida "Ana" pero no "Banana"
        pattern = re.compile(f'@{re.escape(search_term)}\\b|\\b{re.escape(search_term)}\\b', re.IGNORECASE)
        search_patterns.append((pattern, target_id))

    total_rows = len(main_df)
    for index, row in main_df.iterrows():
        if index % 100 == 0:
            print(f"  ...procesando fila {index}/{total_rows}", end='\r')

        author = row.get('username', 'unknown')
        
        # CONCATENACIÓN TOTAL: Convierte toda la fila a string y la une
        # Esto incluye caption, usertags, location, accessibility, etc.
        text_to_scan = " ".join(row.astype(str).values)

        # Buscar menciones en el texto masivo
        found_targets_in_row = set()
        
        for pattern, target_id in search_patterns:
            if author == target_id:
                continue # El autor no cuenta como mención a sí mismo

            if pattern.search(text_to_scan):
                found_targets_in_row.add(target_id)

        # Si encontramos menciones, calculamos peso y guardamos
        if found_targets_in_row:
            weight = calculate_impact_weight(row.get('likes_count', 0), row.get('comments_count', 0))
            for target in found_targets_in_row:
                connections.append({'source': author, 'target': target, 'weight': weight})

    print(f"\n  ✅ Se encontraron {len(connections)} interacciones directas.")
    return connections

def analyze_external_mentions(mentions_folder, search_map):
    """Parte 2: Extrae co-menciones usando el mapa expandido de nombres."""
    print("\n🔎 Parte 2: Analizando conversaciones externas (co-menciones)...")
    connections = []
    mention_files = glob.glob(os.path.join(mentions_folder, '*.csv'))

    # Pre-compilar regex igual que antes
    search_patterns = []
    for search_term, target_id in search_map.items():
        pattern = re.compile(f'@{re.escape(search_term)}\\b|\\b{re.escape(search_term)}\\b', re.IGNORECASE)
        search_patterns.append((pattern, target_id))

    for file_path in mention_files:
        try:
            df_mention = pd.read_csv(file_path)
            # Intentar deducir el candidato principal del nombre del archivo
            filename_clean = os.path.basename(file_path).replace('.csv', '').split('_')[-1]
            
            # Verificar si el nombre del archivo es un alias conocido
            main_candidate_mentioned = filename_clean
            if filename_clean in search_map:
                main_candidate_mentioned = search_map[filename_clean]

            for _, row in df_mention.iterrows():
                author = row.get('username', 'unknown')
                if author == main_candidate_mentioned: 
                    continue

                # También aquí usamos búsqueda en toda la fila por consistencia
                text_to_scan = " ".join(row.astype(str).values)

                mentioned_in_post = set()
                
                # Buscar a todos los candidatos en el texto
                for pattern, target_id in search_patterns:
                    if pattern.search(text_to_scan):
                        mentioned_in_post.add(target_id)

                # Regla de Co-mención:
                # Si el candidato del archivo (main) está implícito o mencionado, 
                # y encontramos OTROS candidatos, creamos conexión.
                
                # Asumimos que si estamos en el archivo de "CandidatoA", CandidatoA es parte de la interacción
                if main_candidate_mentioned in mentioned_in_post:
                    # Ya está incluido en el set, no hacemos nada extra
                    pass
                else:
                    # Lo forzamos porque el archivo pertenece a sus menciones
                    mentioned_in_post.add(main_candidate_mentioned)

                if len(mentioned_in_post) > 1:
                    weight = calculate_impact_weight(row.get('likes_count', 0), row.get('comments_count', 0))
                    for mentioned_candidate in mentioned_in_post:
                        if mentioned_candidate != author: # Evitar bucles propios
                             connections.append({'source': author, 'target': mentioned_candidate, 'weight': weight})
                             
        except Exception as e:
            print(f"  ⚠️  Error procesando archivo {file_path}: {e}")

    print(f"  ✅ Se encontraron {len(connections)} conexiones en menciones de terceros.")
    return connections

# --- FUNCIÓN PRINCIPAL ---

def main():
    print("🚀 Iniciando la generación de datos de red (Modo Nombre Real + Búsqueda Total)...")

    try:
        main_df = pd.read_csv(MAIN_DATA_FILE)
        # Leer candidatos base
        with open(PROFILES_FILE, 'r', encoding='utf-8') as f:
            candidates = {line.strip() for line in f if line.strip()}
    except FileNotFoundError as e:
        print(f"❌ Error: No se pudo encontrar un archivo necesario: {e}")
        return

    # 1. Crear el mapa de búsqueda (Username + Nombres Reales -> ID Único)
    search_map = load_search_mapping(candidates, NAMES_MAPPING_FILE)
    
    print(f"  ℹ️  Se buscarán {len(search_map)} términos (usuarios y nombres reales).")

    # 2. Ejecutar análisis
    direct_connections = analyze_direct_interactions(main_df, search_map)
    external_connections = analyze_external_mentions(MENTIONS_FOLDER, search_map)
    all_connections = direct_connections + external_connections

    if not all_connections:
        print("❌ No se encontraron suficientes conexiones.")
        return

    print("\n📊 Parte 3: Consolidando conexiones...")
    df_net = pd.DataFrame(all_connections)

    # Guardar RAW
    try:
        df_net.to_csv(OUTPUT_CSV_RAW_FILE, index=False, float_format='%.2f')
        print(f"  💾 RAW guardado: '{OUTPUT_CSV_RAW_FILE}'")
    except IOError as e:
        print(f"  ⚠️  Error guardando RAW: {e}")

    # Consolidar (Sumar pesos de interacciones repetidas)
    df_final_net = df_net.groupby(['source', 'target']).sum().reset_index()

    # Guardar CONSOLIDADO
    try:
        df_final_net.to_csv(OUTPUT_CSV_CONSOLIDATED_FILE, index=False, float_format='%.2f')
        print(f"  💾 CONSOLIDADO guardado: '{OUTPUT_CSV_CONSOLIDATED_FILE}'")
    except IOError as e:
        print(f"  ⚠️  Error guardando CONSOLIDADO: {e}")

    print("\n🎉 ¡Proceso completado!")

if __name__ == "__main__":
    main()
