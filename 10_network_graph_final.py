import pandas as pd
from pyvis.network import Network
import leidenalg as la
import igraph as ig
import json

# --- CONFIGURACIÓN DE VISUALIZACIÓN ---
PROFILES_FILE = "perfiles_instagram.txt"
INPUT_CSV_FILE = "network_data_consolidated.csv"
NAME_MAP_FILE = "reemplazo_nombres_perfiles_visualizacion.json"
OUTPUT_HTML_FILE = "mapa_de_red_final.html"

# --- Parámetros de Visualización ---
MIN_WEIGHT_THRESHOLD = 50.0
VISUAL_EDGE_MIN = 1
VISUAL_EDGE_MAX = 10
VISUAL_NODE_MIN = 10
VISUAL_NODE_MAX = 50

# --- SCRIPT PRINCIPAL ---
print("🚀 Iniciando la optimización y normalización del mapa de red...")

# --- Carga de Datos y Mapeo de Nombres ---
try:
    df = pd.read_csv(INPUT_CSV_FILE)
    with open(PROFILES_FILE, 'r', encoding='utf-8') as f:
        candidates = {line.strip() for line in f if line.strip()}
    with open(NAME_MAP_FILE, 'r', encoding='utf-8') as f:
        NAME_MAP = json.load(f)
    print("✅ Mapeo de nombres cargado desde el archivo externo.")
except FileNotFoundError as e:
    print(f"❌ Error: No se pudo encontrar un archivo necesario: {e}")
    exit()
except json.JSONDecodeError:
    print(f"❌ Error: El archivo '{NAME_MAP_FILE}' no tiene un formato JSON válido.")
    exit()

# 1. Aplicar el filtro de umbral
df_filtered = df[df['weight'] >= MIN_WEIGHT_THRESHOLD].copy()
print(f"📊 De {len(df)} conexiones, se mantuvieron {len(df_filtered)} tras aplicar el umbral de {MIN_WEIGHT_THRESHOLD}.")
if df_filtered.empty:
    print("❌ No quedaron conexiones después de filtrar. Intenta con un umbral más bajo.")
    exit()

# 2. Detección de comunidades con Leiden
sources = df_filtered['source']
targets = df_filtered['target']
weights = df_filtered['weight'].reset_index(drop=True)
G_ig = ig.Graph(directed=True)
all_nodes = set(sources) | set(targets)
G_ig.add_vertices(list(all_nodes))
G_ig.add_edges(zip(sources, targets))
G_ig.es['weight'] = weights
partition_leiden = la.find_partition(G_ig, la.ModularityVertexPartition, weights='weight')
partition = {G_ig.vs[i]['name']: membership for i, membership in enumerate(partition_leiden.membership)}
print("🧑‍🤝‍🧑 Se detectaron las comunidades (clusters) en la red con el algoritmo de Leiden.")

# 3. Creación del gráfico interactivo
net = Network(height="900px", width="100%", bgcolor="#222222", font_color="white", notebook=True, directed=True)
net.barnes_hut(gravity=-120000, central_gravity=0.1, spring_length=500, spring_strength=0.01, damping=0.09, overlap=0.2)

# --- 👇 OPCIONES UNIFICADAS AQUÍ 👇 ---
# Se añade 'configure': 'true' para mostrar el menú de físicas.
# Esto reemplaza la necesidad de llamar a net.show_buttons()
options = """
var options = {
  "configure": {
    "enabled": true,
    "filter": "physics"
  },
  "interaction": {
    "hover": true,
    "highlightNearest": {
      "enabled": true,
      "degree": 1,
      "hover": false
    },
    "keyboard": {
      "enabled": true
    }
  }
}
"""
net.set_options(options)
# ---------------------------------------------------------

# Lógica de Normalización
node_relevance = df_filtered.groupby('target')['weight'].sum().to_dict()
if node_relevance:
    min_relevance = min(node_relevance.values())
    max_relevance = max(node_relevance.values())
    relevance_range = max_relevance - min_relevance if max_relevance > min_relevance else 1
min_weight = df_filtered['weight'].min()
max_weight = df_filtered['weight'].max()
weight_range = max_weight - min_weight if max_weight > min_weight else 1

# Añadir nodos
for node in all_nodes:
    is_candidate = node in candidates
    node_name = NAME_MAP.get(node, node)
    community = partition.get(node, 0)
    real_relevance = node_relevance.get(node, 0)
    visual_size = VISUAL_NODE_MIN + (real_relevance - min_relevance) / relevance_range * (VISUAL_NODE_MAX - VISUAL_NODE_MIN) if real_relevance > 0 else VISUAL_NODE_MIN

    node_mass = visual_size / 10
    font_size = 35 if is_candidate else 15
    node_color = f"hsl({community * 360 / len(set(partition.values()))}, 70%, 50%)"

    net.add_node(
        node_name,
        label=node_name,
        size=visual_size,
        mass=node_mass,
        color={
            'background': node_color,
            'border': '#000000',
            'highlight': {
                'background': node_color,
                'border': '#FFFFFF'
            }
        },
        borderWidth=2,
        title=f"Perfil: {node}<br>Comunidad: {community}<br><b>Relevancia Real: {real_relevance:.2f}</b>",
        font={'size': font_size, 'strokeWidth': 3, 'strokeColor': '#000000' if is_candidate else 'none'}
    )

# Añadir aristas
for _, row in df_filtered.iterrows():
    source_name = NAME_MAP.get(row['source'], row['source'])
    target_name = NAME_MAP.get(row['target'], row['target'])
    real_weight = row['weight']
    visual_weight = VISUAL_EDGE_MIN + (real_weight - min_weight) / weight_range * (VISUAL_EDGE_MAX - VISUAL_EDGE_MIN)

    net.add_edge(
        source_name,
        target_name,
        value=visual_weight,
        title=f"<b>Impacto Real: {real_weight:.2f}</b>",
        color="#848484"
    )

print("🎨 Generando el archivo HTML final...")
# Ya no es necesaria la siguiente línea:
# net.show_buttons(filter_=['physics'])
net.save_graph(OUTPUT_HTML_FILE)

print(f"\n🎉 ¡Éxito! El mapa final ha sido guardado en '{OUTPUT_HTML_FILE}'.")