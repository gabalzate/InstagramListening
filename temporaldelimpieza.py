import pandas as pd
import numpy as np

def contar_filas_duplicadas(file_path):
    """
    Lee un archivo CSV y cuenta el número total de filas que son duplicados exactos
    de otras filas en el DataFrame.

    Args:
        file_path (str): El nombre del archivo CSV ("base_de_datos_instagram.csv").

    Returns:
        int: El número total de filas duplicadas encontradas.
    """
    try:
        # 1. Cargar el archivo CSV
        df = pd.read_csv(file_path)

        print(f"Número total de filas en el archivo: {len(df)}")

        # 2. Identificar todas las filas que son duplicados.
        # df.duplicated() devuelve una Serie booleana.
        # 'keep=False' marca *todas* las filas que son duplicados (incluyendo la primera aparición).
        # Esto es útil para obtener un conteo total de filas involucradas en una duplicación.
        duplicados_completos = df.duplicated(keep=False)

        # 3. Contar el total de filas que tienen un valor True (son duplicados).
        # Si queremos contar SOLO las réplicas (es decir, excluyendo la primera aparición),
        # usaríamos df.duplicated(keep='first').sum().
        # Pero para saber cuántas filas *en total* están repetidas:
        conteo_total_duplicados = duplicados_completos.sum()

        # 4. Calcular el número de filas únicas.
        filas_unicas = len(df) - (conteo_total_duplicados if conteo_total_duplicados == 0 else (conteo_total_duplicados - df.duplicated(keep='first').sum()))
        
        # Una forma más sencilla de calcular las filas únicas:
        # filas_unicas = len(df) - df.duplicated(keep='first').sum()


        print(f"Número total de filas COMPLETAMENTE duplicadas (incluyendo la primera aparición): {conteo_total_duplicados}")
        print(f"Número de filas que son réplicas (duplicados después de la primera): {df.duplicated(keep='first').sum()}")

        return df.duplicated(keep='first').sum()

    except FileNotFoundError:
        print(f"❌ ERROR: El archivo '{file_path}' no fue encontrado.")
        return 0
    except Exception as e:
        print(f"❌ Ocurrió un error: {e}")
        return 0

# --- Ejecución Segura del Código ---
def main():
    nombre_archivo = "base_de_datos_instagram.csv"
    
    # La variable 'conteo_replicas' guardará el número de filas que son la 2da, 3ra, etc., aparición.
    conteo_replicas = contar_filas_duplicadas(nombre_archivo)

    print(f"\n--- Resultado Final ---")
    print(f"El archivo tiene un total de **{conteo_replicas}** filas que son réplicas exactas.")

if __name__ == "__main__":
    main()
