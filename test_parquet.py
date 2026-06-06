import pandas as pd

# 1. Leer el archivo Parquet
df = pd.read_parquet('part-00000-913c0a5f-3675-4c78-b1cc-45c54ecd8ccc-c000.snappy.parquet')

# 2. Configurar Pandas para que no recorte filas ni columnas
pd.set_option('display.max_columns', None)  # Muestra todas las columnas
pd.set_option('display.max_rows', 10)       # Asegura que muestre hasta 10 filas
pd.set_option('display.width', 1000)        # Evita que las columnas salten de línea hacia abajo

# 3. Obtener las primeras 10 filas
top_10 = df.head(10)

# 4. Guardar la representación en texto dentro de un fichero aparte
with open('resultado.txt', 'w', encoding='utf-8') as f:
    f.write(top_10.to_string())

print("¡Listo! El contenido se ha guardado en 'resultado.txt'")
