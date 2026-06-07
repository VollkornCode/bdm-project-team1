from pymilvus import MilvusClient

# 1. Inicializar el cliente apuntando a tu servidor
client = MilvusClient(uri="http://localhost:19530")

collection_name = "recipe_texts"

if client.has_collection(collection_name=collection_name):
    # Forzar consolidación de datos en disco
    client.flush(collection_name=collection_name)
    
    # 2. Obtener y mostrar el conteo total
    stats = client.get_collection_stats(collection_name=collection_name)
    total_elementos = stats.get("row_count", 0)
    print(f"Total de elementos indexados: {total_elementos}\n")
    
    if total_elementos > 0:
        print(f"--- Listando los primeros elementos de '{collection_name}' ---")
        
        # 3. Hacer una consulta (Query) para traer los registros
        # filter="recipe_id != ''" actúa como un 'traer todo' ya que todos tienen ID
        # output_fields nos permite elegir qué columnas ver (excluimos 'embedding' para no llenar la consola de números)
        resultados = client.query(
            collection_name=collection_name,
            filter="recipe_id != ''",
            output_fields=["recipe_id", "title"],
            limit=20  # Cambia este número para ver más o menos filas (ej. 50, 100...)
        )
        
        # 4. Iterar y mostrar los elementos de forma limpia
        for i, registro in enumerate(resultados, start=1):
            id_receta = registro.get("recipe_id")
            titulo = registro.get("title")
            print(f"{i}. [ID: {id_receta}] -> {titulo}")
            
    else:
        print("La colección está vacía.")
else:
    print(f"La colección '{collection_name}' no existe.")