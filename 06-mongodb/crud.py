import pymongo
import random
from pprint import pprint

# 1. Conectar a Mongo (Usamos el puerto 27018 de tu Docker)
client = pymongo.MongoClient("mongodb://localhost:27018/")
db = client["nexo_db"]
coleccion = db["productos"]

# Limpiar colección para poder correr el script varias veces sin duplicar
coleccion.drop()

# 2. Generar 300 documentos con estructuras anidadas
categorias = ["pase", "recarga", "casco", "seguro"]
ciudades = ["Mendoza", "CABA", "Rosario", "Cordoba"]

documentos = []
for i in range(1, 301):
    doc = {
        "sku": f"PRD-{i:03d}",
        "nombre": f"Producto Comercial {i}",
        "categoria": random.choice(categorias),
        "detalles": {
            "stock": random.randint(0, 100),
            "tags": ["movilidad", random.choice(["premium", "basico", "eco"])],
            "precios_ciudad": {
                "Mendoza": random.randint(1000, 5000),
                "CABA": random.randint(1200, 6000)
            }
        },
        "activo": True
    }
    documentos.append(doc)

# INSERT MANY (Cumple el >= 30 docs)
coleccion.insert_many(documentos)
print("✅ 1. Se insertaron 300 documentos de catálogo.")

# BÚSQUEDA ANTES DEL ÍNDICE
print("\n--- Búsqueda ANTES del índice ---")
stats_antes = db.command("explain", {"find": "productos", "filter": {"categoria": "casco", "detalles.tags": "premium"}}, verbosity="executionStats")
print(f"Milisegundos: {stats_antes['executionStats']['executionTimeMillis']} ms")
print(f"Documentos examinados: {stats_antes['executionStats']['totalDocsExamined']}")

# CREAR ÍNDICE (En categoría + tags)
coleccion.create_index([("categoria", 1), ("detalles.tags", 1)])
print("\n✅ 2. Índice creado con éxito en categoría y tags.")

# BÚSQUEDA DESPUÉS DEL ÍNDICE
print("\n--- Búsqueda DESPUÉS del índice ---")
stats_despues = db.command("explain", {"find": "productos", "filter": {"categoria": "casco", "detalles.tags": "premium"}}, verbosity="executionStats")
print(f"Milisegundos: {stats_despues['executionStats']['executionTimeMillis']} ms")
print(f"Documentos examinados: {stats_despues['executionStats']['totalDocsExamined']}")

# FIND (Buscar y mostrar un producto filtrado)
print("\n--- 3. FIND: Muestra de un documento filtrado ---")
un_producto = coleccion.find_one({"categoria": "casco"})
pprint(un_producto)

# UPDATE ONE (Actualizar el precio en Mendoza de un artículo)
coleccion.update_one(
    {"sku": "PRD-001"},
    {"$set": {"detalles.precios_ciudad.Mendoza": 9999}}
)
print("\n✅ 4. UPDATE ONE: Precio actualizado correctamente.")

# DELETE ONE (Borrar un SKU que fue descontinuado)
coleccion.delete_one({"sku": "PRD-300"})
print("✅ 5. DELETE ONE: Producto descontinuado borrado de la base.")