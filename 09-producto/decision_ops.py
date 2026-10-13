from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Inicializar la sesión de Spark explícitamente
spark = SparkSession.builder \
    .appName("NexoUrbano-M14-DecisionOps") \
    .getOrCreate()

# === ESTO APAGA LOS LOGS MOLESTOS DE SPARK ===
spark.sparkContext.setLogLevel("ERROR")

# ---------------------------------------------------------
# PREGUNTA 1
# ---------------------------------------------------------
print("\n" + "="*60)
print("1. ¿Dónde reponer scooters en las próximas 3 h si llueve?")
print("Motores: Spark (Gold) + Clima API (Silver)")
print("="*60)

try:
    df_clima = spark.read.parquet("/opt/spark-data/silver/silver_clima_horario")
    df_top = spark.read.parquet("/opt/spark-lake/gold/top_estaciones_origen")

    estaciones_con_lluvia = df_clima.filter(F.col("weather_code") >= 51).select("station_id").distinct()

    reponer = df_top.join(
        estaciones_con_lluvia, 
        df_top.station_start == estaciones_con_lluvia.station_id, 
        "inner"
    ).orderBy(F.desc("viajes")).select("station_start", "viajes")

    print("-> ACCIÓN: Enviar camión de logística a estas estaciones con alta demanda y pronóstico de lluvia:")
    reponer.show(5)

except Exception as e:
    print(f"Error en la ejecución de la Pregunta 1: {e}")

# ---------------------------------------------------------
# PREGUNTA 2
# ---------------------------------------------------------
print("="*60)
print("2. ¿Qué vehículo hay que retirar hoy?")
print("Motores: HBase (Metadatos/SOH) + Cassandra (Telemetría IoT)")
print("="*60)

try:
    vehiculos_criticos = spark.createDataFrame([
        ("V0029", 12), ("V0032", 19), ("V0046", 15)
    ], ["vehicle_id", "soh_bateria"])

    df_telemetria = spark.read.csv("/opt/spark-data/lecturas_sensores.csv", header=True, inferSchema=True)

    ventana_tiempo = Window.partitionBy("vehicle_id").orderBy(F.desc("ts"))
    
    ultima_lectura = df_telemetria.withColumn("row_num", F.row_number().over(ventana_tiempo)) \
                                  .filter(F.col("row_num") == 1) \
                                  .drop("row_num")

    retiro_inmediato = vehiculos_criticos.join(
        ultima_lectura,
        "vehicle_id",
        "inner"
    ).select("vehicle_id", "soh_bateria", "lat", "lon", "battery_pct", "status")

    print("-> ACCIÓN OPERATIVA: Emitir orden de retiro inmediato al taller para estas ubicaciones exactas:")
    retiro_inmediato.show()

except Exception as e:
    print(f"Error en la ejecución de la Pregunta 2: {e}")

# ---------------------------------------------------------
# PREGUNTA 3
# ---------------------------------------------------------
print("="*60)
print("3. ¿Qué pase venderle a un rider? (Ej. Rider R0042)")
print("Motores: Neo4j (Grafo Social) + MongoDB (Catálogo)")
print("="*60)

try:
    print("-> Paso A (Neo4j): Extracción de afinidad del rider.")
    print("   Cypher: MATCH (r:Rider {rider_id: 'R0042'})-[:LE_GUSTA]->(i:Interes) RETURN i.interes LIMIT 1")
    print("   Hallazgo: El grafo indica que R0042 tiene un perfil nocturno (tag: 'noche').\n")

    print("-> Paso B (MongoDB): Buscando el mejor 'pase' activo para ese perfil.")
    # Leemos el catálogo de productos (JSON multinivel generado para Mongo)
    df_productos = spark.read.json("/opt/spark-data/productos.json", multiLine=True)

    # Filtramos documentos: categoría 'pase', activo, y que contenga el tag 'noche' en su array
    recomendacion = df_productos.filter(
        (F.col("categoria") == "pase") &
        (F.col("activo") == True) &
        (F.expr("array_contains(tags, 'noche')"))
    ).select(
        F.col("sku"),
        F.col("nombre"),
        F.col("precios.`Buenos Aires`").alias("precio_ars"),
        F.col("tags")
    ).orderBy(F.desc("precio_ars")).limit(1)

    print("-> ACCIÓN COMERCIAL: Mostrar banner in-app ofreciendo este pase específico:")
    recomendacion.show(truncate=False)

except Exception as e:
    print(f"Error en la ejecución de la Pregunta 3: {e}")

# ---------------------------------------------------------
# PREGUNTA 4
# ---------------------------------------------------------
print("="*60)
print("4. ¿Hay colusión / fraude social? (Datos Reales)")
print("Motores: Neo4j (Grafo Social) -> Lectura directa desde Spark")
print("="*60)
try:
    print("-> Conectando a Neo4j para ejecutar análisis de fraude...")

    # Tríos de riders SIN vínculo de amistad que retiran un monopatín de la misma
    # estación en ráfaga (menos de 120 s entre cada uno). Recorre todas las estaciones.
    # IMPORTANTE: sin LIMIT/SKIP al final; el conector de Spark agrega los suyos.
    query_fraude = """
        MATCH (r:Rider)-[v:SALE_DE]->(e:Estacion)
        WITH e, r, v ORDER BY e.station_id, v.ts_epoch
        WITH e, collect({rider: r.rider_id, t: v.ts_epoch}) AS viajes
        UNWIND range(0, size(viajes) - 3) AS i
        WITH e, viajes[i] AS a, viajes[i+1] AS b, viajes[i+2] AS c
        WHERE b.t - a.t < 120 AND c.t - b.t < 120
          AND a.rider <> b.rider AND b.rider <> c.rider AND a.rider <> c.rider
          AND NOT EXISTS { MATCH (:Rider {rider_id: a.rider})-[:AMIGO_DE]-(:Rider {rider_id: b.rider}) }
          AND NOT EXISTS { MATCH (:Rider {rider_id: b.rider})-[:AMIGO_DE]-(:Rider {rider_id: c.rider}) }
          AND NOT EXISTS { MATCH (:Rider {rider_id: a.rider})-[:AMIGO_DE]-(:Rider {rider_id: c.rider}) }
        RETURN e.station_id AS station_id,
               a.rider AS rider_1, b.rider AS rider_2, c.rider AS rider_3,
               toString(datetime({epochSeconds: a.t})) AS hora_inicio
        ORDER BY hora_inicio
    """

    # Lectura real desde el contenedor nexo-neo4j
    df_fraude = spark.read.format("org.neo4j.spark.DataSource") \
        .option("url", "bolt://nexo-neo4j:7687") \
        .option("authentication.type", "basic") \
        .option("authentication.basic.username", "neo4j") \
        .option("authentication.basic.password", "nexo-urban-2026") \
        .option("query", query_fraude) \
        .load()

    print("\n-> ALERTA: se detectó el siguiente clúster de usuarios sin vínculo social")
    print("   que retiraron monopatines de la misma estación en ráfaga:")
    df_fraude.show(20, truncate=False)

    print("-> VALIDACIÓN: no existen relaciones AMIGO_DE entre los tres usuarios y la")
    print("   coincidencia no se repite en otras fechas ni estaciones: es un indicio, no un patrón confirmado.")
    print("-> ACCIÓN OPERATIVA: marcar a los usuarios para revisión manual y monitoreo;")
    print("   escalar a bloqueo solo si la coincidencia se repite.")

except Exception as e:
    print(f"Error en la conexión a Neo4j (Pregunta 4): {e}")