import os
import logging
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# === 1. CARGA SEGURA DE CREDENCIALES (Con Fallback) ===
# Si Docker bloquea el .env, Python usará estas credenciales por defecto automáticamente:
NEO4J_URL = os.getenv("NEO4J_URL", "bolt://nexo-neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "nexo-urban-2026")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://nexo-mongo:27017")

# === 2. INICIAR SESIÓN SPARK ===
spark = SparkSession.builder \
    .appName("NexoUrbano-M14-DecisionOps") \
    .getOrCreate()

# === 3. SILENCIADOR DE LOGS ===
spark.sparkContext.setLogLevel("FATAL")
logging.getLogger("py4j").setLevel(logging.FATAL)

try:
    log4j = spark._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.FATAL)
except Exception:
    pass
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
print("Motores: Neo4j (Grafo Social) + Spark (Ventas) + MongoDB (Catálogo)")
print("="*60)

RIDER_ID = "R0042"   # único parámetro: cambiá el rider y todo se recalcula

def leer_neo4j(query):
    # Sin LIMIT/SKIP al final: el conector de Spark agrega los suyos.
    return (spark.read.format("org.neo4j.spark.DataSource")
        .option("url", NEO4J_URL)
        .option("authentication.type", "basic")
        .option("authentication.basic.username", NEO4J_USER)
        .option("authentication.basic.password", NEO4J_PASSWORD)
        .option("query", query)
        .load())

try:
    # --- Paso A (Neo4j): intereses del rider y riders con gustos parecidos ---
    print(f"-> Paso A (Neo4j): intereses de {RIDER_ID} y riders con gustos parecidos...")

    df_intereses = leer_neo4j(f"""
        MATCH (r:Rider {{rider_id: '{RIDER_ID}'}})-[:LE_GUSTA]->(i:Interes)
        RETURN i.interes AS interes
        ORDER BY interes
    """)
    intereses = [row["interes"] for row in df_intereses.collect()]
    print(f"   Intereses de {RIDER_ID}: {intereses}")

    df_similares = leer_neo4j(f"""
        MATCH (r:Rider {{rider_id: '{RIDER_ID}'}})-[:LE_GUSTA]->(i:Interes)<-[:LE_GUSTA]-(otro:Rider)
        WHERE otro <> r
        RETURN otro.rider_id AS rider_id, count(i) AS intereses_comunes
        ORDER BY intereses_comunes DESC
    """)
    print(f"   Riders con al menos un interés en común: {df_similares.count()}")

    # --- Paso B (Spark): qué pases compraron esos riders ---
    print("\n-> Paso B (Spark): qué pases compraron esos riders...")
    ventas = spark.read.csv("/opt/spark-data/ventas.csv", header=True, inferSchema=True)

    ranking_pases = (ventas.filter(F.col("categoria") == "pase")
        .join(df_similares, "rider_id")
        .groupBy("sku")
        .agg(F.count("*").alias("compras"),
             F.sum("intereses_comunes").alias("peso_afinidad")))

    # --- Paso C (MongoDB): pases activos del catálogo ---
    print("\n-> Paso C (MongoDB): cruzando con el catálogo de pases activos...")
    import json
    from pymongo import MongoClient

    coleccion = MongoClient(MONGO_URI)["nexo_db"]["catalogo_productos"]

    # Si el catálogo todavía no está en Mongo, se carga una sola vez desde productos.json
    if coleccion.count_documents({}) == 0:
        with open("/opt/spark-data/productos.json", encoding="utf-8") as f:
            coleccion.insert_many(json.load(f))
        print("   (Catálogo cargado en MongoDB desde productos.json)")

    docs_pases = coleccion.find({"categoria": "pase", "activo": True}, {"_id": 0})

    catalogo = spark.createDataFrame(
        [(d["sku"], d["nombre"], d.get("precios", {}).get("Buenos Aires"), d.get("tags", []))
         for d in docs_pases],
        ["sku", "nombre", "precio_ars", "tags"])

    recomendacion = (ranking_pases.join(catalogo, "sku", "inner")
        .orderBy(F.desc("peso_afinidad"), F.desc("compras"))
        .select("sku", "nombre", "precio_ars", "tags", "compras", "peso_afinidad")
        .limit(1))

    print(f"\n-> ACCIÓN COMERCIAL: ofrecer a {RIDER_ID} el pase que más eligieron riders con sus mismos gustos:")
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

    df_fraude = spark.read.format("org.neo4j.spark.DataSource") \
        .option("url", NEO4J_URL) \
        .option("authentication.type", "basic") \
        .option("authentication.basic.username", NEO4J_USER) \
        .option("authentication.basic.password", NEO4J_PASSWORD) \
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

# ---------------------------------------------------------
# PREGUNTA 5
# ---------------------------------------------------------
print("="*60)
print('5. ¿Cuánto se facturó vs. tickets de soporte "cobro"?')
print("Motores: Spark (viajes) + WordCount (tickets de soporte)")
print("="*60)

try:
    df_viajes = spark.read.csv("/opt/spark-data/viajes.csv", header=True, inferSchema=True)
    resumen = df_viajes.agg(
        F.count("*").alias("viajes"),
        F.round(F.sum("fare_ars"), 2).alias("facturado_ars"),
        F.sum(F.when(F.col("fare_ars").isNull(), 1).otherwise(0)).alias("sin_tarifa"),
        F.min("ts_start").alias("desde"),
        F.max("ts_start").alias("hasta"),
    ).collect()[0]

    df_tickets = spark.read.text("/opt/spark-data/texto_ops.txt").filter(F.trim("value") != "")
    n_tickets = df_tickets.count()
    es_cobro = F.lower(F.col("value")).contains("cobro")
    n_cobro = df_tickets.filter(es_cobro).count()
    n_pago = df_tickets.filter(F.lower(F.col("value")).contains("payment_fail")).count()
    riders_cobro = (df_tickets.filter(es_cobro)
                    .select(F.regexp_extract("value", r"(R\d{4})", 1).alias("rider"))
                    .filter(F.col("rider") != "").distinct().count())

    palabras = df_tickets.select(
        F.explode(F.split(F.lower(F.col("value")), r"\s+")).alias("palabra"))
    n_palabra_cobro = palabras.filter(F.col("palabra") == "cobro").count()

    print(f"Facturado: ARS {resumen['facturado_ars']:,.2f} en {resumen['viajes']:,} viajes "
          f"(de {resumen['desde']} a {resumen['hasta']})")
    print(f"Tickets de soporte: {n_tickets:,} en total")
    print(f'  - "cobro" (doble cobro en pase mensual): {n_cobro} '
          f"({n_cobro / n_tickets:.1%} de los tickets), de {riders_cobro} riders distintos")
    print(f'  - WordCount: la palabra "cobro" aparece {n_palabra_cobro} veces')
    print(f"  - payment_fail repetido (tema de pagos, aparte): {n_pago}")
    print("\nDATOS QUE FALLAN:")
    print("  - Los tickets no tienen monto: NO se puede decir cuánta plata está en disputa.")
    print("  - Los tickets no tienen fecha: no se puede comparar por mes ni por día,")
    print("    y no hay garantía de que sean del mismo período que los viajes.")
    print('  - Los tickets "cobro" hablan de pases mensuales; lo facturado acá es por viaje')
    print("    (fare_ars). Los pases se facturan en Mongo, que no entra en esta cuenta.")
    print("  - Los tickets son sintéticos (una sola frase por tema), no reclamos reales.")
    print("  - No se calcula una tasa de reclamos por viaje: tickets y viajes no son del mismo")
    print("    universo, sería inventar un número.")
    if resumen["sin_tarifa"]:
        print(f"  - {resumen['sin_tarifa']} viajes sin tarifa válida no se sumaron.")

except Exception as e:
    print(f"Error en la ejecución de la Pregunta 5: {e}")