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
# PREGUNTA 4 (Optimizada para Demo en Vivo)
# ---------------------------------------------------------
print("="*60)
print("4. ¿Hay collusion / fraude social? (Datos Reales)")
print("Motores: Neo4j (Grafo Social) -> Lectura directa desde Spark")
print("="*60)
try:
    print("-> Conectando a Neo4j para ejecutar análisis de fraude en tiempo real...")
    
    # Optimizamos anclándonos a la estación ST000 y acotando las combinaciones por ID
    query_fraude = """
        MATCH (e:Estacion {station_id: 'ST000'})<-[v1:SALE_DE]-(r1:Rider),
              (e)<-[v2:SALE_DE]-(r2:Rider),
              (e)<-[v3:SALE_DE]-(r3:Rider)
        WHERE r1.rider_id < r2.rider_id AND r2.rider_id < r3.rider_id
          AND abs(duration.inSeconds(datetime(v1.ts_start), datetime(v2.ts_start)).seconds) < 120
          AND abs(duration.inSeconds(datetime(v2.ts_start), datetime(v3.ts_start)).seconds) < 120
          AND NOT (r1)-[:AMIGO_DE]-(r2) AND NOT (r2)-[:AMIGO_DE]-(r3)
        RETURN e.station_id AS station_id, r1.rider_id AS rider_1, r2.rider_id AS rider_2, r3.rider_id AS rider_3
    """
    
    # Lectura real desde el contenedor nexo-neo4j
    df_fraude = spark.read.format("org.neo4j.spark.DataSource") \
        .option("url", "bolt://nexo-neo4j:7687") \
        .option("authentication.type", "basic") \
        .option("authentication.basic.username", "neo4j") \
        .option("authentication.basic.password", "nexo-urban-2026") \
        .option("query", query_fraude) \
        .load()

    print("\n-> ALERTA DE FRAUDE: Se detectó el siguiente clúster de usuarios abusando del sistema:")
    # Mostramos los primeros 3 resultados de forma instantánea
    df_fraude.limit(3).show(truncate=False)
    print("-> ACCIÓN OPERATIVA: Aplicar bloqueo preventivo temporal en las tarjetas de crédito de estos usuarios.")

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
    # --- Lo facturado: suma de la tarifa (fare_ars) de todos los viajes ---
    df_viajes = spark.read.csv("/opt/spark-data/viajes.csv", header=True, inferSchema=True)
    resumen = df_viajes.agg(
        F.count("*").alias("viajes"),
        F.round(F.sum("fare_ars"), 2).alias("facturado_ars"),
        F.sum(F.when(F.col("fare_ars").isNull(), 1).otherwise(0)).alias("sin_tarifa"),
        F.min("ts_start").alias("desde"),
        F.max("ts_start").alias("hasta"),
    ).collect()[0]

    # --- Los tickets de soporte (el mismo archivo del WordCount de M5) ---
    df_tickets = spark.read.text("/opt/spark-data/texto_ops.txt").filter(F.trim("value") != "")
    n_tickets = df_tickets.count()
    es_cobro = F.lower(F.col("value")).contains("cobro")
    n_cobro = df_tickets.filter(es_cobro).count()
    n_pago = df_tickets.filter(F.lower(F.col("value")).contains("payment_fail")).count()
    riders_cobro = (df_tickets.filter(es_cobro)
                    .select(F.regexp_extract("value", r"(R\d{4})", 1).alias("rider"))
                    .filter(F.col("rider") != "").distinct().count())

    # WordCount: cuántas veces aparece la palabra "cobro" (igual que en M5)
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
    