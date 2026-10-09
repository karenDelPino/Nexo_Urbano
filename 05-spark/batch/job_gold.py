# job_gold.py - M8: Spark batch (Bronze -> Silver -> Gold)
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .appName("NexoUrbano-Gold")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

RUTA_VIAJES = "/opt/spark-data/viajes.csv"      # Bronze: CSV crudo
RUTA_SILVER = "/opt/spark-lake/silver/viajes"   # Silver: Parquet limpio

# --- 1. Bronze: leer el CSV crudo ---
bronze = spark.read.csv(RUTA_VIAJES, header=True, inferSchema=True)
n_bronze = bronze.count()
print(f"Filas en Bronze: {n_bronze}")
bronze.printSchema()

# --- 2. Tipos y columnas derivadas ---
# ts_start viene en UTC; lo pasamos a hora de Buenos Aires para agrupar por hora
viajes = (bronze
    .withColumn("ts_start", F.to_timestamp("ts_start"))
    .withColumn("ts_end", F.to_timestamp("ts_end"))
    .withColumn("ts_local", F.from_utc_timestamp("ts_start", "America/Argentina/Buenos_Aires"))
    .withColumn("fecha", F.to_date("ts_local"))
    .withColumn("hora", F.hour("ts_local"))
    .withColumn("duracion_min", F.col("duration_s") / 60)
    .withColumn("vel_kmh", F.col("km") / (F.col("duration_s") / 3600)))

# --- 3. Calidad de dato como código: cada regla marca lo INVALIDO ---
reglas = {
    "duracion_fuera_de_rango": F.col("duration_s").isNull() | ~F.col("duration_s").between(30, 4 * 3600),
    "ids_nulos": F.col("rider_id").isNull() | F.col("vehicle_id").isNull() | F.col("station_start").isNull(),
    "km_invalido": F.col("km").isNull() | (F.col("km") <= 0),
    "fin_antes_que_inicio": F.col("ts_end") <= F.col("ts_start"),
}
for nombre, condicion in reglas.items():
    print(f"Rechazados por {nombre}: {viajes.filter(condicion).count()}")

# un viaje es invalido si rompe cualquier regla (un nulo cuenta como invalido)
invalido = F.coalesce(reduce(lambda a, b: a | b, reglas.values()), F.lit(True))
silver = viajes.filter(~invalido).cache()
n_silver = silver.count()
print(f"Filas en Silver: {n_silver} (rechazadas en total: {n_bronze - n_silver})")

# distribución de velocidad: sirve para decidir un umbral de velocidad máxima
silver.select("km", "duracion_min", "vel_kmh").describe().show()

# --- 4. Guardar Silver en Parquet, particionado por fecha ---
silver.write.mode("overwrite").partitionBy("fecha").parquet(RUTA_SILVER)
print("Silver guardado en", RUTA_SILVER)

# --- 5. Diagnóstico de velocidad (km y duración parecen poco coherentes) ---
for umbral in (25, 40, 60):
    n = silver.filter(F.col("vel_kmh") > umbral).count()
    print(f"Viajes con velocidad > {umbral} km/h: {n} ({n / n_silver:.1%})")

# rango de fechas (explica por qué el primer día tiene pocos viajes)
silver.agg(F.min("ts_start"), F.max("ts_start")).show(truncate=False)

# --- 6. Clima: qué códigos hay y con qué frecuencia ---
silver.groupBy("clima_codigo").count().orderBy("clima_codigo").show()

# --- 7. Join con riders ---
RUTA_RIDERS = "/opt/spark-data/riders.csv"
RUTA_GOLD = "/opt/spark-lake/gold"

riders = spark.read.csv(RUTA_RIDERS, header=True, inferSchema=True)
print(f"Riders cargados: {riders.count()}")

# integridad: viajes cuyo rider_id no existe en riders.csv
sin_rider = silver.join(riders, "rider_id", "left_anti").count()
print(f"Viajes con rider_id inexistente en riders.csv: {sin_rider}")

viajes_rider = silver.join(riders.select("rider_id", "plan", "ciudad"), "rider_id", "left")

# --- 8. Gold: métricas diarias (con % de clima adverso) ---
ADVERSO = F.col("clima_codigo") >= 51   # lluvia, llovizna, chubascos (códigos WMO)

gold_ops_diaria = (viajes_rider.groupBy("fecha")
    .agg(F.count("*").alias("viajes"),
         F.round(F.sum("duracion_min"), 1).alias("minutos"),
         F.round(F.sum("km"), 1).alias("km"),
         F.round(F.sum("fare_ars"), 2).alias("ingresos_ars"),
         F.round(100 * F.avg(F.when(ADVERSO, 1).otherwise(0)), 1).alias("pct_clima_adverso"))
    .orderBy("fecha"))
gold_ops_diaria.show(10)
gold_ops_diaria.write.mode("overwrite").parquet(f"{RUTA_GOLD}/gold_ops_diaria")

# --- 9. Gold: top 10 estaciones de origen ---
top_estaciones = (silver.groupBy("station_start")
    .agg(F.count("*").alias("viajes"))
    .orderBy(F.desc("viajes")).limit(10))
top_estaciones.show()
top_estaciones.write.mode("overwrite").parquet(f"{RUTA_GOLD}/top_estaciones_origen")

# --- 10. Gold: viajes e ingresos por plan de rider (usa el join) ---
gold_por_plan = (viajes_rider.groupBy("plan")
    .agg(F.count("*").alias("viajes"),
         F.round(F.sum("fare_ars"), 2).alias("ingresos_ars"),
         F.round(F.avg("duracion_min"), 1).alias("duracion_media_min"))
    .orderBy(F.desc("viajes")))
gold_por_plan.show()
gold_por_plan.write.mode("overwrite").parquet(f"{RUTA_GOLD}/gold_por_plan")

# --- 11. Plan físico (explain) ---
# Consulta 1: lectura de Silver con filtro de fecha (partition pruning)
consulta = (spark.read.parquet(RUTA_SILVER)
    .filter(F.col("fecha") >= "2026-05-01")
    .groupBy("station_start").count())
consulta.explain(True)

# Consulta 2: el join con riders (buscar el tipo de join en el plan)
gold_por_plan.explain()

spark.stop()