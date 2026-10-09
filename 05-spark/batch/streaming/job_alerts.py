from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

# Iniciar Spark
spark = SparkSession.builder.appName("NexoUrbano_Alerts").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Definir el esquema JSON
esquema = StructType([
    StructField("ts", TimestampType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("event", StringType(), True),
    StructField("battery_pct", IntegerType(), True)
])

# Leer archivos JSON a medida que caen en la carpeta
eventos = spark.readStream.schema(esquema).json("/opt/spark-data/stream/")

# Aplicar Watermark de 1 min y filtrar eventos
eventos_con_retraso = eventos \
    .withWatermark("ts", "1 minute") \
    .filter(col("event").isin("payment_fail", "battery_low"))

# Agrupar por ventana de tiempo (2 min window, 30 seg slide)
alertas = eventos_con_retraso \
    .groupBy(
        window(col("ts"), "2 minutes", "30 seconds"),
        col("event")
    ).count()

# Sink: Escribir resultado en la consola para el HMV
#query = alertas.writeStream \
 #   .outputMode("update") \
  #  .format("console") \
   # .option("truncate", "false") \
    #.start()

#query.awaitTermination()
# 5. Sink 1: Consola (Para tu video)
query_consola = alertas.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# 5. Sink 2: Guardar archivo físico (Para cumplir la consigna)
query_parquet = alertas.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/opt/spark-lake/gold_alerts") \
    .option("checkpointLocation", "/opt/spark-lake/checkpoints") \
    .start()

spark.streams.awaitAnyTermination()