from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# 1. Inicializar la sesión de Spark explícitamente
spark = SparkSession.builder \
    .appName("NexoUrbano-M14-DecisionOps") \
    .getOrCreate()

# 2. Tu código original (ahora la variable 'spark' sí existe)
df_clima = spark.read.parquet("/opt/spark-lake/silver/silver_clima_horario")

# Mostrar las primeras líneas para verificar la lectura
df_clima.show(5)