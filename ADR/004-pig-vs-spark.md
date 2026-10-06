# ADR 004 - M7: Limpieza de viajes con Pig Latin y su equivalente en Spark

## Decisión
Para limpiar `viajes` se escribió un script en **Pig Latin** (`tools/clean_viajes.pig`) que lee el CSV de bronze, descarta el encabezado, filtra duraciones absurdas (`duration_s < 30` o `> 14400`, es decir, más de 4 horas), calcula la columna `distancia_nula` y escribe el resultado en `/nexo/silver/clean_viajes`. Se reimplementó la misma lógica en **Spark** (abajo) y, para producción en 2026, se elige **Spark**. Pig queda como ejercicio de legado.

## Resultado de la corrida en Pig
Pig 0.17.0 corrió sobre Hadoop 3.3.6 en modo local (`job_local`), dentro del contenedor `nexo-hadoop`. Leyó 2000 viajes (más el encabezado) y guardó **1960**: los 40 restantes quedaron fuera por duración. De esos 1960, **98** tienen `distancia_nula = 1` y 1862 tienen 0. Evidencia: `evidencias/pig/m7-pig-run.log`.

Se definió `distancia_nula = 1` cuando la estación de salida y de llegada es la misma, o `km` es 0, o `km` está vacío. Es un supuesto nuestro: en esta muestra ningún `km` es 0 (el mínimo es 0.001), así que usar solo `km` habría dado cero casos. Además, `km` no coincide con las coordenadas (hay viajes con 0.1 km entre puntos separados por casi 2 km), por lo que no es confiable como única fuente.

## Equivalente en Spark
Versión de referencia, no ejecutada en este sandbox:

```python
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("clean_viajes").getOrCreate()

viajes = spark.read.csv("/nexo/bronze/viajes/viajes_muestra.csv",
                        header=True, inferSchema=True)

clean_viajes = (
    viajes
    .filter((F.col("duration_s") >= 30) & (F.col("duration_s") <= 14400))
    .withColumn(
        "distancia_nula",
        F.when(
            (F.col("station_start") == F.col("station_end"))
            | F.col("km").isNull()
            | (F.col("km") == 0),
            1,
        ).otherwise(0),
    )
)

clean_viajes.write.mode("overwrite").csv("/nexo/silver/clean_viajes_spark", header=True)
```

Diferencias visibles: en Spark el encabezado se resuelve con `header=True` (en Pig hubo que filtrarlo a mano), el esquema se puede inferir en vez de declarar las 17 columnas, y el código es Python, que ya usamos en el resto de la carrera.

## Justificación (qué dejaríamos en producción 2026)
Elegimos **Spark**. La versión de Pig que instalamos (0.17.0) es de 2017 y para correrla en Hadoop 3 hay que confiar en que sea compatible: funcionó, pero no es un terreno con soporte activo. Spark cubre lo mismo (y más) con una sola herramienta: batch y streaming, SQL, lectura de Parquet y conectores a lagos de datos como ADLS. Además hay mucha más comunidad y gente que lo sabe usar. Pig sigue apareciendo en bancos y telcos porque hay scripts viejos que nadie quiere reescribir, no porque se elija para proyectos nuevos.

## Trade-off aceptado
Spark es más pesado de operar que un script de Pig (necesita configurar un cluster o un servicio gestionado y manejar memoria), y para una limpieza tan simple como esta puede ser más de lo necesario. Lo aceptamos porque el pipeline va a crecer (más fuentes, streaming de logs en Flume/Kafka) y conviene tener una sola herramienta. Si ya existieran trabajos en Pig funcionando en producción, mantenerlos sería razonable hasta que haya un motivo concreto para migrarlos.