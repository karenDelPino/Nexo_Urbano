# Notas del explain (M8)

Fuente: `evidencias/spark/salida_job_gold.txt`

El `explain` es el plan que arma Spark antes de ejecutar una consulta. Muestra cómo decidió hacer el trabajo. En mi salida aparecen tres cosas que le ahorran trabajo a Spark.

## 1. Partition pruning (PartitionFilters)
- **Qué se ve:** en la línea `FileScan parquet` aparece `PartitionFilters: [isnotnull(fecha), (fecha >= 2026-05-01)]`.
- **Qué significa:** guardé Silver en una carpeta por cada día. Como la consulta pide solo desde el 1 de mayo, Spark abre solo esas carpetas y se saltea las anteriores.
- **Qué ahorra:** los datos van del 1/3 al 30/5, así que mayo es más o menos un tercio del total. Spark lee solo ese tercio.

## 2. Column pruning (ReadSchema)
- **Qué se ve:** `ReadSchema: struct<station_start:string>`.
- **Qué significa:** la tabla tiene 22 columnas, pero esta consulta solo necesita `station_start`. Parquet guarda los datos por columna, así que Spark lee solo esa.
- **Qué ahorra:** no lee las otras 21 columnas.

## 3. Broadcast join (BroadcastHashJoin)
- **Qué se ve:** `BroadcastHashJoin ... LeftOuter, BuildRight`.
- **Qué significa:** la tabla de riders tiene solo 120 filas. Spark la copia entera a cada ejecutor, así no tiene que mover los 245 mil viajes para unir las dos tablas.
- **Por qué LeftOuter:** se mantienen todos los viajes aunque un rider faltara. Comprobé que no falta ninguno (0 viajes sin rider).

## Lo que cacheamos
`silver` se guarda en memoria con `.cache()`, porque se usa en varias cuentas (rechazos, velocidades, Gold). Así Spark no la recalcula cada vez.