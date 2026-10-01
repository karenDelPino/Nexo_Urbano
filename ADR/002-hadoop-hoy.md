# ADR 002 - Por qué Hadoop sigue en el diseño en 2026

## Contexto
Con la adopción de Azure Data Lake y Databricks (Lakehouse), surge la duda de si mantener nuestro clúster on-premise de Hadoop. Debemos justificar su continuidad operativa y definir sus límites de uso.

## Decisión
Se decide **mantener el clúster Hadoop (HDFS + Hive + HBase)** exclusivamente como capa de almacenamiento de bajo costo, cumplimiento normativo y servicio NoSQL, pero **se deprecia el uso de MapReduce** para nuevos desarrollos.

## Justificación
1. **Almacenamiento barato y cumplimiento:** HDFS nos permite mantener un histórico profundo de logs y datos on-premise a una fracción del costo de la nube, cumpliendo con normativas locales de retención de datos.
2. **Hive como sistema legado:** Tenemos procesos de negocio y reportes históricos que ya corren sobre Apache Hive. Migrar todo a Spark SQL tomaría meses de refactorización que hoy no aportan valor directo.
3. **HBase activo:** Seguimos necesitando HBase para lecturas/escrituras de muy baja latencia, apoyado sobre HDFS.

## Lo que ya NO usaríamos (MapReduce)
En 2026, **no volveríamos a escribir un job analítico en MapReduce** (como el WordCount clásico en Java). La lentitud de escribir resultados intermedios en disco y la falta de optimizadores hacen que sea obsoleto frente a procesar todo en memoria usando Apache Spark.