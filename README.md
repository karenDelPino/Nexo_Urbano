# NexoUrbano 2026 — pack del TP

Este directorio es el **pack de cátedra** del trabajo práctico evaluativo.

| Archivo | Quién lo usa |
|---|---|
| [TP_NexoUrbano_2026.md](TP_NexoUrbano_2026.md) | Estudiantes (enunciado completo) |
| [ANEXO_DOCENTE.md](ANEXO_DOCENTE.md) | Docentes (rúbrica viva, orales, plan B) |
| [tools/generar_datos.py](tools/generar_datos.py) | Squads, día 0 |
| [docker-compose.yml](docker-compose.yml) | Mongo, Cassandra, Neo4j, MySQL, MinIO |
| [contratos/bronze_viajes.yaml](contratos/bronze_viajes.yaml) | Plantilla de data contract |
| [.env.example](.env.example) | Secretos fuera de Git |

## Arranque en 10 minutos

```bash
python3 tools/generar_datos.py --seed 101 --n-viajes 250000
docker compose up -d mysql mongo minio
```

Luego cada squad copia la estructura de carpetas del enunciado a **su** repositorio (no entreguen este pack mezclado con el producto).

## Semilla

`--seed` = número de grupo × 100 + comisión. Dos grupos con la misma semilla tienen los mismos IDs: eso se considera copia.

## Hardware

- 8 GB RAM: levantar **un** NoSQL a la vez.
- 16 GB: MySQL + Mongo + Neo4j juntos; Cassandra aparte.
- Hadoop/Hive/HBase: sandbox Docker de la cátedra o el que documente el squad.

Azure y Databricks van por **cuenta free / Community**. Si piden tarjeta, MinIO + Spark local con ADR de paridad (está permitido en el enunciado).

## M2 - Azure Data Lake

**Suscripción:** Azure subscription 1
**Región:** Brazil South
**SKU:** Standard_LRS

**URI del dataset en Bronze:**

​```
abfss://bronze@nexourbanodata2026.dfs.core.windows.net/viajes/2026/09/16/viajes.csv
​```

**Evidencia del contenedor:**
![Listado del contenedor bronze](evidencias/azure/captura-azure.png)


## M8 - Spark batch (Bronze → Silver → Gold)

**Script:** `05-spark/batch/job_gold.py`  
**Cómo correrlo:**

    docker compose up -d spark
    docker compose exec spark /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 2g /opt/spark-apps/batch/job_gold.py

**Volumen:** 250.000 viajes (seed 402). [Escribir el motivo real de este volumen.]

**Calidad de dato (Bronze → Silver):** 244.944 de 250.000 filas pasan (98%). Se rechazaron 5.045 por duración fuera del rango 30 s – 4 h y 11 por km inválido. Las reglas de ids nulos y fin antes que inicio no rechazaron ninguna.

**Zona horaria:** `ts_start` viene en UTC y se convierte a hora de Buenos Aires antes de agrupar por fecha y hora. Por eso el primer día (28/02) tiene solo 312 viajes: los datos arrancan el 01/03 00:00 UTC, que son las 21:00 locales del día anterior.

**Silver:** Parquet particionado por `fecha`.  
**Gold:** `gold_ops_diaria` (viajes, minutos, km, ingresos) y `top_estaciones_origen`. Los ingresos son la suma de `fare_ars` del CSV.

**Observación sobre los datos:** el 71,8% de los viajes supera los 25 km/h y el 44,4% los 60 km/h, algo irreal para monopatines. No se filtró por velocidad porque eliminaría más de la mitad del dataset. [Hipótesis: el generador sortea km y duración por separado; sin verificar.]

**Explain:** ver `evidencias/spark/salida_job_gold.txt`. Se observa partition pruning (`PartitionFilters` sobre `fecha`) y column pruning (`ReadSchema` con una sola columna).

**Pendiente:** % de viajes con clima adverso y join con riders. 

**Join con riders:** `riders.csv` (120 riders) se une por `rider_id`. 0 viajes quedaron sin rider. En el plan físico Spark eligió `BroadcastHashJoin`, porque la tabla es chica.

**Clima adverso:** `clima_codigo >= 51` (61, 63 y 80; parecen códigos WMO de lluvia, es una inferencia). Resultado: 33,4% de los viajes. El código viene en el CSV; el join con Open-Meteo (M3) queda pendiente.

**Límite de los datos:** el generador sortea clima, km, duración y plan de forma independiente, por eso el % de clima adverso es casi igual cada día y los planes no se diferencian.
