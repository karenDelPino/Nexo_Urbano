# Bitácora de IA — NexoUrbano

Squad:

Herramientas usadas (versiones):
-Claude Sonnet 5.5, de Anthropic
-Gemini 1.5 PRO

Regla: una fila por sesión relevante. Mentir acá desaprueba el bloque A.

| Fecha | Módulo | Prompt (resumen) | Qué se aceptó | Qué se corrigió a mano | Qué falló |

| 16–19/09/2026 | M2 | Guía para crear Azure ADLS Gen2, subir el CSV y poner la URI en el README | Cuenta nexourbanodata2026, contenedores bronze/silver/gold, URI en el README | Recreé el contenedor mal escrito ("broze") | La IA confundió el nombre del deployment con el de la cuenta. El push falló por falta de permiso en el repo|

| 30/09/2026 | M2 | ADR 001 con 4 criterios | Estructura y cifras de la calculadora de Azure (US$33 por 1 TB/mes y US$162,90 de salida) | Reemplacé cifras aproximadas de una búsqueda web | ⚠ La columna on-premise quedó cualitativa |

| 30/09/2026 | M3 (Databricks) | Discrepancia entre CSV (Bs As) y código hardcodeado (Mendoza). Configuración de widgets. | Explicación del límite de API (3 estaciones) para Open-Meteo. Corrección de ciudad y timezone. | Restauración manual del bloque `try` inicial en Databricks que se había eliminado. | La celda falló con `SyntaxError` por pegar un bloque `except` sin su `try`. |

| 01/10/2026 | M1, M2 y M4 | Revisión de `4V.md` y creación de documentos de arquitectura (ADR 001 y 002). | Argumentos de costos operativos y justificación técnica para mantener HDFS pero descartar MapReduce. | Se reescribieron el acta de las 4V y el ADR 002 usando un lenguaje más coloquial y explicativo. | Los primeros borradores generados utilizaban un lenguaje demasiado técnico y estructurado. |

| 01/10/2026 | M5 | Hadoop con Docker, comandos HDFS y WordCount | apache/hadoop:3, HDFS y WordCount con top 20 | Reescribí el XML con basura y creé la carpeta backup | start-dfs.sh falló por falta de SSH. ⚠ Bloque de 128 MB sin verificar; resumen del job escrito a mano |
| 01/10/2026 | M6 HBase | HBase con tabla de 50 vehículos y ADR del rowkey | HBase 2.5.9, get y scan, ADR 003 | Salí del shell por escribir comandos adentro | La URL de descarga de la IA ya no existía. ⚠ Los 50 vehículos son datos sintéticos |

| 02/10/2026 | M6 Hive | Hive con tabla externa e interna, 3 consultas y DROP | Hive 3.1.3, tabla particionada, el DROP borró datos solo en la interna | Agregué skip.header.line.count y cambié HOUR() por SUBSTR | El INSERT falló por falta de memoria (la IA sospechó de versiones) |

| 04–05/10/2026 | M7 Pig | Script Pig que limpie viajes y calcule distancia nula, más ADR contra Spark | Pig 0.17.0, 1960 de 2000 viajes, ADR 004 | Redefiní distancia_nula (98 casos) y re-corrí | Con km = 0 daba 0 casos. Definición propia; el código Spark no se ejecutó |

| 05/10/2026 | M7 Sqoop | Importar crm.riders de MySQL a HDFS, con ADR | 120 registros en HDFS, ADR 005 con plan B | Conecté la red de Docker y descargué driver, librería y JDK a mano | Sqoop (de la era Hadoop 2) falló varias veces: IP equivocada, URL del driver mala, JDK faltante y ClassNotFoundException. Plan B no ejecutado |

| 05–06/10/2026 | M7 Flume | Agente que lleve un log a HDFS con 200 eventos, más ADR | 200 eventos en HDFS, ADR 006 | Vacié el archivo vigilado antes de reiniciar y creé un which casero | Falló por falta de librerías de Hadoop; el primer arreglo no funcionó. Los 200 eventos son una selección de app.log |

| 05/10/2026 | M4 y M5 | Comandos HDFS (`hdfs dfs -ls /`) no reconocen el contenedor. Dudas sobre NameNode/DataNode. | Modificación del compose para agregar `apache/hadoop:3`. Explicación de componentes HDFS. | Ajuste manual de las rutas origen/destino en los comandos `docker cp` (carpeta `data/`). | El test inicial falló porque el entorno Docker provisto por la cátedra comentaba Hadoop. |

| 07–09/10/2026 | M8 (Spark batch) | Agregar Spark al compose y armar un job que pase los viajes de Bronze a Silver y Gold (calidad, join con riders, clima adverso, explain) | Imagen apache/spark:3.5.3, job_gold.py con 4 reglas de calidad, Silver en Parquet por fecha y 3 tablas Gold. Clima adverso = clima_codigo >= 51; no filtrar por velocidad; usar riders.csv | Corregí la indentación del YAML, di permisos al volumen con chown, verifiqué que gold_por_plan suma 244.944 (igual a Silver) y restauré archivos con git restore.  | La IA propuso bitnami/spark:3.5, una imagen sin versiones publicadas (se verificó). El job falló al escribir en el volumen nuevo hasta dar permisos (causa inferida). El 71,8% de los viajes supera 25 km/h (hipótesis sin verificar: el generador sortea km y duración por separado). Falta el join con Open-Meteo (M3) |

| 07/10/2026 | M9 (Streaming)| Script PySpark para alertas. Errores al correr socket en terminal. | Script de PySpark adaptado a Opción B (monitoreo de directorio con archivos JSON). | Ejecución del script utilizando el ejecutable nativo de `python` dentro del entorno virtual `.venv`. | Los comandos `nc -lk 9999` y `spark-submit` fallaron por incompatibilidad con Windows. |
2026-10-09 | M9 - Spark | Error `No such file or directory` al hacer `spark-submit` del script. | Identificó el mapeo de volúmenes en `docker-compose.yml` (`./data` -> `/opt/spark-data`). | Se ajustaron las rutas internas en el script `job_alerts.py` y en el comando de ejecución. |
| 2026-10-09 | M9 - Spark | Error `java.io.IOException: mkdir failed` al escribir los resultados. | Detectó que el volumen `/opt/spark-data` era *Read-Only* (ro). Sugirió enrutar la salida hacia `/opt/spark-lake`. | Los *sinks* se configuraron en modo *append* hacia `/opt/spark-lake/gold_alerts`, generando los `.parquet`. |
| 2026-10-09 | M9 - Spark | Duda sobre duplicidad en consola y eventos que no se registraban. | Explicó mediante un diagrama visual el comportamiento del *sliding window* y el filtro del *watermark*. | Se validó el procesamiento en vivo, se grabó el HMV y se redactó el caso de uso técnico final. |
| 2026-10-09 | M10 - Mongo | `ServerSelectionTimeoutError` bloqueando conexiones de Python y Mongo Compass desde Windows. | Explicó que Docker estaba restringiendo el puerto 27018 por seguridad. Sugirió usar el motor nativo `mongosh` por dentro. | Se descartó `pymongo` y se ejecutó el script `crud.js` directamente en la terminal interactiva del contenedor. |
| 2026-10-09 | M10 - Mongo | Duda sobre qué métricas del `explain()` servían de evidencia y qué eran los `ObjectId`. | Explicó que `ObjectId` es el ID interno automático y que la validación era la caída de "Docs examinados" (de 300 a 65). | Se completó el CRUD, se demostró la optimización del índice compuesto y se redactó la comparativa SQL. |