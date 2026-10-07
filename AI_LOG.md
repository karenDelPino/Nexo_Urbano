# Bitácora de IA — NexoUrbano

Squad:
Herramientas usadas (versiones):

Regla: una fila por sesión relevante. Mentir acá desaprueba el bloque A.

| Fecha | Módulo | Prompt (resumen) | Qué se aceptó | Qué se corrigió a mano | Qué falló |
| 30/09/2026 | M3 (Databricks) | Discrepancia entre CSV (Bs As) y código hardcodeado (Mendoza). Configuración de widgets. | Explicación del límite de API (3 estaciones) para Open-Meteo. Corrección de ciudad y timezone. | Restauración manual del bloque `try` inicial en Databricks que se había eliminado. | La celda falló con `SyntaxError` por pegar un bloque `except` sin su `try`. |
| 01/10/2026 | M1, M2 y M4 | Revisión de `4V.md` y creación de documentos de arquitectura (ADR 001 y 002). | Argumentos de costos operativos y justificación técnica para mantener HDFS pero descartar MapReduce. | Se reescribieron el acta de las 4V y el ADR 002 usando un lenguaje más coloquial y explicativo. | Los primeros borradores generados utilizaban un lenguaje demasiado técnico y estructurado. |
| 05/10/2026 | M4 y M5 | Comandos HDFS (`hdfs dfs -ls /`) no reconocen el contenedor. Dudas sobre NameNode/DataNode. | Modificación del compose para agregar `apache/hadoop:3`. Explicación de componentes HDFS. | Ajuste manual de las rutas origen/destino en los comandos `docker cp` (carpeta `data/`). | El test inicial falló porque el entorno Docker provisto por la cátedra comentaba Hadoop. |
| 07/10/2026 | M9 (Streaming)| Script PySpark para alertas. Errores al correr socket en terminal. | Script de PySpark adaptado a Opción B (monitoreo de directorio con archivos JSON). | Ejecución del script utilizando el ejecutable nativo de `python` dentro del entorno virtual `.venv`. | Los comandos `nc -lk 9999` y `spark-submit` fallaron por incompatibilidad con Windows. |