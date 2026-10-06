# ADR 006 - M7: Llevar logs de la app a HDFS con Flume

## Decisión
Se configuró un agente de **Flume 1.11.0** (`tools/flume-nexo-agent.conf`) que vigila un archivo de log con `tail -F` y escribe cada línea nueva en HDFS, en `/nexo/bronze/logs/`. Para la prueba se simularon **200 eventos** tomados de `data/app.log`, usando solo los tres tipos que pide la consigna (`ride_start`, `ride_end` y `payment_fail`), y se fueron agregando al archivo vigilado. Para un diseño nuevo en 2026 se elige **Kafka** para recibir los eventos y **Spark Structured Streaming** para llevarlos a HDFS. Flume queda como ejercicio de legado.

## Resultado de la corrida
Llegaron a HDFS **200 líneas, ni una más ni una menos**: 104 `ride_end`, 76 `ride_start` y 20 `payment_fail`. Flume las guardó en 11 archivos `app-log.*` (tandas de 20 líneas, que es el `batchSize` configurado). Evidencia: `evidencias/flume/m7-flume-evidencia.log`.

Hubo que resolver tres cosas para que funcionara:
- El contenedor no trae el comando `which`, y Flume lo usa para encontrar las librerías de Hadoop. Sin eso falló con `NoClassDefFoundError`. Se creó un `which` casero en `/tmp/bin`.
- Al reiniciar el agente, `tail -F` vuelve a leer las últimas líneas del archivo y habría duplicado eventos. Se vació el archivo vigilado antes de arrancar.
- El agente no sobrevive a un reinicio del contenedor: hay que volver a lanzarlo a mano.

Observación de calidad de datos: el log trae valores que no tienen sentido físico, como una batería de 126.6%. Además, `app.log` tiene otros dos tipos de evento (`unlock_ok` y `battery_low`) que no se usaron en esta prueba.

## Contraste: Flume vs. Structured Streaming vs. Kafka
- **Flume:** lleva logs a HDFS con solo un archivo de configuración, sin programar nada.
- **Flume:** nació para el mundo Hadoop, Apache lo marcó como inactivo (*dormant*) en octubre de 2024 y su última versión es de octubre de 2022; además su canal en memoria pierde eventos si el agente se cae.
- **Structured Streaming (Spark):** lee un flujo de datos, lo transforma y lo guarda con código, así que sirve para limpiar y calcular en el momento.
- **Structured Streaming:** necesita un cluster de Spark andando, más pesado de operar que Flume para solo copiar logs.
- **Kafka:** guarda los eventos en una cola durable que varios sistemas pueden leer a la vez y volver a leer más tarde.
- **Kafka:** es una pieza más de infraestructura para operar y por sí solo no escribe a HDFS (necesita un conector).
- **Diseño nuevo en 2026:** Kafka como puerta de entrada de los eventos y Spark Structured Streaming para llevarlos a la capa bronze.
- **Cuándo usar Flume:** solo si ya existe en la empresa y hay que mantenerlo, no para empezar un proyecto desde cero.

## Trade-off aceptado
Usamos Flume porque la consigna lo pide como ejemplo del legado que sigue en bancos y telcos, y porque ayuda a entender cómo se movían los logs antes de Kafka y Spark. El costo es que es una herramienta que Apache marcó como inactiva, que se rompe fácil en un entorno como el nuestro (hubo que arreglarle el `which` y las librerías) y que puede perder datos si el agente se cae. En un proyecto real eso no se aceptaría para datos importantes.