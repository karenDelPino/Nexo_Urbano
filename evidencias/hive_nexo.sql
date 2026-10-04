-- hive_nexo.sql - Scripts de Hive para NexoUrbano (M6)

-- 1. Tabla externa apuntando al CSV en HDFS
CREATE EXTERNAL TABLE bronze_viajes (
  trip_id STRING,
  inicio STRING,
  fin STRING,
  duracion_seg INT,
  rider_id STRING,
  vehiculo_id STRING,
  estacion_origen STRING,
  estacion_destino STRING,
  barrio_origen STRING,
  barrio_destino STRING,
  lat_origen DOUBLE,
  lon_origen DOUBLE,
  lat_destino DOUBLE,
  lon_destino DOUBLE,
  distancia_km DOUBLE,
  costo DOUBLE,
  calificacion INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/nexo/bronze/viajes/';

ALTER TABLE bronze_viajes SET TBLPROPERTIES ('skip.header.line.count'='1');

-- 2. Tabla interna particionada por fecha
CREATE TABLE silver_viajes (
  trip_id STRING,
  rider_id STRING,
  vehiculo_id STRING,
  estacion_origen STRING,
  estacion_destino STRING,
  barrio_origen STRING,
  barrio_destino STRING,
  distancia_km DOUBLE,
  costo DOUBLE,
  duracion_seg INT
)
PARTITIONED BY (fecha STRING)
STORED AS TEXTFILE;

SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO TABLE silver_viajes PARTITION (fecha)
SELECT
  trip_id, rider_id, vehiculo_id,
  estacion_origen, estacion_destino,
  barrio_origen, barrio_destino,
  distancia_km, costo, duracion_seg,
  SUBSTR(inicio, 1, 10) AS fecha
FROM bronze_viajes;

-- 3a. Estaciones con mas destinos
SELECT estacion_destino, COUNT(*) AS cantidad_viajes
FROM silver_viajes
GROUP BY estacion_destino
ORDER BY cantidad_viajes DESC
LIMIT 10;

-- 3b. Viajes de mas de 45 minutos
SELECT trip_id, rider_id, duracion_seg, ROUND(duracion_seg/60.0, 1) AS duracion_min
FROM silver_viajes
WHERE duracion_seg > 2700
ORDER BY duracion_seg DESC;

-- 3c. Agrupado por hora x barrio, conteo y duracion media
SELECT
  SUBSTR(inicio, 12, 2) AS hora,
  barrio_origen,
  COUNT(*) AS cantidad_viajes,
  ROUND(AVG(duracion_seg)/60.0, 1) AS duracion_media_min
FROM bronze_viajes
GROUP BY SUBSTR(inicio, 12, 2), barrio_origen
ORDER BY hora, cantidad_viajes DESC;

-- 4. Experimento DROP: externa vs interna
DROP TABLE bronze_viajes;
-- Resultado: el archivo /nexo/bronze/viajes/viajes_muestra.csv SIGUE
-- existiendo en HDFS (tabla externa = solo se borra la metadata).

DROP TABLE silver_viajes;
-- Resultado: la carpeta /user/hive/warehouse/silver_viajes/ (con sus
-- 90 particiones) DESAPARECE (tabla interna = Hive gestiona y borra
-- tambien los datos).