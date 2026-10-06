-- M7 - Limpieza de viajes con Pig Latin
-- distancia_nula = 1 si misma estacion de salida y llegada, o km = 0, o km vacio
viajes_raw = LOAD '/nexo/bronze/viajes/viajes_muestra.csv' USING PigStorage(',') AS (
    ride_id:chararray, ts_start:chararray, ts_end:chararray, duration_s:int,
    rider_id:chararray, vehicle_id:chararray, station_start:chararray, station_end:chararray,
    barrio_start:chararray, barrio_end:chararray,
    lat_start:double, lon_start:double, lat_end:double, lon_end:double,
    km:double, fare_ars:double, clima_codigo:int);

sin_header = FILTER viajes_raw BY ride_id != 'ride_id';

validos = FILTER sin_header BY duration_s >= 30 AND duration_s <= 14400;

clean_viajes = FOREACH validos GENERATE
    ride_id, ts_start, ts_end, duration_s, rider_id, vehicle_id,
    station_start, station_end, barrio_start, barrio_end,
    lat_start, lon_start, lat_end, lon_end, km, fare_ars, clima_codigo,
    ((station_start == station_end OR km IS NULL OR km == 0.0) ? 1 : 0) AS distancia_nula;

STORE clean_viajes INTO '/nexo/silver/clean_viajes' USING PigStorage(',');
