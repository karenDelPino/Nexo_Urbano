-- 1) Ultimas 20 lecturas de un vehiculo (el orden viene del clustering DESC)
SELECT * FROM nexo.lecturas_vehiculo
WHERE vehicle_id = 'V0073' LIMIT 20;

-- 2) Rango horario de un vehiculo
SELECT * FROM nexo.lecturas_vehiculo
WHERE vehicle_id = 'V0073'
  AND ts >= '2026-05-01 00:00:00+0000'
  AND ts <  '2026-05-01 12:00:00+0000';

-- 3) Consulta que NO respeta la clave: no filtra por vehicle_id.
--    Cassandra la rechaza porque tendria que recorrer todas las particiones.
SELECT * FROM nexo.lecturas_vehiculo WHERE status = 'idle';
