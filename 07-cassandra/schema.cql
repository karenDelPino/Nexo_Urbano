-- Keyspace: RF=1 porque es un sandbox con un solo nodo.
-- En produccion se usaria RF >= 3 para tolerar la caida de un nodo.
CREATE KEYSPACE IF NOT EXISTS nexo
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Tabla pensada para consultar por vehiculo, de la lectura mas nueva a la mas vieja.
CREATE TABLE IF NOT EXISTS nexo.lecturas_vehiculo (
  vehicle_id  text,
  ts          timestamp,
  lat         double,
  lon         double,
  battery_pct double,
  temp_c      double,
  status      text,
  PRIMARY KEY ((vehicle_id), ts)
) WITH CLUSTERING ORDER BY (ts DESC);