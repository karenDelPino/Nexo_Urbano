// 1. Limpiar la base por si había datos previos
MATCH (n) DETACH DELETE n;

// 2. Cargar entidades físicas (Estaciones) reales desde el CSV de NexoUrbano
LOAD CSV WITH HEADERS FROM 'file:///estaciones.csv' AS row
MERGE (e:Estacion {station_id: row.station_id})
SET e.nombre = row.nombre, e.barrio = row.barrio;

// 3. Cargar usuarios (Riders) reales desde el CSV
LOAD CSV WITH HEADERS FROM 'file:///riders.csv' AS row
MERGE (r:Rider {rider_id: row.rider_id})
SET r.nombre = row.nombre, r.plan = row.plan;

// 4. Generar el historial real de extracciones (SALE_DE)
// Limitamos la muestra a 15.000 viajes para no saturar la RAM de Docker local
LOAD CSV WITH HEADERS FROM 'file:///viajes.csv' AS row
WITH row LIMIT 15000
MATCH (r:Rider {rider_id: row.rider_id})
MATCH (e:Estacion {station_id: row.station_start})
MERGE (r)-[:SALE_DE {ts_start: row.ts_start}]->(e);

// 5. Poblar el Grafo Social desde el JSON oficial exportado por el generador
CALL apoc.load.json('file:///grafo.json') YIELD value
UNWIND value.amigos AS amigo
MATCH (r1:Rider {rider_id: amigo.a}), (r2:Rider {rider_id: amigo.b})
MERGE (r1)-[:AMIGO_DE]-(r2);

// 6. Poblar la Afinidad Comercial (Gustos / Intereses)
CALL apoc.load.json('file:///grafo.json') YIELD value
UNWIND value.le_gusta AS gusto
MATCH (r:Rider {rider_id: gusto.rider_id})
MERGE (i:Interes {interes: gusto.interes})
MERGE (r)-[:LE_GUSTA]->(i);