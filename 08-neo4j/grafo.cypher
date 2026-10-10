// 1. Limpiar la base por si había datos previos
MATCH (n) DETACH DELETE n;

// 2. Crear Estaciones locales e Intereses
CREATE (:Estacion {id: "E1", nombre: "Plaza Independencia"}),
       (:Estacion {id: "E2", nombre: "Parque San Martin"}),
       (:Estacion {id: "E3", nombre: "Plaza Godoy Cruz"});

CREATE (:Interes {id: "I1", nombre: "Movilidad Sustentable"}),
       (:Interes {id: "I2", nombre: "Tecnologia"}),
       (:Interes {id: "I3", nombre: "Data Science"});

// 3. Generar 85 Riders con un UNWIND
UNWIND range(1, 85) AS i
CREATE (:Rider {id: "R" + i, nombre: "Rider " + i});

// 4. Relaciones aleatorias masivas (Amigos, Viajes, Gustos y Estaciones)
MATCH (r1:Rider), (r2:Rider) WHERE r1.id <> r2.id AND rand() < 0.04 MERGE (r1)-[:AMIGO_DE]->(r2);
MATCH (r:Rider), (e:Estacion) WHERE rand() < 0.3 MERGE (r)-[:SALE_DE]->(e);
MATCH (r:Rider), (i:Interes) WHERE rand() < 0.4 MERGE (r)-[:LE_GUSTA]->(i);
MATCH (r1:Rider), (r2:Rider) WHERE r1.id <> r2.id AND rand() < 0.02 MERGE (r1)-[:VIAJO_CON]->(r2);

// 5. Inyectar el Patrón de Fraude (Obligatorio para que la query del bonus funcione)
MATCH (e:Estacion {id: "E1"})
MATCH (r1:Rider {id: "R10"}), (r2:Rider {id: "R11"}), (r3:Rider {id: "R12"})
CREATE (r1)-[:SALE_DE {hora: time('08:00:00')}]->(e)
CREATE (r2)-[:SALE_DE {hora: time('08:01:00')}]->(e)
CREATE (r3)-[:SALE_DE {hora: time('08:01:30')}]->(e);