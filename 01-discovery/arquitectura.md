# Arquitectura Medallion y Las 'V' del Big Data
**Proyecto: NexoUrbano**

## 1. Asignación de Motores a las 'V' del Big Data
Nuestra arquitectura políglota no usa múltiples bases de datos por capricho, sino porque cada tecnología resuelve un desafío específico del Big Data en NexoUrbano:

*   **Velocidad (Apache Cassandra):** Elegido para la ingesta de telemetría IoT de los monopatines. Soporta miles de escrituras por segundo sin bloqueos (GPS, % de batería, temperatura).
*   **Variedad (MongoDB):** Ideal para el catálogo de productos (Pases, Suscripciones) y perfiles de Riders. Permite esquemas flexibles (documentos JSON anidados) que cambian constantemente sin romper la base.
*   **Veracidad (Neo4j):** Seleccionado para garantizar la legitimidad de las interacciones sociales y detectar fraudes. Al centrarse en las relaciones (grafos), permite descubrir colusiones y patrones ocultos que las bases tabulares no ven.
*   **Volumen (Hadoop HDFS / Hive):** El repositorio central inmutable. Almacena el histórico masivo de viajes, transacciones y logs de texto de soporte al cliente, escalando horizontalmente a bajo costo.
*   **Valor (Apache Spark / Python DecisionOps):** El motor de procesamiento distribuido que cruza todas las bases anteriores para responder las preguntas de negocio (Gold) y generar acciones operativas.

---

## 2. Diagrama Lógico: Arquitectura Medallion

El flujo de datos de NexoUrbano se estructura en tres capas progresivas de calidad y refinamiento:

### Capa BRONZE (Datos Crudos / Raw)
*Almacena la historia inmutable tal como llega desde las fuentes.*
*   **IoT & Eventos:** Lecturas de sensores de los monopatines (Cassandra).
*   **Operacional:** Registros de viajes finalizados y tickets de soporte de Zendesk (HDFS / Data Lake).
*   **Transaccional:** Compras de pases y alta de usuarios (MongoDB).

### Capa SILVER (Datos Conformados / Limpios)
*Datos estandarizados, filtrados y estructurados para analítica cruzada.*
*   **Spark ETL:** Limpieza de nulos en viajes, normalización de formatos de fecha y parseo de logs de soporte.
*   **Grafo Base:** Consolidación de nodos de Usuarios y Estaciones (Neo4j) sin algoritmos complejos aún.
*   **Tablas Hive:** Creación de la tabla particionada de viajes limpios.

### Capa GOLD (Datos de Negocio / Consumo)
*Agregaciones listas para la toma de decisiones (DecisionOps).*
*   **Recomendaciones:** Cruce de pipeline `$facet` (MongoDB) con gustos sociales (Neo4j) para upsell de pases.
*   **Prevención de Fraude:** Algoritmos de grafos (Neo4j) alertando colusiones en tiempo real.
*   **Logística y Operaciones:** Spark SQL calculando el Top de estaciones para reposición cruzado con alertas climáticas externas.
*   **Mantenimiento:** Alertas de retiro de flota basadas en la última lectura de batería (Cassandra).