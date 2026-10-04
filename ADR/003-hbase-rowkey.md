# ADR 003 - HBase: Diseño del Rowkey de la tabla `vehiculos`

## Decisión
Se usa como rowkey el identificador propio del vehículo, con formato `vehNNN` (ej. `veh001`, `veh002`, ..., `veh050`), en vez de anteponer una fecha o timestamp.

## Justificación (evitar hotspot)
En HBase, las filas se distribuyen entre regiones según el orden alfabético del rowkey. Si hubiéramos usado un timestamp al inicio de la clave (ej. `20261002-veh001`), todas las escrituras nuevas de telemetría caerían siempre en la región con las claves "más recientes", generando un **hotspot**: un único servidor sobrecargado mientras el resto queda ocioso, porque todo el tráfico de escritura se concentra en el mismo punto temporal. Usando el ID de vehículo como prefijo (`veh001`...`veh050`), las escrituras de telemetría de los 50 vehículos se reparten de forma pareja entre las distintas regiones de la tabla, ya que no existe un patrón creciente compartido entre ellas: cada vehículo escribe en su propia región de forma independiente del momento en que ocurre la escritura.

## Trade-off aceptado
Con este diseño, las consultas por rango de fechas (ej. "traer toda la telemetría de las últimas 2 horas, de todos los vehículos") son menos eficientes, porque los datos recientes están dispersos entre
muchas regiones en vez de estar agrupados. Para este caso de uso (consultar el estado puntual de un vehículo por su ID) esto no es un problema, ya que las consultas típicas son por `rowkey` exacto (`get`)
o filtradas por estado (`scan` + filtro), no por rango temporal.