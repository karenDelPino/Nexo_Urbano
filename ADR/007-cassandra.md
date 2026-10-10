# ADR 007 - Cassandra y consistencia (M12)

**Estado:** aceptada

## Contexto
Las lecturas de sensores (6.000 en el sandbox) llegan seguido y siempre se consultan por vehiculo.

## Decision
Tabla con vehicle_id como clave de particion y ts DESC como clustering. Keyspace con RF=1.

## Por que RF=1 aca y RF>=3 en produccion
El sandbox tiene un solo nodo, asi que no se puede tener mas de una copia. En produccion se usan 3 copias para que, si un nodo cae, los datos sigan disponibles.

## ONE vs QUORUM
- ONE: alcanza con que responda una copia. Es rapido y sigue funcionando aunque haya nodos caidos.
- QUORUM: hace falta que responda la mayoria (2 de 3). Es mas lento, pero asegura leer el dato actualizado.
- Bateria en vivo: ONE. Llega una lectura cada pocos segundos; si una esta un poco vieja, la siguiente la corrige. Importa la velocidad.
- Facturacion: QUORUM. Un error se traduce en plata, y no se puede cobrar con un dato viejo.

## Limite
Con RF=1, ONE y QUORUM se comportan igual: la diferencia es conceptual y no se puede demostrar en este sandbox.
