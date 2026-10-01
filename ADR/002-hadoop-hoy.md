# ADR 002 - Por qué seguimos usando Hadoop en 2026

## Contexto
Aunque ahora estamos modernizando todo usando la nube (Azure y Databricks), nos planteamos si vale la pena seguir manteniendo nuestros propios servidores locales con Hadoop. 

## Decisión
Decidimos que vamos a conservar el ecosistema Hadoop (HDFS, Hive y HBase) porque todavía nos sirve para tareas específicas y baratas. Sin embargo, dejamos de usar MapReduce para cualquier desarrollo nuevo.

## ¿Por qué lo mantenemos?
1. **Es muy barato para guardar historia:** Usar HDFS en nuestros servidores nos permite guardar muchísimos datos viejos y logs sin pagar los costos mensuales de la nube. Además, nos ayuda a cumplir con las leyes locales que nos obligan a retener datos dentro del país.
2. **Hive ya hace el trabajo:** Tenemos varios reportes y procesos históricos que ya están armados en Hive y funcionan bien. Pasar todo eso a Spark nos tomaría meses de trabajo y hoy no es prioridad.
3. **HBase es súper rápido:** Lo seguimos necesitando para poder leer y escribir los datos que mandan los sensores de los monopatines en tiempo real.

## Lo que ya NO vamos a usar: MapReduce
En pleno 2026, ya no tiene sentido hacer procesos analíticos nuevos con MapReduce. Escribir el código es pesadísimo, y como guarda resultados temporales en el disco a cada rato, es demasiado lento. Para cualquier cálculo nuevo, nos conviene mil veces usar Apache Spark, que es más inteligente y procesa todo rapidísimo en la memoria.