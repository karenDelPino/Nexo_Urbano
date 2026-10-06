# ADR 005 - M7: Importar el CRM (`crm.riders`) a HDFS con Sqoop

## Decisión
Se importó la tabla `crm.riders` de MySQL (contenedor `nexo-mysql`) a HDFS en `/nexo/bronze/riders_sqoop/` usando **Sqoop 1.4.7**. La importación trajo **120 registros**, igual a los que hay en MySQL. Como alternativa documentada (plan B) queda el equivalente con `spark.read.jdbc`, que no se ejecutó en este entorno. Para un diseño nuevo en 2026 se elige Spark JDBC, no Sqoop.

Comando usado (la contraseña se pide por teclado con `-P`, no queda escrita):

```
sqoop import --connect "jdbc:mysql://172.18.0.4:3306/crm?useSSL=false&allowPublicKeyRetrieval=true" --username root -P --table riders --target-dir /nexo/bronze/riders_sqoop --delete-target-dir --bindir $SQOOP_HOME/lib -m 1
```

Evidencia: `evidencias/sqoop/m7-sqoop-evidencia.log` (listado en HDFS, primeras filas y total de 120 filas).

## Qué hubo que resolver para que funcionara
Sqoop 1.4.7 es de la época de Hadoop 2 y nosotras usamos Hadoop 3.3.6, así que no anduvo de entrada:
- Faltaba la librería `commons-lang` 2.6 (error `NoClassDefFoundError`): se agregó a la carpeta `lib` de Sqoop.
- El contenedor trae solo un JRE y Sqoop necesita compilar una clase Java: se descargó un JDK 8 en `/opt` y se usó solo para Sqoop (`JAVA_HOME` en esa sesión), sin tocar el Java de Hadoop.
- Al lanzar el trabajo, Hadoop no encontraba la clase `riders` recién generada (`ClassNotFoundException`): se resolvió con `--bindir $SQOOP_HOME/lib` y repitiendo la importación.
- Hubo que conectar `nexo-hadoop` a la red de Docker de MySQL (`nexo_urbano_default`), porque estaban en redes distintas.

También se observó que Sqoop dejó las columnas en orden alfabético (`alta, ciudad, email, nombre, plan, rider_id`) y no en el orden de la tabla original. Hay que tenerlo en cuenta si se arma una tabla de Hive encima.

## Plan B (no ejecutado)
Equivalente con Spark, que habría dejado los datos en la misma carpeta:

```python
riders = (spark.read.format("jdbc")
          .option("url", "jdbc:mysql://172.18.0.4:3306/crm")
          .option("dbtable", "riders")
          .option("user", "root")
          .option("password", "<contraseña>")
          .load())
riders.write.mode("overwrite").csv("/nexo/bronze/riders_sqoop", header=True)
```

## Justificación (por qué Sqoop sigue apareciendo en RFPs viejos y qué usar hoy)
Sqoop nació en la era de Hadoop 1 y 2 como la herramienta estándar para pasar tablas de bases relacionales a HDFS, y por eso figura en pliegos y diseños de bancos y telcos que se escribieron entonces y hoy siguen vigentes. Apache lo pasó a su archivo de proyectos retirados en 2021, y el esfuerzo que nos tomó instalarlo en Hadoop 3 lo refleja. En un diseño nuevo en 2026 usaríamos **Spark JDBC** para cargas por lotes, o una herramienta de captura de cambios (como Debezium con Kafka) si se necesita copiar la base casi en tiempo real.

## Trade-off aceptado
Usar Sqoop nos costó más trabajo del que merecía una tarea tan simple como copiar 120 filas: hubo que agregar librerías, instalar un JDK y arreglar varios errores de compatibilidad. Además, Sqoop ya no tiene mantenimiento, así que en un proyecto real no lo elegiríamos para algo nuevo. Lo aceptamos porque la consigna pide usarlo como ejemplo del legado que sigue en bancos y telcos, y porque pasar por esos problemas ayuda a entender por qué esos sistemas son difíciles de mantener.