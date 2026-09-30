# ADR 001 - Data Lake: On-premise vs. Nube (Azure)

## Contexto
NexoUrbano necesita una zona de aterrizaje (landing zone) para el dato crudo de viajes (capa Bronze) antes de procesarlo hacia Silver y Gold. Se evalúa si conviene una solución on-premise (servidor propio) o un Data Lake en
la nube (Azure Data Lake Storage Gen2).

## Decisión
Se elige **Azure Data Lake Storage Gen2** como zona de aterrizaje, por los motivos detallados abajo.

## Comparación por criterios

| Criterio | On-premise | Azure (nube) |
|---|---|---|
| **Costo de 1 TB/mes** | Costo de hardware amortizado + mantenimiento + energía + backup (estimado, no hay facturación directa mensual) | US$33,00/mes (Hot, LRS, ADLS Gen2, Brazil South) [1] |
| **Salida (egress)** | Sin costo adicional (red propia) | US$162,90 por TB (salida a Internet, Brazil South) [1] |
| **IAM** | Requiere administrar usuarios, permisos y auditoría manualmente (ej. Active Directory local) | Integrado con Microsoft Entra ID, RBAC granular por contenedor/carpeta, políticas de acceso condicional |
| **Tiempo hasta el primer byte** | Bajo (milisegundos) dentro de la red local, pero requiere aprovisionar hardware desde cero si no existe (semanas) | Minutos: la cuenta de almacenamiento se crea y está lista para usar al instante |

**Fuente [1]:** Calculadora de precios de Azure — https://azure.microsoft.com/pricing/calculator
(consultado el 30/09/2026, configuración: Brazil South, Hot, LRS, ADLS Gen2, pago por uso, 1000 GB)

## Consecuencias
- El costo de almacenamiento en sí es bajo, pero la **salida (egress)** es casi 5 veces más cara que guardar 1 TB un mes entero (US$162,90 vs US$33,00) — esto indica que conviene minimizar descargas y procesar los datos (Silver/Gold) dentro de la misma nube en vez de traerlos afuera.
- Ganamos IAM centralizado y gestión de acceso sin tener que mantener infraestructura propia de autenticación.
- El tiempo hasta el primer byte es prácticamente inmediato en la nube, contra semanas de aprovisionamiento si tuviéramos que montar un servidor on-premise desde cero.

## Qué cambiaría en producción (vs. este TP)
- Se usaría redundancia **GRS** (geo-redundante) en vez de LRS, para tolerar la caída de una región completa — esto incrementa el costo de almacenamiento pero reduce el riesgo de pérdida de datos.
- Se configurarían **políticas de ciclo de vida** (lifecycle management) para mover automáticamente datos viejos de Bronze a tier Cool/Archive y reducir costos.
- Se restringiría el acceso con **Private Endpoints** en vez de acceso público, por seguridad.