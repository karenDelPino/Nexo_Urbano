use('nexo_db');

// --- 1. GENERADOR DE VENTAS SINTÉTICAS ---
db.ventas.drop();
let ventasMock = [];
let skusActivos = ["PRD-001", "PRD-002", "PRD-003", "PRD-010", "PRD-050", "PRD-100"];

for (let i = 1; i <= 200; i++) {
    // Generar fechas aleatorias del último trimestre (Julio a Octubre 2026)
    let mesRandom = 6 + Math.floor(Math.random() * 4);
    ventasMock.push({
        tx_id: "TXN-" + i,
        sku: skusActivos[Math.floor(Math.random() * skusActivos.length)],
        fecha: new Date(2026, mesRandom, Math.floor(Math.random() * 28) + 1),
        monto: Math.floor(Math.random() * 5000) + 1000,
        // EXTRA BIO: Coordenadas Geoespaciales listas para 2dsphere
        ubicacion_tienda: {
            type: "Point",
            coordinates: [-68.8272, -32.8895] 
        }
    });
}
db.ventas.insertMany(ventasMock);
print("✅ Generador: 200 ventas inyectadas con éxito.");

// --- 2. EL PIPELINE DE AGREGACIÓN ---
let pipeline = [
    // 1. $match: Último trimestre
    { $match: { fecha: { $gte: ISODate("2026-07-01T00:00:00Z") } } },
    
    // 5. $lookup: Traer información de la colección productos
    { $lookup: {
        from: "productos",
        localField: "sku",
        foreignField: "sku",
        as: "detalle_prod"
    }},
    { $unwind: "$detalle_prod" },
    
    // 2, 3 y 4. $facet: Ejecutar múltiples pipelines en paralelo
    { $facet: {
        "Total_por_Categoria": [
            { $group: { _id: "$detalle_prod.categoria", totalFacturado: { $sum: "$monto" } } }
        ],
        "Ventas_por_Mes": [
            { $group: { _id: { $month: "$fecha" }, totalFacturado: { $sum: "$monto" } } },
            { $sort: { "_id": 1 } }
        ],
        "Top_5_Productos": [
            { $group: { _id: "$sku", nombre: { $first: "$detalle_prod.nombre" }, ingresos: { $sum: "$monto" } } },
            { $sort: { ingresos: -1 } },
            { $limit: 5 }
        ]
    }},
    
    // 6. $project: Limpiar el documento final de salida
    { $project: {
        _id: 0,
        reporte: "Analítica Trimestral",
        Total_por_Categoria: 1,
        Ventas_por_Mes: 1,
        Top_5_Productos: 1
    }}
];

let reporteAnalitico = db.ventas.aggregate(pipeline).toArray();

// --- 3. EXPORTAR A PANTALLA ---
print("\n========== COPIAR EL SIGUIENTE JSON ==========\n");
print(EJSON.stringify(reporteAnalitico[0], null, 2));
print("\n==============================================\n");