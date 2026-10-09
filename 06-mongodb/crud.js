// 1. Conectar a la base de datos (se crea automáticamente)
use('nexo_db');

// Limpiar colección para pruebas
db.productos.drop();

// 2. Generar 300 documentos (InsertMany)
let categorias = ["pase", "recarga", "casco", "seguro"];
let documentos = [];

for (let i = 1; i <= 300; i++) {
    let randomCat = categorias[Math.floor(Math.random() * categorias.length)];
    documentos.push({
        sku: "PRD-" + i.toString().padStart(3, '0'),
        nombre: "Producto Comercial " + i,
        categoria: randomCat,
        detalles: {
            stock: Math.floor(Math.random() * 100),
            tags: ["movilidad", "premium"],
            precios_ciudad: {
                Mendoza: 2500,
                CABA: 3000
            }
        },
        activo: true
    });
}
db.productos.insertMany(documentos);
print("✅ 1. Se insertaron 300 documentos.");

// BÚSQUEDA ANTES DEL ÍNDICE
print("\n--- Búsqueda ANTES del índice ---");
let statsAntes = db.productos.find({ categoria: "casco", "detalles.tags": "premium" }).explain("executionStats");
print("Milisegundos: " + statsAntes.executionStats.executionTimeMillis + " ms");
print("Docs examinados: " + statsAntes.executionStats.totalDocsExamined);

// CREAR ÍNDICE COMPUESTO
db.productos.createIndex({ categoria: 1, "detalles.tags": 1 });
print("\n✅ 2. Índice creado en categoria y tags.");

// BÚSQUEDA DESPUÉS DEL ÍNDICE
print("\n--- Búsqueda DESPUÉS del índice ---");
let statsDespues = db.productos.find({ categoria: "casco", "detalles.tags": "premium" }).explain("executionStats");
print("Milisegundos: " + statsDespues.executionStats.executionTimeMillis + " ms");
print("Docs examinados: " + statsDespues.executionStats.totalDocsExamined);

// UPDATE ONE
db.productos.updateOne(
    { sku: "PRD-001" },
    { $set: { "detalles.precios_ciudad.Mendoza": 9999 } }
);
print("\n✅ 3. UPDATE ONE: Precio actualizado correctamente.");

// DELETE ONE
db.productos.deleteOne({ sku: "PRD-300" });
print("✅ 4. DELETE ONE: Producto borrado.");