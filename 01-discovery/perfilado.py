import pandas as pd

# 1. Carga de datos
# Reemplaza con el nombre real de tu archivo
df = pd.read_csv('../data/tu_archivo_de_viajes.csv') 

print("================ PERFILADO DE DATOS ================\n")

# 2. Tipos de datos y valores nulos
print("--- 1. Tipos de datos y Nulos ---")
print(df.info())
print("\nConteo de Nulos por columna:")
print(df.isnull().sum())

# 3. Cardinalidad (cuántos valores únicos hay por columna)
print("\n--- 2. Cardinalidad ---")
print(df.nunique())

# 4. Outliers (Para esto necesitamos calcular la duración del viaje)
df['started_at'] = pd.to_datetime(df['started_at'])
df['ended_at'] = pd.to_datetime(df['ended_at'])
df['duracion_minutos'] = (df['ended_at'] - df['started_at']).dt.total_seconds() / 60

print("\n--- 3. Detección de Outliers (Duración en minutos) ---")
print(df['duracion_minutos'].describe())

viajes_muy_cortos = len(df[df['duracion_minutos'] < 1])
viajes_muy_largos = len(df[df['duracion_minutos'] > 240]) # Más de 4 horas
print(f"Viajes absurdamente cortos (< 1 min): {viajes_muy_cortos}")
print(f"Viajes absurdamente largos (> 4 horas): {viajes_muy_largos}")

# 5. Drift Temporal (Ver si la distribución de viajes cambia en el tiempo)
print("\n--- 4. Drift Temporal (Distribución por día) ---")
df['fecha'] = df['started_at'].dt.date
conteo_por_dia = df.groupby('fecha').size()
print(conteo_por_dia.head(10)) # Muestra los primeros 10 días