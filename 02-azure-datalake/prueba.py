import pandas as pd

# 1. Carga de datos
# Leemos la muestra sintética generada por el script de NexoUrbano
df = pd.read_csv('data/viajes_muestra.csv') 

print("================ PERFILADO DE DATOS ================\n")

# 2. Tipos de datos y valores nulos
print("--- 1. Tipos de datos y Nulos ---")
print(df.info())
print("\nConteo de Nulos por columna:")
print(df.isnull().sum())

# 3. Cardinalidad
print("\n--- 2. Cardinalidad ---")
print(df.nunique())

# 4. Outliers 
# El dataset sintético trae la columna 'duration_s' en segundos.
df['duracion_minutos'] = df['duration_s'] / 60

print("\n--- 3. Detección de Outliers (Duración en minutos) ---")
print(df['duracion_minutos'].describe())

viajes_muy_cortos = len(df[df['duracion_minutos'] < 1])
viajes_muy_largos = len(df[df['duracion_minutos'] > 240]) # Más de 4 horas
print(f"Viajes absurdamente cortos (< 1 min): {viajes_muy_cortos}")
print(f"Viajes absurdamente largos (> 4 horas): {viajes_muy_largos}")

# 5. Drift Temporal (Distribución de viajes a lo largo del tiempo)
print("\n--- 4. Drift Temporal (Distribución por día) ---")
# Usamos 'ts_start'
df['ts_start'] = pd.to_datetime(df['ts_start'])
df['fecha'] = df['ts_start'].dt.date
conteo_por_dia = df.groupby('fecha').size()

print(conteo_por_dia.head(15))