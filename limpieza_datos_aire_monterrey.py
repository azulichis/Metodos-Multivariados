import pandas as pd
import numpy as np

# Rangos de operación
rangos_2025 = {
    'PM10': (0, 450),
    'PM2.5': (0, 350),
    'O3': (0, 180),
    'NO': (0, 400),
    'NO2': (0, 400),
    'NOX': (0, 400),
    'SO2': (0, 450),
    'CO': (0, 45),
    'RH': (0, 100),
    'WS': (0, 45),
    'TOUT': (0, 45),
    'SR': (0, 1500),
    'BP': (850, 1050),
    'WDR': (0, 360),
    'RAINF': (0, 35)
}

# Lista de hojas (estaciones meteorológicas)
hojas = ['SE', 'CE', 'SO', 'NE2', 'SE2', 'SE3', 'NE', 'NO', 'NO2', 'NTE', 'NTE2', 'SO2', 'SUR', 'NO3', 'NE3']
archivo_entrada = "DATOS HISTÓRICOS 2023_TODAS ESTACIONES.xlsx"
hojas_limpias = {}

for hoja in hojas:
    # Leer hoja individualmente
    df = pd.read_excel(archivo_entrada, sheet_name=hoja)

    # Eliminar fila de unidades (segunda fila)
    df = df.iloc[1:].reset_index(drop=True)

    # Reemplazar "NULL" por NaN
    df.replace("NULL", np.nan, inplace=True)

    # Convertir columna 'date' a fecha
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')

    # Eliminar columnas completamente vacías
    df = df.dropna(axis=1, how='all')

    # Separar columna de fecha
    columna_fecha = df['date'] if 'date' in df.columns else None
    df_datos = df.drop(columns=['date']) if 'date' in df.columns else df

    # Convertir columnas a numérico (Por si hay errores humanos)
    df_datos = df_datos.apply(pd.to_numeric, errors='coerce')

    # Eliminar columnas con menos del 10% de datos válidos (Se puede cambiar este parametro)
    columnas_validas = [col for col in df_datos.columns if df_datos[col].count() / len(df_datos) >= 0.10]
    df_datos = df_datos[columnas_validas]

    # Eliminar filas con más del 50% de datos faltantes
    mask_filas_validas = df_datos.notna().sum(axis=1) >= len(df_datos.columns) // 2
    df_datos = df_datos[mask_filas_validas]

    # Aplicar la misma máscara a la columna de fecha
    if columna_fecha is not None:
        columna_fecha = columna_fecha[mask_filas_validas]

    # Interpolación lineal
    df_datos = df_datos.interpolate(method='linear', limit_direction='both')

    # Rellenar con mediana si aún hay valores faltantes
    for col in df_datos.columns:
        if df_datos[col].isnull().any():
            mediana = df_datos[col].median()
            df_datos[col] = df_datos[col].fillna(mediana)

    # Detección y corrección de outliers según rangos
    for col in df_datos.columns:
        if col in rangos_2025:
            min_val, max_val = rangos_2025[col]
            mediana = df_datos[col].median()
            df_datos[col] = df_datos[col].apply(lambda x: mediana if x < min_val or x > max_val else x)

    # Reconstruir DataFrame final con fecha alineada
    if columna_fecha is not None:
        df_final = pd.concat([columna_fecha.reset_index(drop=True), df_datos.reset_index(drop=True)], axis=1)
    else:
        df_final = df_datos

    hojas_limpias[hoja] = df_final

# Guardar en nuevo archivo Excel
archivo_salida = "DATOS FINAL 2023_TODAS ESTACIONES.xlsx"
with pd.ExcelWriter(archivo_salida, engine='openpyxl') as writer:
    for hoja, df in hojas_limpias.items():
        df.to_excel(writer, sheet_name=hoja, index=False)

print("Limpieza completada, archivo guardado como:", archivo_salida)
