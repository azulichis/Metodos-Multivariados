import pandas as pd
import numpy as np

# Parámetros
archivo_entrada = "Datos sucios finales 2024.xlsx"
archivo_salida = "Datos reducidos imputados 2024.xlsx"
hojas = ['SE', 'CE', 'SO2', 'SUR']

# Rangos físicos por variable
rangos = {
    'CO': (0, 18),
    'NO': (0, 500),
    'NO2': (0, 175),
    'NOX': (0, 500),
    'O3': (0, 180),
    'PM10': (0, 999),
    'PM2.5': (0, 999),
    'PRS': (687.5, 740),
    'SO2': (0, 250),
    'RAINF': (0, 70),
    'RH': (0, 100),
    'SR': (0, 1.26),
    'TOUT': (-4, 45.5),
    'WSR': (0, 40),
    'WDR': (0, 360)
}

def imputacion_preliminar(serie):
    serie = serie.astype(float)
    is_na = serie.isna()
    bloques = []
    inicio = None
    for i, val in enumerate(is_na):
        if val and inicio is None:
            inicio = i
        elif not val and inicio is not None:
            bloques.append((inicio, i - 1))
            inicio = None
    if inicio is not None:
        bloques.append((inicio, len(serie) - 1))
    for inicio, fin in bloques:
        if (fin - inicio + 1) <= 10:
            segmento = serie.iloc[inicio:fin+1]
            serie.iloc[inicio:fin+1] = segmento.interpolate(method='linear', limit_direction='both')
    return serie

def reducir_diariamente(df):
    df['day'] = df.index.date
    reducida = []
    for fecha, grupo in df.groupby('day'):
        fila = {'day': fecha}
        for col in df.columns:
            if col != 'day':
                datos_validos = grupo[col].dropna()
                if len(datos_validos) == 24:
                    fila[col] = datos_validos.mean()
                elif len(datos_validos) >= 4:
                    fila[col] = datos_validos.mean()
                else:
                    fila[col] = np.nan
        reducida.append(fila)
    return pd.DataFrame(reducida)

def imputacion_final(df):
    df = df.copy()
    for col in df.columns:
        if col != 'day':
            df[col] = df[col].interpolate(method='linear', limit_direction='both')
            if df[col].isna().any():
                df[col] = df[col].fillna(df[col].median())
    return df

# Procesamiento principal
hojas_reducidas = {}

for hoja in hojas:
    df = pd.read_excel(archivo_entrada, sheet_name=hoja)

    # Eliminar fila 2 (índice 1)
    df = df.drop(index=1).reset_index(drop=True)

    # Reemplazar "NULL" por NaN
    df.replace("NULL", np.nan, inplace=True)

    # Convertir fecha y establecer como índice
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.set_index('date')

    # Filtrar solo datos de 2024
    df = df[df.index.year == 2024]

    # Reconstruir línea de tiempo completa para 2024
    fechas_completas = pd.date_range(start="2024-01-01", end="2024-12-31 23:00:00", freq='h')
    df = df.reindex(fechas_completas)
    df.index.name = 'date'

    # Convertir columnas a numérico
    df = df.apply(pd.to_numeric, errors='coerce')

    # Eliminar datos erróneos fuera de rango
    for col in df.columns:
        if col in rangos:
            min_val, max_val = rangos[col]
            df[col] = df[col].apply(lambda x: np.nan if pd.notna(x) and (x < min_val or x > max_val) else x)

    # Imputación preliminar
    for col in df.columns:
        df[col] = imputacion_preliminar(df[col])

    # Reducción diaria
    df_reducida = reducir_diariamente(df)

    # Imputación final
    df_final = imputacion_final(df_reducida)

    hojas_reducidas[hoja] = df_final

# Guardar archivo final
with pd.ExcelWriter(archivo_salida, engine='openpyxl') as writer:
    for hoja, df in hojas_reducidas.items():
        df.to_excel(writer, sheet_name=hoja, index=False)

print("Reducción diaria e imputación completadas para 2024. Archivo guardado como:", archivo_salida)
