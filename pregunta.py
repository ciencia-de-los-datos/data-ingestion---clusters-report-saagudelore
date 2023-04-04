"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd
import numpy as np


def ingest_data():
    #
    # Inserte su código aquí
    #
    
    # Read the txt and rename columns
    col_names = ['cluster', 'cantidad_de_palabras_clave', 'porcentaje_de_palabras_clave', 'principales_palabras_clave']
    df = pd.read_fwf('./clusters_report.txt', widths=[8, 16, 16, 78], header=2, names=col_names)

    # Variables to be replaced
    cluster = 0
    cantidad_de_palabras_clave = 0
    porcentaje_de_palabras_clave = ''

    for index, row in df.iterrows():
        if np.isnan(row['cluster']):
            df.loc[index] = [cluster, cantidad_de_palabras_clave, porcentaje_de_palabras_clave, row['principales_palabras_clave']]
        else: 
            cluster = row['cluster']
            cantidad_de_palabras_clave = row['cantidad_de_palabras_clave']
            porcentaje_de_palabras_clave = row['porcentaje_de_palabras_clave']

    # Groupby and combine the 'principales_palabras_clave0 column
    df = df.groupby(['cluster', 'cantidad_de_palabras_clave', 'porcentaje_de_palabras_clave']).agg({'principales_palabras_clave': lambda x: ' '.join(x)})
    df['principales_palabras_clave'] = df['principales_palabras_clave'].str.replace(',,', ', ')
    df['principales_palabras_clave'] = df['principales_palabras_clave'].str.replace('  ', ' ').str.replace('  ', ' ').str.replace('  ', ' ')
    df['principales_palabras_clave'] = df['principales_palabras_clave'].str.replace('.', '', regex=False)
    df = df.reset_index()
    df['porcentaje_de_palabras_clave'] = df['porcentaje_de_palabras_clave'].str.replace(' %', '').str.replace(',', '.').astype(float)

    return df