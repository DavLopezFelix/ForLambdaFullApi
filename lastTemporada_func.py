import boto3
from time import strftime
import pandas as pd
from io import StringIO
import logging
from build_response import *
import os


logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Crear o cargar el archivo Excel desde S3
s3_client = boto3.client('s3')
bucket_name = 'exceltrigger'
file_key = 'TemporadasSNP.csv'

data = os.environ['data']


def getLastTemporada():
    try:
        csv_file = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        df = pd.read_csv(StringIO(csv_file['Body'].read().decode('utf-8')))

        # Obtener el último valor ingresado
        ultimo_valor = df.iloc[-1].to_dict()
        if ultimo_valor:
            body = {
            'FechaDeRegistro': str(ultimo_valor['date']),
            'Temporada': ultimo_valor['temporada'],
            'A': ultimo_valor['A'],
            'B': ultimo_valor['B']
            }
            print(data)
            return buildResponse(200, body)
        else:
            return buildResponse(404, {'Message': 'No hay ultimo registro'})
    except:
        logger.exception("Error al leer las Tempordas")


def saveTemporada(requestBody):
    current_date = strftime("%Y-%m-%d %H:%M:%S")
    requestBody.update({'date': str(current_date)})

    new_df = pd.DataFrame(requestBody, index=[0])

    try:
        csv_file = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        existing_df = pd.read_csv(StringIO(csv_file['Body'].read().decode('utf-8')))
    except :
        logger.exception("Error al leer las Tempordas")
    
    # Concatenar los DataFrames
    final_df = pd.concat([existing_df, new_df], ignore_index=True)

    # Guardar el DataFrame actualizado en S3
    csv_buffer = StringIO()
    final_df.to_csv(csv_buffer, index=False)
    s3_client.put_object(Body=csv_buffer.getvalue().encode('utf-8'), Bucket=bucket_name, Key=file_key)

    body = {
        'Operation': 'Save',
        'Message': 'Success',
        'Item': requestBody
    }
    
    return buildResponse(201, body)