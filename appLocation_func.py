import boto3
from time import strftime
import pandas as pd
from io import BytesIO
import logging
from build_response import *


logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Crear o cargar el archivo Excel desde S3
s3_client = boto3.client('s3')
bucket_name = 'exceltrigger'
file_key = 'UbicacionesPorAppSNP.xlsx'


def getAppLocation():
    try:
        excel_file = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        df = pd.read_excel(BytesIO(excel_file['Body'].read()))         

        # Obtener el último valor ingresado
        ultimo_valor = df.iloc[-1].to_dict()
        print("ultimo_valor: ", ultimo_valor)
        if ultimo_valor:
            body = {
            'FechaDeRegistro': str(ultimo_valor['Date']),
            'App1': ultimo_valor['App1'],
            'App2': ultimo_valor['App2'],
            'App3': ultimo_valor['App3']
            }

            return buildResponse(200, body)
        else:
            return buildResponse(404, {'Message': 'No hay ultimo registro'})
    except:
        logger.exception("Error al leer las Tempordas")


def saveAppLocation(requestBody):
    current_date = strftime("%Y-%m-%d %H:%M:%S")
    requestBody.update({'Date': str(current_date)})

    df = pd.DataFrame(requestBody, index=[0])

    try:
        excel_file = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        existing_df = pd.read_excel(BytesIO(excel_file['Body'].read()))
    except Exception:
        existing_df = pd.DataFrame()
    
# Concatenar los DataFrames
    final_df = pd.concat([existing_df, df], ignore_index=True)

# Guardar el DataFrame actualizado en S3
    with BytesIO() as buffer:
        final_df.to_excel(buffer, index=False)
        s3_client.put_object(Body=buffer.getvalue(), Bucket=bucket_name, Key=file_key)

    body = {
        'Operation': 'Save',
        'Message': 'Success',
        'Item': requestBody
    }
    
    return buildResponse(200, body)