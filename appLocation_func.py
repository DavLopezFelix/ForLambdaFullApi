import boto3
from time import strftime
import pandas as pd
from io import StringIO
import logging
from build_response import *


logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Crear o cargar el archivo Excel desde S3
s3_client = boto3.client('s3')
bucket_name = 'exceltrigger'
file_key = 'UbicacionesPorAppSNP.csv'


def getAppLocation():
    try:
        csv_file = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        df = pd.read_csv(StringIO(csv_file['Body'].read().decode('utf-8')))         

        #Getting  the last one of each column
        column_headers = df.columns.tolist()
        if len(column_headers) > 1:
            body = {}
            for column_header in column_headers:
                str_column_header = str(column_header)
                last_index = df[str_column_header].last_valid_index()

                # Obtener el último valor ingresado y agregar como parte del body
                body[str_column_header] = df[str_column_header].loc[last_index]

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
        csv_file = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        existing_df = pd.read_csv(StringIO(csv_file['Body'].read().decode('utf-8')))
    except:
        logger.exception("Error al leer las Ubicacion de Applicaciones")
    
# Concatenar los DataFrames
    final_df = pd.concat([existing_df, df], ignore_index=True)

# Guardar el DataFrame actualizado en S3
    csv_buffer = StringIO()
    final_df.to_csv(csv_buffer, index=False)
    s3_client.put_object(Body=csv_buffer.getvalue().encode('utf-8'), Bucket=bucket_name, Key=file_key)

    body = {
        'Operation': 'Save',
        'Message': 'Success',
        'Item': requestBody
    }
    
    return buildResponse(200, body)