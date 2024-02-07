from lastTemporada_func import *
from appLocation_func import *

getMethod = 'GET'
postMethod = 'POST'
healtPath = '/health'
lastTemporadaPath = '/lastTemporada'
appLocation =  '/appLocation'

def lambda_handler(event, context):

    httpMethod = event['httpMethod']
    path = event['path']

    if httpMethod == getMethod and path == healtPath:
        response = buildResponse(200, {'Message': 'API funcionando'})
    elif httpMethod == getMethod and path == lastTemporadaPath:
        response = getLastTemporada()
    elif httpMethod == postMethod and path == lastTemporadaPath:
        response = saveTemporada(json.loads(event['body']))
    elif httpMethod == getMethod and path == appLocation:
        response = getAppLocation()
    elif httpMethod == postMethod and path == appLocation:
        response = saveAppLocation(json.loads(event['body']))
    else:
        response = buildResponse(404, {'Message': 'No encontrado'})
    
    return response