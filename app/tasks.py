import os
import requests

from io import BytesIO
from urllib.parse import urlparse
from celery.utils.log import get_task_logger
from minio import Minio
from minio.error import BucketAlreadyExists, BucketAlreadyOwnedByYou, NoSuchKey
from worker import app
from celery import chord
from datetime import datetime as d

logger = get_task_logger(__name__)

hex_num_lst = []

@app.task(bind=True, name='hit')
def hit(self, N, url):
    logger.info(f'......Inside hit method Hit Count: {N}, Api Url: {url}')
    for i in range(int(N)):
        getHexVal.s(i).delay()
    logger.info(f'......Exiting hit method Hit Count: {N}, Api Url: {url}')
    sum_val = xsum(hex_num_lst)
    save_sum('results', sum_val)    


@app.task(bind=True, name='getHexVal')
def getHexVal(self, i):
    logger.info(f'......Inside getHexVal Hit Counter: {i}......')
    response = requests.request('GET','https://5ju149k98f.execute-api.ap-south-1.amazonaws.com/pinacatestjob_beta').json()
    hex_num_lst.append(int(response['hex_num'], 16))
    logger.info(f'......Exiting getHexVal Hit Counter: {i}......')
 
@app.task(bind=True, name='xsum')
def xsum(self, numbers):
    logger.info(f'......Inside xsum......')
    return sum(numbers)



@app.task(bind=True, name='save_sum', queue='minio')
def save_sum(self, bucket, sum_val):
    logger.info(f'......Inside save_sum : {sum_val}......')
    minio_client = Minio(os.environ['MINIO_HOST'], 
        access_key=os.environ['MINIO_ACCESS_KEY'],
        secret_key=os.environ['MINIO_SECRET_KEY'],
        secure=False)

    try:
        minio_client.make_bucket(bucket, location="ind")
    except BucketAlreadyExists:
        pass
    except BucketAlreadyOwnedByYou:
        pass

    logger.info(f'Write {bucket}/{sum_val} to minio')
    stream = BytesIO(hex(sum_val).encode())
    minio_client.put_object(bucket, 'sum'+str(d.now().time()), stream, stream.getbuffer().nbytes)

  
    
    

