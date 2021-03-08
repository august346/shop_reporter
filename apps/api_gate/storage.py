import os

from flask import request
from flask_restful import abort
from minio import Minio, S3Error

MINIO_URL = os.environ['MINIO_URL']
MINIO_ACCESS_KEY = os.environ['MINIO_ACCESS_KEY']
MINIO_SECRET_KEY = os.environ['MINIO_SECRET_KEY']

MINIO_BUCKET_CLIENT = os.environ['MINIO_BUCKET_CLIENT']
MINIO_BUCKET_RESPONSE = os.environ['MINIO_BUCKET_RESPONSE']

client = Minio(
    MINIO_URL,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

for name in (MINIO_BUCKET_CLIENT, MINIO_BUCKET_RESPONSE):
    if not client.bucket_exists(name):
        client.make_bucket(name)


def save(_id: str):
    file = request.files.get('file')
    size = os.fstat(file.fileno()).st_size

    client.put_object(
        bucket_name=MINIO_BUCKET_CLIENT,
        object_name=_id,
        data=file,
        length=size,
        content_type=file.content_type
    )


def get(_id: str):
    try:
        return client.get_object(MINIO_BUCKET_RESPONSE, _id)
    except S3Error:
        return abort(404)
