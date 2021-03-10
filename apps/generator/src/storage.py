import io
import os

from flask import abort
from minio import Minio, S3Error

MINIO_URL = os.environ['MINIO_URL']
MINIO_ACCESS_KEY = os.environ['MINIO_ACCESS_KEY']
MINIO_SECRET_KEY = os.environ['MINIO_SECRET_KEY']

MINIO_BUCKET_CLIENT = os.environ['MINIO_BUCKET_CLIENT']
MINIO_BUCKET_DOC = os.environ['MINIO_BUCKET_DOC']

MAX_FILE_SIZE = 200_000

client = Minio(
    MINIO_URL,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

for bucket_name in (MINIO_BUCKET_CLIENT, MINIO_BUCKET_DOC):
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)


def save(name: str, file: io.BytesIO) -> None:
    size = file.getbuffer().nbytes

    client.put_object(
        bucket_name=MINIO_BUCKET_DOC,
        object_name=name,
        data=file,
        length=size,
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )


def get(file_id: str):
    try:
        return client.get_object(MINIO_BUCKET_CLIENT, file_id)
    except S3Error:
        return abort(404)
