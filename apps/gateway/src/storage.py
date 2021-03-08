import os
from http import HTTPStatus
from typing import Dict
from uuid import uuid4

from flask import request
from flask_restful import abort
from minio import Minio, S3Error

MINIO_URL = os.environ['MINIO_URL']
MINIO_ACCESS_KEY = os.environ['MINIO_ACCESS_KEY']
MINIO_SECRET_KEY = os.environ['MINIO_SECRET_KEY']

MINIO_BUCKET_CLIENT = os.environ['MINIO_BUCKET_CLIENT']
MINIO_BUCKET_RESPONSE = os.environ['MINIO_BUCKET_RESPONSE']

MAX_FILE_SIZE = 200_000

client = Minio(
    MINIO_URL,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

for bucket_name in (MINIO_BUCKET_CLIENT, MINIO_BUCKET_RESPONSE):
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)


def save_files() -> Dict[str, str]:
    files = {}

    for key, file in request.files.items():
        file_id = str(uuid4())

        size = os.fstat(file.fileno()).st_size

        if size > MAX_FILE_SIZE or size == 0:
            abort(HTTPStatus.REQUEST_ENTITY_TOO_LARGE)

        client.put_object(
            bucket_name=MINIO_BUCKET_CLIENT,
            object_name=file_id,
            data=file,
            length=size,
            content_type=file.content_type
        )

        files[key] = file_id

    return files


def get(file_id: str):
    try:
        return client.get_object(MINIO_BUCKET_RESPONSE, file_id)
    except S3Error:
        return None
