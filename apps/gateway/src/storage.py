import hashlib
import os
from http import HTTPStatus
from typing import Dict, Optional, Tuple
from uuid import uuid4

from flask import request
from flask_restful import abort
from minio import Minio, S3Error
from werkzeug.datastructures import FileStorage

from . import redis

MINIO_URL = os.environ['MINIO_URL']
MINIO_ACCESS_KEY = os.environ['MINIO_ACCESS_KEY']
MINIO_SECRET_KEY = os.environ['MINIO_SECRET_KEY']

MINIO_BUCKET_CLIENT = os.environ['MINIO_BUCKET_CLIENT']
MINIO_BUCKET_DOC = os.environ['MINIO_BUCKET_DOC']

MAX_FILE_SIZE = 200_000
REDIS_FILES_CLIENT_PREFIX = 'files:client'

client = Minio(
    MINIO_URL,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

for bucket_name in (MINIO_BUCKET_CLIENT, MINIO_BUCKET_DOC):
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)


def save_all() -> Dict[str, str]:
    return {
        key: _save(file)
        for key, file in request.files.items()
    }


def _save(file: FileStorage) -> str:
    redis_key, file_id = _is_exist(file)

    if file_id:
        return file_id

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

    redis.client.set(redis_key, file_id)

    return file_id


def _is_exist(file: FileStorage) -> Tuple[str, Optional[str]]:
    key = f'{REDIS_FILES_CLIENT_PREFIX}:{hashlib.md5(file.read()).hexdigest()}'

    file.seek(0)

    value = redis.client.get(key)

    return key, value and value.decode()


def get(file_id: str, bucket: str = MINIO_BUCKET_DOC):
    try:
        return client.get_object(bucket, file_id)
    except S3Error:
        return None
