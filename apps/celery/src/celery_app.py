import os

from celery import Celery

os.environ.setdefault('CELERY_CONFIG_MODULE', 'config')

app = Celery()
app.config_from_envvar('CELERY_CONFIG_MODULE')
