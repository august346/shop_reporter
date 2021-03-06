from logging import Logger
from os import environ
from celery import Celery

environ.setdefault('CELERY_CONFIG_MODULE', 'config')

app = Celery()
app.config_from_envvar('CELERY_CONFIG_MODULE')

logger = Logger(__name__)


@app.task
def run_new():
    return "hello world"
