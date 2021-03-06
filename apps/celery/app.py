from dataclasses import dataclass
from logging import Logger
from os import environ

import requests
from celery import Celery

environ.setdefault('CELERY_CONFIG_MODULE', 'config')

app = Celery()
app.config_from_envvar('CELERY_CONFIG_MODULE')

logger = Logger(__name__)


@app.task
def run_new():
    tasks = requests.get(app.conf['DB_TASKS'], json={'filter': {'status': 'init'}}).json()

    for t_info in tasks:
        execute.delay(t_info)

    return len(tasks)


@dataclass
class Task:
    id: str
    platform: str
    doc_type: str
    status: str


@app.task
def execute(task_info: dict):
    task: Task = Task(**task_info)
    return requests.patch(f'{app.conf["DB_TASKS"]}/{task.id}/', json={
        'filter': {'status': 'init'},
        'update': {'status': 'process'}
    }).json()
