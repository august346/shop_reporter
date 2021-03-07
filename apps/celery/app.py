from datetime import date
from logging import Logger
from os import environ

import requests
from celery import Celery

from src.runner import Runner
from task import Task

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


@app.task
def execute(task_info: dict):
    for d_key in ('date_from', 'date_to'):
        task_info[d_key] = date.fromisoformat(task_info[d_key])

    task: Task = Task(**task_info)

    runner: Runner = Runner.get_runner(task)

    requests.patch(f'{app.conf["DB_TASKS"]}/{task.id}/', json={
        'filter': {'status': 'init'},
        'update': {'status': 'process'}
    }).json()

    runner.run()

    return requests.patch(f'{app.conf["DB_TASKS"]}/{task.id}/', json={
        'filter': {'status': 'process'},
        'update': {'status': 'complete'}
    }).json()
