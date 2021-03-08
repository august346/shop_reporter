from datetime import date

import requests

from celery_app import app
from shop.runner import get_runner
from shop.utils import Task


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

    runner = get_runner(task)

    task_url = f'{app.conf["DB_TASKS"]}/{task.id}/'

    requests.patch(task_url, json={
        'filter': {'status': 'init'},
        'update': {'status': 'process'}
    }).json()

    runner.run()

    return requests.patch(task_url, json={
        'filter': {'status': 'process'},
        'update': {'status': 'complete'}
    }).json()
