from datetime import datetime
from http import HTTPStatus

import requests
from requests import Response

from celery_app import app
from shop.runner import get_runner
from utils import Report, get_reports_url


@app.task
def run_new():
    reports = requests.get(
        get_reports_url(),
        json={'filters': {'state': 'init'}}
    ).json()

    for r in reports:
        execute.delay(r)

    return len(reports)


@app.task
def execute(report_info: dict):
    for d_key in ('date_from', 'date_to'):
        report_info[d_key] = datetime.fromisoformat(report_info[d_key])

    report: Report = Report(**report_info)

    runner = get_runner(report)

    task_url = f'{get_reports_url()}/{report.id}/'

    rsp: Response = requests.patch(task_url, json={
        'filters': {'state': 'init'},
        'updates': {'$set': {'state': 'process'}}
    })
    assert rsp.status_code == HTTPStatus.OK, rsp.content.decode()

    runner.run()

    return requests.patch(task_url, json={
        'filters': {'state': 'process'},
        'updates': {'$set': {'state': 'complete'}}
    }).json()
