from datetime import datetime

import requests
from requests import Response

from celery_app import app
from shop.runner import get_runner
from utils import Report, get_reports_url, assert_patch


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

    report_url = f'{get_reports_url()}/{report.id}/'

    rsp: Response = requests.patch(report_url, json={
        'filters': {'state': 'init'},
        'updates': {'$set': {'state': 'process'}}
    })
    assert_patch(rsp)

    runner.run()

    rsp: Response = requests.patch(report_url, json={
        'filters': {'state': 'process'},
        'updates': {'$set': {'state': 'complete'}}
    })
    assert_patch(rsp)

    return rsp.json()
