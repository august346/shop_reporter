from http import HTTPStatus

import requests
from flask import Flask, jsonify

from src import generator
from src.utils import get_reports_url, Report

app = Flask(__name__)
app.config.from_object('config')


@app.route('/gen/<string:report_id>')
def gen(report_id: str):
    rsp = requests.get(f'{get_reports_url()}/{report_id}/')
    assert rsp.status_code == HTTPStatus.OK

    file_id = generator.gen(Report(**rsp.json()))

    return jsonify(file_id)


if __name__ == '__main__':
    app.run(threaded=True)
