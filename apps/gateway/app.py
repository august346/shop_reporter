from http import HTTPStatus

import requests
from flask import Flask, jsonify, Response, request

from src import storage
from src.report import report_bp


app = Flask(__name__)
app.config.from_object('config')
app.register_blueprint(report_bp)


@app.route('/')
def index():
    return jsonify({'status': 'ok'}), HTTPStatus.OK


@app.route('/document/<string:report_id>/')
def report(report_id: str):
    if request.args.get('force_regen') == 'true':
        file_rsp = None
    else:
        file_rsp = storage.get(report_id)

    if file_rsp is None:
        rsp: requests.Response = requests.post(get_gen_url(report_id))
        assert rsp.status_code == HTTPStatus.OK, (rsp.request.url, rsp.content.decode())

        file_rsp = storage.get(rsp.json())

    return Response(
        file_rsp.stream(32 * 1024),
        headers=dict(file_rsp.headers),
        direct_passthrough=True
    )


def get_gen_url(report_id: str):
    return f'{app.config["GENERATOR_URL"]}{report_id}/'


if __name__ == '__main__':
    app.run(threaded=True)
