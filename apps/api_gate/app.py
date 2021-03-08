from http import HTTPStatus

from flask import Flask, jsonify, Response

import storage
from report import report_bp


app = Flask(__name__)
app.config.from_object('config')
app.register_blueprint(report_bp)


@app.route('/')
def index():
    return jsonify({'status': 'ok'}), HTTPStatus.OK


@app.route('/reports/<string:_id>')
def report(_id: str):
    file_rsp = storage.get(_id)

    return Response(
        file_rsp.stream(32 * 1024),
        headers=dict(file_rsp.headers),
        direct_passthrough=True
    )


if __name__ == '__main__':
    app.run(threaded=True)
