from http import HTTPStatus

from flask import Flask, jsonify

from task import task_bp


app = Flask(__name__)
app.config.from_object('config')
app.register_blueprint(task_bp)


@app.route('/')
def index():
    return jsonify({'status': 'ok'}), HTTPStatus.OK


if __name__ == '__main__':
    app.run(threaded=True)
