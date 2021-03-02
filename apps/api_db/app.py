from http import HTTPStatus

from flask import Flask, jsonify

from db import db

from task import task_bp

app = Flask(__name__)
app.config.from_object('config')
app.register_blueprint(task_bp)
db.init_app(app)

with app.app_context():
    from models import Task
    db.create_all()


@app.route('/')
def index():
    return jsonify({'status': 'ok'}), HTTPStatus.OK


if __name__ == '__main__':
    app.run(threaded=True)
