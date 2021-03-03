from flask import Flask

from db import db

from task import task_bp

app = Flask(__name__)
app.config.from_object('config')
app.register_blueprint(task_bp)
db.init_app(app)

with app.app_context():
    db.create_all()


if __name__ == '__main__':
    app.run(threaded=True)
