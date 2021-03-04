from flask import Flask

from task import task_bp


app = Flask(__name__)
app.config.from_object('config')
app.register_blueprint(task_bp)


if __name__ == '__main__':
    app.run(threaded=True)
