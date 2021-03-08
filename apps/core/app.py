from flask import Flask

import mongo

from report import report_bp

app = Flask(__name__)
app.config.from_object('config')
app.register_blueprint(report_bp)
mongo.connection.init_app(app)


if __name__ == '__main__':
    app.run(threaded=True)
