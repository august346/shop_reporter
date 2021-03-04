from http import HTTPStatus

import requests
from flask import Blueprint, current_app
from flask_restful import Api, Resource
from requests import Response

task_bp = Blueprint('tasks', __name__, url_prefix='/tasks')
api = Api(task_bp)


class Task(Resource):
    def post(self, task_id: str):
        rsp: Response = requests.patch(
            f'{current_app.config["DB_URL"]}/tasks/{task_id}',
            json={"status": "process"}
        )
        assert rsp.status_code == HTTPStatus.OK, rsp.status_code

        ...

        return '', HTTPStatus.NO_CONTENT


api.add_resource(Task, '/<string:task_id>/')
