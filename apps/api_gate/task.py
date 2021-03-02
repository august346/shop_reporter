from http import HTTPStatus

import requests
from flask import Blueprint, request, current_app
from flask_restful import Api, Resource
from requests import Response

task_bp = Blueprint('tasks', __name__, url_prefix='/tasks')
api = Api(task_bp)


class Task(Resource):
    def get(self, task_id: str):
        rsp: Response = requests.get(f'{current_app.config["DB_URL"]}/tasks/{task_id}')
        return rsp.json()


class TaskList(Resource):
    def post(self):
        rsp: Response = requests.post(f'{current_app.config["DB_URL"]}/tasks/', json=request.json)
        # requests.post(current_app.config['PROCESS_URL'])
        return rsp.json(), HTTPStatus.CREATED


api.add_resource(TaskList, '/')
api.add_resource(Task, '/<string:task_id>/')
