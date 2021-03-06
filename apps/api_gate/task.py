import requests
from flask import Blueprint, request, current_app
from flask_restful import Api, Resource
from requests import Response

task_bp = Blueprint('tasks', __name__, url_prefix='/tasks')
api = Api(task_bp)


class TaskResource(Resource):
    @property
    def db_tasks_url(self):
        return current_app.config['DB_TASKS']


class Task(TaskResource):

    def get(self, task_id: str):
        rsp: Response = requests.get(f'{self.db_tasks_url}/{task_id}')
        return rsp.json()


class TaskList(TaskResource):
    def post(self):
        rsp: Response = requests.post(self.db_tasks_url, json=request.json)
        return rsp.json(), rsp.status_code


api.add_resource(TaskList, '/')
api.add_resource(Task, '/<string:task_id>/')
