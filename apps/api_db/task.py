from http import HTTPStatus
from uuid import uuid4, UUID

from flask import Blueprint, request
from flask_restful import Api, Resource

import models

task_bp = Blueprint('tasks', __name__, url_prefix='/tasks')
api = Api(task_bp)

storage = {}


class Task(Resource):
    def get(self, task_id: str):
        t: models.Task = models.Task.query.get(task_id)
        return t.as_dict()


class TaskList(Resource):
    def post(self):
        t = models.Task(**request.json)
        models.db.session.add(t)
        models.db.session.commit()
        return str(t.id), HTTPStatus.CREATED


api.add_resource(TaskList, '/')
api.add_resource(Task, '/<string:task_id>/')
