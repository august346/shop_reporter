from copy import copy
from datetime import date
from http import HTTPStatus

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

    def patch(self, task_id: str):
        counter = (
            models.Task.query
            .filter_by(id=task_id, **request.json['filter'])
            .update(request.json['update'])
        )
        models.db.session.commit()
        return counter, HTTPStatus.OK


class TaskList(Resource):
    def get(self):
        return list(map(
            lambda t: t.as_dict(),
            models.Task.query.filter_by(**request.json['filter'])
        ))

    def post(self):
        t_info = copy(request.json)
        for d_key in ('date_from', 'date_to'):
            t_info[d_key] = date.fromisoformat(t_info[d_key])

        t = models.Task(**t_info)
        models.db.session.add(t)
        models.db.session.commit()
        return str(t.id), HTTPStatus.CREATED


api.add_resource(TaskList, '/')
api.add_resource(Task, '/<string:task_id>/')
