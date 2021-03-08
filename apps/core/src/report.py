from http import HTTPStatus

from flask import Blueprint
from flask_restful import Api, Resource

from .mongo import Report

report_bp = Blueprint('reports', __name__, url_prefix='/reports')
api = Api(report_bp)


class ReportResource(Resource):
    def get(self, report_id: str):
        return Report().get(report_id) or (None, HTTPStatus.NOT_FOUND)

    def patch(self, report_id: str):
        counter = Report().update(report_id)
        return counter, HTTPStatus.OK if counter else HTTPStatus.NOT_FOUND


class ReportListResource(Resource):
    def get(self):
        return Report().get_many()

    def post(self):
        return Report().create(), HTTPStatus.CREATED


api.add_resource(ReportListResource, '/')
api.add_resource(ReportResource, '/<string:report_id>/')
