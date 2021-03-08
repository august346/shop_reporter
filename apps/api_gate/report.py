from uuid import uuid4

import requests
from flask import Blueprint, request, current_app
from flask_restful import Api, Resource
from requests import Response

import storage

report_bp = Blueprint('reports', __name__, url_prefix='/reports')
api = Api(report_bp)


class UrlMixin:
    @property
    def db_reports_url(self):
        return current_app.config['CORE_REPORTS_URL']


class Report(UrlMixin, Resource):
    def get(self, report_id: str):
        rsp: Response = requests.get(f'{self.db_reports_url}/{report_id}')
        return rsp.json()


class ReportList(UrlMixin, Resource):
    def post(self):
        file_id = str(uuid4())
        storage.save(file_id)

        json = {
            k: request.form[k]
            for k in ('platform', 'doc_type', 'date_from', 'date_to')
        }
        json['files'] = {'price': file_id}
        rsp: Response = requests.post(self.db_reports_url, json=json)

        ...     # TODO run task

        return rsp.json(), rsp.status_code


api.add_resource(ReportList, '/')
api.add_resource(Report, '/<string:report_id>/')
