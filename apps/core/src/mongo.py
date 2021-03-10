import os
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from bson import ObjectId
from flask import request

from flask_pymongo import PyMongo
from flask_pymongo.wrappers import Collection

connection: PyMongo = PyMongo()

_collection_names = {
    'report': os.environ['MONGO_COL_NAME_REPORT']
}


def get_collection(name: str) -> Collection:
    return connection.db.get_collection(_collection_names[name])


@dataclass
class CreateParams:
    necessary: List[str]
    dates: List[str]


class Report:
    _collection: Collection
    _create_params: CreateParams = CreateParams(
        necessary=['platform', 'doc_type', 'date_from', 'date_to', 'files'],
        dates=['date_from', 'date_to']
    )

    def __init__(self):
        self._collection = get_collection('report')

    def create(self) -> str:
        document = {key: request.json[key] for key in self._create_params.necessary}

        for date_key in self._create_params.dates:
            document[date_key] = datetime.fromisoformat(document[date_key])

        document['state'] = 'init'

        return str(self._collection.insert_one(document).inserted_id)

    def update(self, oid: str) -> int:
        updates = request.json['updates']
        filters = request.json.get('filters')

        if filters is None:
            filters = {}

        filters['_id'] = ObjectId(oid)

        return self._collection.update_one(filters, updates).modified_count

    def get(self, oid: str):
        return self.prepare_json(self._collection.find_one({'_id': ObjectId(oid)}, self.get_project))

    def get_many(self):
        return list(map(
            self.prepare_json,
            self._collection.find(request.json['filters'], self.get_project)
        ))

    @property
    def get_project(self) -> dict:
        fields = ['state', 'platform', 'doc_type', 'date_from', 'date_to', 'files']

        if request.json and request.json.get('full'):
            fields.append('rows')

        return {k: 1 for k in fields}

    def prepare_json(self, document: Optional[dict]):
        if document is None:
            return

        document['_id'] = str(document['_id'])

        for date_key in self._create_params.dates:
            document[date_key] = document[date_key].isoformat()

        return document
