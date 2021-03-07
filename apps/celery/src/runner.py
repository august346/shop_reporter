import os
from datetime import datetime
from functools import cached_property
from typing import Type

from pymongo import MongoClient
from pymongo.collection import Collection

from src.rows_getter import WbRowsGetter, RowsGetter
from src.task import Task
from src.updater import Updater, WbUpdater

__all__ = ['Runner']

MONGO_URL, MONGO_DB = (os.environ[name] for name in ('MONGO_URL', 'MONGO_DB'))


def to_datetime(date):
    return datetime.combine(date, datetime.min.time())


class Runner:
    id_key: str
    rows_getter: Type[RowsGetter]
    updates_getter: Type[Updater]

    @staticmethod
    def get_runner(task: Task) -> 'Runner':
        return {
            'wb': WbRunner,
            'test': TestRunner
        }[task.platform](task)

    def __init__(self, task: Task):
        self.task = task

    @cached_property
    def collection(self) -> Collection:
        return MongoClient(MONGO_URL).get_database(MONGO_DB).get_collection(self.task.doc_type)

    def run(self):
        ids = []
        rows = []

        for row in self.rows_getter(to_datetime(self.task.date_from), to_datetime(self.task.date_to))():
            ids.append(row[self.id_key])
            rows.append(row)

        inserted_id = self.collection.insert_one(self.get_document(rows)).inserted_id

        updates = {}
        for ind, _id in enumerate(ids):
            for name, value in self.updates_getter(_id)().items():
                updates[f'rows.{ind}.{name}'] = value

        self.collection.update_one({'_id': inserted_id}, {'$set': updates})

    def get_document(self, rows):
        return {
            'task_id': self.task.id,
            'platform': self.task.platform,
            'doc_type': self.task.doc_type,
            'date': {
                'from': to_datetime(self.task.date_from),
                'to': to_datetime(self.task.date_to)
            },
            'rows': rows
        }


class TestRunner(Runner):
    id_key = 'id'

    def rows_getter(self, *args, **kwargs):
        return self._rows_getter

    def _rows_getter(self):
        for i in range(10):
            yield {
                'id': i,
                'data': f'test_#{i}'
            }

    def updates_getter(self, _id):
        return lambda: self._updates_getter(_id)

    def _updates_getter(self, _id):
        return {
            'name': f'name_#{_id}',
            'not_name': f'not_name_#{_id}',
        }


class WbRunner(Runner):
    id_key = 'nm_id'
    rows_getter = WbRowsGetter
    updates_getter = WbUpdater
