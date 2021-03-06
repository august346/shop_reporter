import os
from datetime import datetime
from functools import cached_property
from typing import Callable

from pymongo import MongoClient
from pymongo.collection import Collection

from src.task import Task

__all__ = ['run']


MONGO_URL, MONGO_DB = (os.environ[name] for name in ('MONGO_URL', 'MONGO_DB'))


class Runner:
    id_key: str
    rows_getter: Callable
    get_updates: Callable

    @staticmethod
    def generate(task: Task) -> None:
        {
            'test': TestRunner
        }[task.platform](task).run()

    def __init__(self, task: Task):
        self.task = task

    @cached_property
    def collection(self) -> Collection:
        return MongoClient(MONGO_URL).get_database(MONGO_DB).get_collection(self.task.doc_type)

    def run(self):
        ids = {}
        rows = []

        for i, row in enumerate(self.rows_getter(self.task.date_from, self.task.date_to)):
            rows.append(row)
            ids[row[self.id_key]] = i

        inserted_id = self.collection.insert_one(self.get_document(rows)).inserted_id

        updates = {}
        for _id, upd in self.get_updates(*ids):
            for key, value in upd.items():
                updates[f'rows.{ids[_id]}.{key}'] = value

        self.collection.update_one({'_id': inserted_id}, {'$set': updates})

    def get_document(self, rows):
        def to_datetime(date):
            return datetime.combine(date, datetime.min.time())

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


run = Runner.generate


class TestRunner(Runner):
    id_key = 'id'

    def rows_getter(self, *args):
        for i in range(10):
            yield {'id': i, 'data': f'test_#{i}'}

    def get_updates(self, *ids):
        for _id in ids:
            yield _id, {'name': f'name_#{_id}'}
