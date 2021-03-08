import os
from functools import cached_property
from typing import Dict, Type

from pymongo import MongoClient
from pymongo.collection import Collection

from shop.collector import Collector, WbFinDoc, TestCollector
from shop.utils import to_datetime, Task

__all__ = ['get_runner']

MONGO_URL, MONGO_DB = (os.environ[name] for name in ('MONGO_URL', 'MONGO_DB'))


def get_runner(task: Task) -> 'Runner':
    return Runner(platforms[task.platform].collectors[task.doc_type](task))


class Runner:
    collector: Collector

    def __init__(self, collector: Collector):
        self.collector = collector

    @cached_property
    def collection(self) -> Collection:
        return MongoClient(MONGO_URL).get_database(MONGO_DB).get_collection(self.collector.task.doc_type)

    def run(self):
        rows = self.collector.get_rows()

        inserted_id = self.collection.insert_one(self._get_document(rows)).inserted_id

        updates = {}
        for ind, row in enumerate(rows):
            for name, value in self.collector.get_row_updates(row).items():
                updates[f'rows.{ind}.{name}'] = value

        self.collection.update_one({'_id': inserted_id}, {'$set': updates})

    def _get_document(self, rows):
        return {
            'task_id': self.collector.task.id,
            'platform': self.collector.task.platform,
            'doc_type': self.collector.task.doc_type,
            'date': {
                'from': to_datetime(self.collector.task.date_from),
                'to': to_datetime(self.collector.task.date_to)
            },
            'rows': rows
        }


class Platform:
    collectors: Dict[str, Type[Collector]]


class Test(Platform):
    collectors = {
        'fin_monthly': TestCollector
    }


class WB(Platform):
    collectors = {
        'fin_monthly': WbFinDoc
    }


platforms: Dict[str, Type[Platform]] = {
    'test': Test,
    'wb': WB
}

