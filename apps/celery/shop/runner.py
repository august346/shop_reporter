from typing import Dict, Type, List

import requests
from requests import Response

from shop.collector import Collector, WbFinDoc, TestCollector

from utils import Report, get_reports_url, assert_patch

__all__ = ['get_runner']


def get_runner(report: Report) -> 'Runner':
    return Runner(platforms[report.platform].collectors[report.doc_type](report))


class Runner:
    collector: Collector
    rows: List[dict]

    def __init__(self, collector: Collector):
        self.collector = collector

    def run(self):
        for upd_getter in (self.get_rows, self.get_updates):
            rsp: Response = requests.patch(
                f'{get_reports_url()}/{self.collector.report.id}',
                json={'updates': {'$set': upd_getter()}}
            )
            assert_patch(rsp)

    def get_rows(self) -> dict:
        self.rows = self.collector.get_rows()

        return {'rows': self.rows}

    def get_updates(self) -> dict:
        updates = {}

        for ind, row in enumerate(self.rows):
            for name, value in self.collector.get_row_updates(row).items():
                updates[f'rows.{ind}.{name}'] = value

        return updates


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

