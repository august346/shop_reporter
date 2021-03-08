from http import HTTPStatus
from typing import Dict, Type

import requests
from requests import Response

from shop.collector import Collector, WbFinDoc, TestCollector
from utils import Report, get_reports_url, assert_patch

__all__ = ['get_runner']


def get_runner(report: Report) -> 'Runner':
    return Runner(platforms[report.platform].collectors[report.doc_type](report))


class Runner:
    collector: Collector

    def __init__(self, collector: Collector):
        self.collector = collector

    def run(self):
        rows = self.collector.get_rows()
        updates = {'$set': {'rows': rows}}

        rsp: Response = requests.patch(
            f'{get_reports_url()}/{self.collector.report.id}',
            json={'updates': updates}
        )
        assert_patch(rsp)

        updates = {}
        for ind, row in enumerate(rows):
            for name, value in self.collector.get_row_updates(row).items():
                updates[f'rows.{ind}.{name}'] = value
        updates = {'$set': updates}

        rsp: Response = requests.patch(
            f'{get_reports_url()}/{self.collector.report.id}',
            json={'updates': updates}
        )
        assert_patch(rsp)


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

