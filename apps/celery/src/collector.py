import os
import time
from datetime import datetime
from functools import lru_cache
from http import HTTPStatus
from typing import List, Dict, Any, Iterable

import requests
from bs4 import BeautifulSoup
from requests import Response

from .utils import Report, paused


class Collector:
    report: Report

    def __init__(self, report: Report):
        self.report = report

    def get_rows(self) -> List[dict]:
        raise NotImplementedError

    def get_row_updates(self, row: dict) -> Dict[str, Any]:
        raise NotImplementedError


class TestCollector(Collector):

    def get_rows(self) -> List[dict]:
        return [{
            'id': i,
            'data': f'test_#{i}'
        } for i in range(10)]

    def get_row_updates(self, row: dict) -> Dict[str, Any]:
        return {
            'name': f'name_#{row["id"]}',
            'not_name': f'not_name_#{row["id"]}',
        }


class WbFinDoc(Collector):
    api_key: str = os.environ['WB_API_KEY']
    url: str = 'https://suppliers-stats.wildberries.ru/api/v1/supplier/reportDetailByPeriod'
    sleep_between: int = 1
    common_keys = ('nm_id', 'barcode', 'sa_name')
    unique_keys = (
        'realizationreport_id', 'order_dt', 'sale_dt', 'supplier_reward', 'supplier_oper_name', 'quantity',
        'delivery_rub'
    )
    datetime_keys = ('order_dt', 'sale_dt',)

    def get_rows(self) -> List[dict]:
        return self._get_aggregated()

    def _get_aggregated(self) -> List[dict]:
        result = {}

        for data in self._get_payloads():
            barcode = data['barcode']

            if barcode not in result:
                result[barcode] = self._get_common_fields(data)
                result[barcode]['reports'] = []

            result[barcode]['reports'].append(self._get_unique_fields(data))

        return list(result.values())

    def _get_common_fields(self, data: dict):
        return {k: data[k] for k in self.common_keys}

    def _get_unique_fields(self, data: dict):
        result = {k: data[k] for k in self.unique_keys}

        for key in self.datetime_keys:
            result[key] = datetime.fromisoformat(result[key])

        return result

    def _get_payloads(self) -> Iterable[dict]:
        _id = 0

        while _id is not None:
            rsp: Response = self._do_request(_id)
            assert rsp.status_code == HTTPStatus.OK, (rsp, rsp.content.decode(), rsp.request.url)

            json = rsp.json()

            if not json:
                return

            yield from json

            _id = max([p['rrd_id'] for p in json])

            time.sleep(self.sleep_between)

    def _do_request(self, _id) -> Response:
        return requests.get(
            self.url,
            params=dict(
                key=self.api_key,
                limit=1000,
                rrdid=_id,
                dateFrom=self.report.date_from.date().isoformat(),
                dateTo=self.report.date_to.date().isoformat()
            )
        )

    def get_row_updates(self, row: dict) -> Dict[str, Any]:
        return {
            'name': self._get_name(row['nm_id'])
        }

    @staticmethod
    @lru_cache(maxsize=5000)
    @paused(seconds=1)
    def _get_name(nm_id: str) -> 'str':
        tag = WbFinDoc._get_soup(nm_id).find(
            'span',
            {'class': 'name'}
        )
        if tag:
            return text.strip() if (text := tag.text) else text
        raise ValueError('No span_class_name in response!')

    @staticmethod
    def _get_soup(nm_id: str) -> BeautifulSoup:
        rsp = requests.get(
            f'https://www.wildberries.ru/catalog/{nm_id}/detail.aspx',
            headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) '
                              'Chrome/39.0.2171.95 Safari/537.36 '
            },
        )

        if rsp.status_code != 200:
            raise ResourceWarning(f'Invalid {rsp.request.url=}, {rsp.status_code=} with {rsp.content=}')

        return BeautifulSoup(rsp.content.decode('utf-8'), 'html.parser')
