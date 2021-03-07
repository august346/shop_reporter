import os
import time
from datetime import datetime
from typing import List

import requests
from requests import Response


class RowsGetter:
    def __init__(self, date_from: datetime, date_to: datetime):
        self.date_from = date_from
        self.date_to = date_to

    def __call__(self):
        raise NotImplementedError


class WbRowsGetter(RowsGetter):
    api_key: str = os.environ['WB_API_KEY']
    url: str = 'https://suppliers-stats.wildberries.ru/api/v1/supplier/reportDetailByPeriod'
    sleep_between: int = 1
    common_keys = ('nm_id', 'barcode', 'sa_name')
    unique_keys = (
        'realizationreport_id', 'order_dt', 'sale_dt', 'supplier_reward', 'supplier_oper_name', 'quantity',
        'delivery_rub'
    )
    datetime_keys = ('order_dt', 'sale_dt',)

    def __call__(self):
        return self.aggregated

    @property
    def aggregated(self) -> List[dict]:
        result = {}

        for data in self.payloads:
            barcode = data['barcode']

            if barcode not in result:
                result[barcode] = self.get_common_fields(data)
                result[barcode]['reports'] = []

            result[barcode]['reports'].append(self.get_unique_fields(data))

        return list(result.values())

    def get_common_fields(self, data: dict):
        return {k: data[k] for k in self.common_keys}

    def get_unique_fields(self, data: dict):
        result = {k: data[k] for k in self.unique_keys}

        for key in self.datetime_keys:
            result[key] = datetime.fromisoformat(result[key])

        return result

    @property
    def payloads(self):
        _id = 0

        while _id is not None:
            json = self.do_request(_id).json()

            if not json:
                return

            yield from json

            _id = max([p['rrd_id'] for p in json])

            time.sleep(self.sleep_between)

    def do_request(self, _id) -> Response:
        return requests.get(
            self.url,
            params=dict(
                key=self.api_key,
                limit=1000,
                rrdid=_id,
                dateFrom=self.date_from.date().isoformat(),
                dateTo=self.date_to.date().isoformat()
            )
        )
