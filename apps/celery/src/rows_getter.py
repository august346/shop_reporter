import os
import time
from datetime import datetime
from typing import Iterable, List

import requests


class RowsGetter:
    def __init__(self, date_from: datetime, date_to: datetime):
        self.date_from = date_from
        self.date_to = date_to

    def __call__(self):
        return self.aggregate(self.payloads)

    def aggregate(self, payloads: Iterable[dict]) -> List[dict]:
        raise NotImplementedError

    @property
    def payloads(self):
        raise NotImplementedError


class WbRowsGetter(RowsGetter):
    api_key: str = os.environ['WB_API_KEY']
    url: str = 'https://suppliers-stats.wildberries.ru/api/v1/supplier/reportDetailByPeriod'
    sleep_between: int = 1
    common_keys = ('nm_id', 'barcode', 'sa_name')
    unique_keys = (
        'realizationreport_id', 'order_dt', 'supplier_reward', 'supplier_oper_name', 'quantity', 'delivery_rub'
    )

    def aggregate(self, payloads: Iterable[dict]) -> List[dict]:
        result = {}

        for data in payloads:
            barcode = data['barcode']

            if barcode not in result:
                result[barcode] = {
                    k: data[k]
                    for k in self.common_keys
                }
                result[barcode]['payloads'] = []

            result[barcode]['payloads'].append({
                key: data[key]
                for key in self.unique_keys
            })

        return list(result.values())

    @property
    def payloads(self):
        _id = 0

        while _id is not None:
            rsp = requests.get(
                self.url,
                params=dict(
                    key=self.api_key,
                    limit=1000,
                    rrdid=_id,
                    dateFrom=self.date_from.isoformat(),
                    dateto=self.date_to.isoformat()
                )
            )
            payload = rsp.json()

            if not payload:
                return

            yield from payload

            _id = max([p['rrd_id'] for p in payload])

            time.sleep(self.sleep_between)
