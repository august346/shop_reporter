import time
from dataclasses import dataclass
from datetime import datetime, date
from functools import partial, wraps
from typing import Callable, Dict

from celery_app import app


@dataclass
class Report:
    _id: str
    platform: str
    doc_type: str
    state: str
    date_from: datetime
    date_to: datetime
    files: Dict[str, str]

    @property
    def id(self):
        return self._id


def paused(f: Callable = None, seconds: float = 1):
    if not f:
        return partial(paused, seconds=seconds)

    @wraps(f)
    def wrapper(*args, **kwargs):
        await_time = time.time()

        if wrapper.previous_timestamp:
            await_time = wrapper.previous_timestamp + seconds

        while await_time > time.time():
            time.sleep(0.1)

        result = f(*args, **kwargs)

        wrapper.previous_timestamp = time.time()

        return result

    wrapper.previous_timestamp = None

    return wrapper


def get_reports_url():
    return app.conf['CORE_REPORTS_URL']
