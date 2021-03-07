import time
from functools import wraps, lru_cache
from typing import Tuple, Any

import requests
from bs4 import BeautifulSoup


def paused(seconds: float = 1):
    def decorator(f):
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

    return decorator


class UpdGetter:
    name: str = None

    @lru_cache(maxsize=5000)
    def __init__(self, _id: str):
        self.id = _id

    def __call__(self) -> Tuple[str, Any]:
        if not isinstance(self.name, str):
            raise NotImplementedError

        return self.value

    @property
    def value(self):
        raise NotImplementedError


class WbNameGetter(UpdGetter):
    name = 'name'

    @property
    def value(self):
        return self.get_name()

    def get_name(self):
        tag = self.soup.find(
            'span',
            {'class': 'name'}
        )
        if tag:
            time.sleep(1)
            return text.strip() if (text := tag.text) else text
        raise ValueError('No span_class_name in response!')

    @property
    def soup(self):
        rsp = requests.get(
            f'https://www.wildberries.ru/catalog/{self.id}/detail.aspx',
            headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) '
                              'Chrome/39.0.2171.95 Safari/537.36 '
            },
        )
        if rsp.status_code == 200:
            return BeautifulSoup(
                rsp.content.decode('utf-8'),
                'html.parser'
            )
        raise ResourceWarning(f'Invalid {rsp.request.url=}, {rsp.status_code=} with {rsp.content=}')
