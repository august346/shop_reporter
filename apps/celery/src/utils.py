import time
from datetime import datetime
from functools import partial, wraps
from typing import Callable


def to_datetime(date):
    return datetime.combine(date, datetime.min.time())


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
