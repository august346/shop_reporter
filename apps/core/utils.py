from datetime import datetime


def to_datetime(date):
    return datetime.combine(date, datetime.min.time())