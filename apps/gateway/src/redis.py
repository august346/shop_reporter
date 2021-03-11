import os

from redis import Redis

REDIS_HOST = os.environ['REDIS_URL']

client = Redis(host=REDIS_HOST)
