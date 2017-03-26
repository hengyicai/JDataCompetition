# coding=utf-8
"""Redis helper"""

import redis
from utils import config_util

HOST = config_util.get('Redis', 'Host')
Port = config_util.get('Redis', 'Port')

__REDIS_BASIC_DB = redis.Redis(host=HOST, port=Port, db=config_util.get('Redis', 'BasicDayActionDB'))
__REDIS_PAIR_DB = redis.Redis(host=HOST, port=Port, db=config_util.get('Redis', 'PairDayActionDB'))


def get_basic_db():
    if __REDIS_BASIC_DB:
        return __REDIS_BASIC_DB
    else:
        return redis.Redis(host=HOST, port=Port, db=config_util.get('Redis', 'BasicDayActionDB'))


def get_pair_db():
    if __REDIS_PAIR_DB:
        return __REDIS_PAIR_DB
    else:
        return redis.Redis(host=HOST, port=Port, db=config_util.get('Redis', 'PairDayActionDB'))
