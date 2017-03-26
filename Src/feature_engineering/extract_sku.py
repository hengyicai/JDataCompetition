# coding=utf-8
"""Generate brand data(per day) from action data"""
from __future__ import print_function

import copy
import redis

sku_data_path = '../../Res/sku_action.txt'
sku_data_file = open(sku_data_path, 'a')


def __get_date(line):
    return line.split(',')[2].split(' ')[0]


def __get_brand(line):
    return line.split(',')[-1]


def __get_type(line):
    return line.split(',')[-3]


def __get_skuid(line):
    return line.split(',')[1]


def __get_userid(line):
    return line.split(',')[0]


def extract_sku_from_redis():
    m_dict = {
        'count': 0,
        'user': set(),
    }
    redis_cli = redis.Redis(host='10.30.6.33', db=0, port=6379)
    for day in redis_cli.scan_iter():
        sku_list = [copy.deepcopy(m_dict),
                      copy.deepcopy(m_dict),
                      copy.deepcopy(m_dict),
                      copy.deepcopy(m_dict),
                      copy.deepcopy(m_dict),
                      copy.deepcopy(m_dict)]
        sku_dict = {}
        # {'sku_id' : sku_list, 'sku_id' : sku_list}
        len_day = redis_cli.llen(day)
        for i in xrange(len_day):
            line = redis_cli.lindex(day, i).strip()
            sku_id = __get_skuid(line)
            click_type = int(__get_type(line))
            user_id = __get_userid(line)
            if sku_id not in sku_dict:
                sku_dict[sku_id] = copy.deepcopy(sku_list)

            sku_dict[sku_id][click_type - 1]['count'] += 1
            sku_dict[sku_id][click_type - 1]['user'].add(user_id)

        for sku_id, sku_list in sku_dict.iteritems():
            line = [sku_id, day]
            for m_type in sku_list:
                line.append(m_type['count'])
                line.append(len(m_type['user']))

            write_line = ','.join([str(item) for item in line])
            print(write_line, file=sku_data_file)

if __name__ == '__main__':
    # uncomment this to build the profile
    extract_sku_from_redis()
