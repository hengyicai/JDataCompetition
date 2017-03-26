# coding=utf-8
"""Generate brand data(per day) from action data"""
from __future__ import print_function

import copy
import redis

user_data_path = '../../Res/user_action.txt'
user_data_file = open(user_data_path, 'a')


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


def __get_cate(line):
    return line.split(',')[-2]


def extract_user_from_redis():
    m_dict = {
        'count': 0,
        'sku': set(),
        'brand': set(),
        'cate': set()
    }
    redis_cli = redis.Redis(host='10.30.6.33', db=0, port=6379)
    for day in redis_cli.scan_iter():

        user_list = [copy.deepcopy(m_dict),
                     copy.deepcopy(m_dict),
                     copy.deepcopy(m_dict),
                     copy.deepcopy(m_dict),
                     copy.deepcopy(m_dict),
                     copy.deepcopy(m_dict)]
        user_dict = {}
        # {'user_id' : user_list, 'user_id' : user_list}
        len_day = redis_cli.llen(day)
        for i in xrange(len_day):
            line = redis_cli.lindex(day, i).strip()
            user_id = __get_userid(line)

            click_type = int(__get_type(line))
            sku_id = __get_skuid(line)
            brand_id = __get_brand(line)
            cate_id = __get_cate(line)
            if user_id not in user_dict:
                user_dict[user_id] = copy.deepcopy(user_list)

            user_dict[user_id][click_type - 1]['count'] += 1
            user_dict[user_id][click_type - 1]['sku'].add(sku_id)
            user_dict[user_id][click_type - 1]['brand'].add(brand_id)
            user_dict[user_id][click_type - 1]['cate'].add(cate_id)

        for user_id, user_list in user_dict.iteritems():
            line = [user_id, day]
            for m_type in user_list:
                line.append(m_type['count'])
                line.append(len(m_type['sku']))
                line.append(len(m_type['brand']))
                line.append(len(m_type['cate']))

            write_line = ','.join([str(item) for item in line])

            print(write_line, file=user_data_file)
    user_data_file.close()


if __name__ == '__main__':
    # uncomment this to build the profile
    extract_user_from_redis()

    # test redis connection
    # redis_cli = redis.Redis(host='10.30.6.33', db=0, port=6379)
    # print(redis_cli.info(section='keyspace'))
