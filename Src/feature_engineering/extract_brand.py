# coding=utf-8
"""Generate brand data(per day) from action data"""
from __future__ import print_function

import copy
from Src.utils import redis_util
import extract_single

brand_data_path = '../../Res/brand_action.txt'
brand_data_file = open(brand_data_path, 'a')


def extract_brand_from_redis():
    m_dict = {
        'count': 0,
        'user': set(),
        'sku': set()
    }
    redis_cli = redis_util.get_basic_db()
    for day in redis_cli.scan_iter():
        brand_list = [copy.deepcopy(m_dict),
                      copy.deepcopy(m_dict),
                      copy.deepcopy(m_dict),
                      copy.deepcopy(m_dict),
                      copy.deepcopy(m_dict),
                      copy.deepcopy(m_dict)]
        brand_dict = {}
        # {'brand_id' : brand_list, 'brand_id' : brand_list}
        len_day = redis_cli.llen(day)
        for i in xrange(len_day):
            line = redis_cli.lindex(day, i).strip()
            brand_id = extract_single.get_brand(line)
            click_type = int(extract_single.get_type(line))
            sku_id = extract_single.get_skuid(line)
            user_id = extract_single.get_userid(line)
            if brand_id not in brand_dict:
                brand_dict[brand_id] = copy.deepcopy(brand_list)

            brand_dict[brand_id][click_type - 1]['count'] += 1
            brand_dict[brand_id][click_type - 1]['user'].add(user_id)
            brand_dict[brand_id][click_type - 1]['sku'].add(sku_id)

        for brand_id, brand_list in brand_dict.iteritems():
            line = [brand_id, day]
            for m_type in brand_list:
                line.append(m_type['count'])
                line.append(len(m_type['user']))
                line.append(len(m_type['sku']))

            write_line = ','.join([str(item) for item in line])
            print(write_line, file=brand_data_file)

if __name__ == '__main__':
    # uncomment this to build the profile
    extract_brand_from_redis()

    # test redis connection
    # redis_cli = redis.Redis(host='10.30.6.33', db=0, port=6379)
    # print(redis_cli.info(section='keyspace'))
