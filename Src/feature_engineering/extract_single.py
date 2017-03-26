# coding=utf-8
"""Helper function used in profile extraction"""
from __future__ import print_function
from utils import redis_util
import copy


def get_date(line):
    return line.split(',')[2].split(' ')[0]


def get_brand(line):
    return line.split(',')[-1]


def get_type(line):
    return line.split(',')[-3]


def get_skuid(line):
    return line.split(',')[1]


def get_userid(line):
    return line.split(',')[0]


def get_cate(line):
    return line.split(',')[-2]


call_tbl = {
    'user': get_userid,
    'sku': get_skuid,
    'cate': get_cate,
    'brand': get_brand
}


def extract_single_from_redis(attr_names, single_key, file_path):
    """Extract single basic profile from redis.

    Args:
        attr_names(list): List of string, e.g. ['user','brand','cate'].
        single_key(str): Primary key(with day time) in the result profile.
        file_path(str): Path to save the profile file.

    Returns:
        None
    """
    file_obj = open(file_path, 'a')
    m_dict = {
        'count': 0
    }
    for attr in attr_names:
        m_dict[attr] = set()

    redis_cli = redis_util.get_basic_db()
    for day in redis_cli.scan_iter():
        value = [copy.deepcopy(m_dict),
                 copy.deepcopy(m_dict),
                 copy.deepcopy(m_dict),
                 copy.deepcopy(m_dict),
                 copy.deepcopy(m_dict),
                 copy.deepcopy(m_dict)]
        res_dict = {}
        len_day = redis_cli.llen(day)
        for i in xrange(len_day):
            line = redis_cli.lindex(day, i).strip()
            key = call_tbl[single_key](line)
            click_type = int(get_type(line))

            attr_values = []
            for attr in attr_names:
                attr_values.append(call_tbl[attr](line))
            if key not in res_dict:
                res_dict[key] = copy.deepcopy(value)

            res_dict[key][click_type - 1]['count'] += 1

            for j in range(len(attr_names)):
                res_dict[key][click_type - 1][attr_names[j]].add(attr_values[j])

        for key, value in res_dict.iteritems():
            line = [key, day]
            for m_type in value:
                line.append(m_type['count'])
                for i in range(len(attr_names)):
                    line.append(len(m_type[attr_names[i]]))

            write_line = ','.join([str(item) for item in line])
            print(write_line, file=file_obj)
    if file_obj:
        file_obj.close()
