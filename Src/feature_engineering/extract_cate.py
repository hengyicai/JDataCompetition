# coding=utf-8
"""Generate cate data(per day) from action data"""


import extract_single

if __name__ == '__main__':
    CATE_FILE = '../../Res/cate_action_day.csv'
    ATTR_NAMES = ['user', 'sku']
    extract_single.extract_single_from_redis(ATTR_NAMES, 'cate', CATE_FILE)
