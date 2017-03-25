# coding=utf-8
"""Generate brand data(per day) from action data"""
from __future__ import print_function

from Src.utils import config_util
import copy

brand_data_path = '../../Res/brand_action.txt'
brand_data_file = open(brand_data_path, 'a')


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


def process_group(group):
    """Process each group, line within one group share the same day

    Args:
        group(list): List of line(striped)

    Returns:
        None

    """
    brand_list = [{
        'count': 0,
        'user': set(),
        'sku': set()
    }] * 6

    brand_dict = {}
    # {'brand_id' : brand_list, 'brand_id' : brand_list}
    for line in group:
        brand_id = __get_brand(line)
        click_type = int(__get_type(line))
        sku_id = __get_skuid(line)
        user_id = __get_userid(line)
        if brand_id not in brand_dict:
            brand_dict[brand_id] = copy.deepcopy(brand_list)

        brand_dict[brand_id][click_type - 1]['count'] += 1
        brand_dict[brand_id][click_type - 1]['user'].add(user_id)
        brand_dict[brand_id][click_type - 1]['sku'].add(sku_id)

    for brand_id, brand_list in brand_dict.iteritems():
        line = [brand_id, __get_date(group[0])]
        for m_type in brand_list:
            line.append(m_type['count'])
            line.append(len(m_type['user']))
            line.append(len(m_type['sku']))

        write_line = ','.join([str(item) for item in line])
        print(write_line, file=brand_data_file)


def extract_brand_from(action_path):
    with open(action_path) as m_f:
        m_f.readline()
        line = m_f.readline()
        group = []
        pre_date = __get_date(line)
        group.append(line.strip())
        line = m_f.readline()
        while line:
            cur_date = __get_date(line)
            if cur_date == pre_date:
                # same group
                group.append(line.strip())
            else:
                # new group begin
                process_group(group)
                group = []
            pre_date = cur_date
            line = m_f.readline()
        process_group(group)


def main():
    action_path_keys = ['Action02',
                        'Action03',
                        'Action03_extra',
                        'Action04']
    action_paths = [config_util.get('Path', item) for item in action_path_keys]
    for path in action_paths:
        extract_brand_from(path)


if __name__ == '__main__':
    main()
