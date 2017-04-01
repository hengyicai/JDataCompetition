# coding=utf-8
"""从 user 库中抽取每一个 user 的相关特征，
然后再添加到当前 user 的这条 document 中"""
from __future__ import print_function
import mongo_helper
import datetime
import copy

type_arr = ['skim', 'add_cart', 'del_cart', 'order', 'attention', 'click']


def gen_day_week_mapping():
    # 日期与周的映射
    day_format = '%Y-%m-%d'
    dt = datetime.datetime(2016, 01, 31)
    end = datetime.datetime(2016, 04, 15)
    step = datetime.timedelta(days=1)
    result = []

    while dt <= end:
        result.append(dt.strftime(day_format))
        dt += step
    m_day_week = {}
    for day in result:
        m_day_week[day] = datetime.datetime.strptime(day, day_format).isocalendar()[1]
    m_day_week['2016-01-31'] = 5
    return m_day_week

day_week_dict = gen_day_week_mapping()


def add_week_count_attr():
    # 添加与周行为次数相关的属性
    coll = mongo_helper.get_coll('users_action', 'JData')
    cursor = coll.find(no_cursor_timeout=True)

    for user in cursor:
        # process one user
        type_count = {
            'skim': 0,  # 1
            'add_cart': 0,  # 2
            'del_cart': 0,  # 3
            'order': 0,  # 4
            'attention': 0,  # 5
            'click': 0  # 6
        }
        week_dict = {}

        for day in user["day_actions"]:
            day = dict(day)
            day_str = day.keys()[0]
            if day_str not in week_dict:
                week_dict[day_week_dict[day_str]] = copy.deepcopy(type_count)
            for m_type in type_arr:
                    week_dict[day_week_dict[day_str]][m_type] += len(day[day_str][m_type])
        for week_num, val in week_dict.iteritems():
            coll.update(
                {
                    'user_id': user["user_id"]
                },
                {
                    '$set': {
                        'week_' + str(week_num) + '_count': val
                    }
                }
            )
    cursor.close()


def add_week_day_attr():
    # 添加与周行为天数相关的属性
    coll = mongo_helper.get_coll('users_action', 'JData')
    cursor = coll.find(no_cursor_timeout=True)

    for user in cursor:
        # process one user
        type_day_count = {
            'skim': set(),  # 1
            'add_cart': set(),  # 2
            'del_cart': set(),  # 3
            'order': set(),  # 4
            'attention': set(),  # 5
            'click': set()  # 6
        }
        week_dict = {}

        for day in user["day_actions"]:
            day = dict(day)
            day_str = day.keys()[0]
            if day_str not in week_dict:
                week_dict[day_week_dict[day_str]] = copy.deepcopy(type_day_count)
            for m_type in type_arr:
                if day[day_str][m_type]:
                    week_dict[day_week_dict[day_str]][m_type].add(day_str)
        for week_num, val in week_dict.iteritems():
            for m_type in type_arr:
                val[m_type] = len(val[m_type])
            coll.update(
                {
                    'user_id': user["user_id"]
                },
                {
                    '$set': {
                        str(week_num) + '_day_count': val
                    }
                }
            )
    cursor.close()


def add_week_attr():
    # 添加与周{商品数，品牌数，种类数，模块数}相关的属性
    coll = mongo_helper.get_coll('users_action', 'JData')
    cursor = coll.find(no_cursor_timeout=True)

    for user in cursor:
        # process one user
        type_count = {
            'skim': set(),  # 1
            'add_cart': set(),  # 2
            'del_cart': set(),  # 3
            'order': set(),  # 4
            'attention': set(),  # 5
            'click': set()  # 6
        }
        m_dict = {
            'sku_id': copy.deepcopy(type_count),
            'brand': copy.deepcopy(type_count),
            'cate': copy.deepcopy(type_count),
            'model_id': copy.deepcopy(type_count)
        }
        week_dict = {}
        m_arr = m_dict.keys()
        for day in user["day_actions"]:
            day = dict(day)
            day_str = day.keys()[0]
            if day_str not in week_dict:
                week_dict[day_week_dict[day_str]] = copy.deepcopy(m_dict)
            for m_type in type_arr:
                for key in m_arr:
                    item_arr = day[day_str][m_type]
                    # item_arr like this: (maybe empty)
                    '''[
                            {
                                "model_id" : "0",
                                "brand" : "677",
                                "sku_id" : "30272",
                                "cate" : "8"
                            },
                            {
                                "model_id" : "216",
                                "brand" : "677",
                                "sku_id" : "30272",
                                "cate" : "8"
                            },
                            {
                                "model_id" : "217",
                                "brand" : "677",
                                "sku_id" : "30272",
                                "cate" : "8"
                            }
                        ]
                    '''
                    if item_arr:
                        for item in item_arr:
                            if item[key]:
                                week_dict[day_week_dict[day_str]][key][m_type].add(item[key])

        for week_num, val in week_dict.iteritems():
            for item in m_arr:
                for m_type in type_arr:
                    val[item][m_type] = list(val[item][m_type])
            coll.update(
                {
                    'user_id': user["user_id"]
                },
                {
                    '$set': {
                        'week_' + str(week_num): val
                    }
                }
            )
    cursor.close()


def main():
    add_week_count_attr()
    # add_week_attr()
    # add_week_day_attr()

if __name__ == '__main__':
    main()
