# coding=utf-8
"""将 user 的 action 信息入库，每一个 user 对应一个 document"""
from __future__ import print_function
import sys
import mongo_helper

current_user = None
user = None
user_list = []
type_arr = ['skim', 'add_cart', 'del_cart', 'order', 'attention', 'click']

# config mongodb
coll = mongo_helper.get_coll('users_action', 'JData')


def process_user_day(user_day_list):
    # process a user with a day, this method was called by process_user()
    day_actions = {
        'skim': [],  # 1
        'add_cart': [],  # 2
        'del_cart': [],  # 3
        'order': [],  # 4
        'attention': [],  # 5
        'click': []  # 6
    }
    for line in user_day_list:
        item_arr = line.split(',')
        type = item_arr[-3]
        sku_id = item_arr[1]
        brand = item_arr[-1]
        cate = item_arr[-2]
        model_id = item_arr[3]

        day_actions[type_arr[int(type) - 1]].append({
            'sku_id': sku_id,
            'brand': brand,
            'cate': cate,
            'model_id': model_id
        })
    return {
        user_day_list[0].split(',')[2]: day_actions
    }


def process_user(user_actions):
    # process a user, this method was called by main program
    user_dict = {
        'user_id': user_actions[0].split(',')[0],
        'day_actions': []
    }
    current_day = None
    day = None
    user_day_list = []
    for line in user_actions:
        item_arr = line.split(',')
        day = item_arr[2]
        if current_day == day:
            # same day
            user_day_list.append(line)
        else:
            if current_day:
                # new day begins 
                user_dict['day_actions'].append(process_user_day(user_day_list))
                user_day_list = []
            current_day = day
            user_day_list.append(line)
    if current_day:
        # process last day
        user_dict['day_actions'].append(process_user_day(user_day_list))
    return user_dict


for out_line in sys.stdin:
    out_line = out_line.strip()
    out_item_arr = out_line.split(',')
    user = out_item_arr[0]
    if current_user == user:
        # same user
        user_list.append(out_line)
    else:
        if current_user:
            # new user begins
            # process previous user
            user_dict = process_user(user_list)
            # write this user to mongodb
            mongo_helper.write_mongo(user_dict, coll)
            user_list = []
        current_user = user
        user_list.append(out_line)

if current_user:
    # process last user 
    user_dict = process_user(user_list)
    mongo_helper.write_mongo(user_dict, coll)

mongo_helper.close_mongo()
