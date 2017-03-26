# coding=utf-8
"""Common utils function"""
import datetime


def cmp_day(a, b):
    a_datetime = datetime.datetime.strptime(a, '%Y-%m-%d')
    b_datetime = datetime.datetime.strptime(b, '%Y-%m-%d')

    if a_datetime > b_datetime:
        return -1
    elif a_datetime < b_datetime:
        return 1
    else:
        return 0
