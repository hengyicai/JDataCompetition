# coding=utf-8

from Src.utils import redis_util
import operator

from Src.utils.common_util import cmp_day

redis_cli = redis_util.get_basic_db()


def main():
    day_dict = {}
    for day in redis_cli.scan_iter():
        day_dict[day] = redis_cli.llen(day)
    print('before' + str(day_dict))
    day_dict = sorted(day_dict.items(), key=operator.itemgetter(0), cmp=cmp_day, reverse=True)
    print('after' + str(day_dict))
    with open('../../Res/day_count.csv', 'a') as m_f:
        for item in day_dict:
            m_f.write(item[0] + ',' + str(item[1]) + '\n')


if __name__ == '__main__':
    main()
