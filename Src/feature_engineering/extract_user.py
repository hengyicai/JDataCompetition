# coding=utf-8
import numpy as np
import pandas as pd

from Src.utils import config_util

'''
df['time'] = df['time'].astype(np.datetime64)
for x in  df['time'] :
    print x.year
'''


# pd.to_datetime(df)
def fetch_user_actions(user_id, start_time, end_time):
    """Fetches rows from action log.

    Retrieves rows pertaining to the given user_id from the action data


    Args:
        user_id: int; user_id.
        start_time: date; the statistic start date
        end_time: date; the statistic end date

    Returns:
        A list contains the corresponding records.
        Each row is represented as a tuple of ints, represents the num of
        module clicked, the number of skimming, the # of add to cart, the # of remove from cart,
        the # of placing an order, the # of favourite and the # of click pertaining to the
         specific product_id.
        For example:

        [('product_id','#mod', '#skim','#add_card','#rm_card','#buy','#favor','#click')]

    """
    # end_time : <String> '2005-02-25'
    # <type 'datetime.date'>
    # todo uniform the date format
    end = np.datetime64(end_time).astype(object)
    # filter as day
    end_day = end.day
    end_month = end.month
    start_day = np.datetime64(end_time).astype(object).day
    start_month = np.datetime64(end_time).astype(object).month
    print 'the start month is ' + str(start_month) + ' and end month is ' + str(end_month)
    if start_month == 2 & end_month == 2:
        df = pd.read_csv(config_util.get(section='Path', key='Action02'))
    elif start_month == 2 & end_month == 3:
        df2 = pd.read_csv(config_util.get(section='Path', key='Action02'))
        df3 = pd.read_csv(config_util.get(section='Path', key='Action03'))
        df = pd.concat([df2, df3])
    elif start_month == 2 & end_month == 4:
        df2 = pd.read_csv(config_util.get(section='Path', key='Action02'))
        df3 = pd.read_csv(config_util.get(section='Path', key='Action03'))
        df4 = pd.read_csv(config_util.get(section='Path', key='Action04'))
        df = pd.concat([df2, df3, df4])
    elif start_month == 3 & end_month == 3:
        df = pd.read_csv(config_util.get(section='Path', key='Action03'))
    elif start_month == 3 & end_month == 4:
        df3 = pd.read_csv(config_util.get(section='Path', key='Action03'))
        df4 = pd.read_csv(config_util.get(section='Path', key='Action04'))
        df = pd.concat([df3, df4])
    elif start_month == 4 & end_month == 4:
        df = pd.read_csv(config_util.get(section='Path', key='Action04'))

    df['time'] = df['time'].astype(np.datetime64)
    print df.describe
    # df = df.sort(['time'], ascending=1)
    # if has multi condition, then the () is needed.
    df = df[(df['time'] > start_time) & (df['time'] < end_time)]
    print df.describe

    grouped = df.groupby(['user_id', 'sku_id'])
    # can't use user_id directly again after group by

    for (key1, key2), group in grouped:
        print key1
        print key2
        for element in group[group['user_id'] == user_id]:
            print element


fetch_user_actions(27630, '2016-02-01', '2016-02-25')
