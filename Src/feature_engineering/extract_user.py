# coding=utf-8
import Src.conf
import pandas as pd

'''
file_p = '/Users/yaoguangzhong/比赛/京东/data/JData_Action_201602.csv'

with open(file_p,'r') as m_f:
    line = m_f.readline()
    line = m_f.readline()

    while line:
            # process your line
            print line
            # process end
            line = m_f.readline()
            row = line.split(',')
'''

df = pd.read_csv(Src.conf.action2_p)
print df.describe

def fetch_user_actions(user_id, start_time, end_time ):
    """Fetches rows from action log.

    Retrieves rows pertaining to the given user_id from the action data


    Args:
        user_id(int): user_id.
        keys: A sequence of strings representing the key of each table row
            to fetch.
        other_silly_variable: Another optional variable, that has a much
            longer name than the other args, and which does nothing.

    Returns:
        A dict mapping keys to the corresponding table row data
        fetched. Each row is represented as a tuple of strings. For
        example:

        {'Serak': ('Rigel VII', 'Preparer'),
         'Zim': ('Irk', 'Invader'),
         'Lrrr': ('Omicron Persei 8', 'Emperor')}

        If a key from the keys argument is missing from the dictionary,
        then that row was not found in the table.

    Raises:
        IOError: An error occurred accessing the bigtable.Table object.
    """
    pass