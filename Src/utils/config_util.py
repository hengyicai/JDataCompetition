# coding=utf-8
"""Utilization function for config file"""
import ConfigParser

__config = ConfigParser.ConfigParser()
__config.read('../../config.ini')


def __config_section_map(section):
    section_dict = {}
    options = __config.options(section)
    for option in options:
        try:
            section_dict[option] = __config.get(section, option)
            if section_dict[option] == -1:
                print("skip: %s" % option)
        except:
            print("exception on %s!" % option)
            section_dict[option] = None
    return section_dict


def get(section, key):
    """Get value with respect to the config_key in config file.

    Args:
        section(str): Section name.
        key(str): Key in config file.

    Returns:
        str: Value of the given key in section, None if key not exists.

    """
    m_dict = __config_section_map(section)
    key = key.lower()
    if key in m_dict:
        return m_dict[key]
    else:
        return None
