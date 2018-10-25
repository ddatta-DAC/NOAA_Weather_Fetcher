import datetime
import time
import sys
import constants

sys.path.append ("./../.")


def get_current_year():
    return int (time.strftime ("%Y"))


def get_current_day():
    today = datetime.datetime.now ()
    day = today.strftime ('%j')
    return int (day)


def get_dims():
    return 181, 360;


def get_num_weeks_in_year(year):
    if year in constants._53_wk_yrs:
        return 53
    else:
        return 52


# Input : region string [ nat , hhs1, hhs2 ...]
# output : Integers 0,1,2,...
def get_region_id(region_str):
    region_str = region_str.lower ()
    if region_str == 'nat':
        return 0;
    else:
        reg = region_str.lstrip ('hhs')
        return int (reg)


def get_region_str(region_id):
    if region_id == 0:
        return 'nat'
    else:
        return 'hhs' + str (region_id)


def get_regions_list():
    reg = []
    reg.append ("nat")
    for i in range (1, 10 + 1):
        reg.append ('hhs' + str (i))
    return reg
