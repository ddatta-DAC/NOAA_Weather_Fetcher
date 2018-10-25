import src.constants
import numpy as np
from datetime import timedelta, date, datetime
import src.mongoDBI
import src.utils
import src.aggregate_buffer as aggregate_buffer
from multiprocessing import Process
import src.week_util  as week_util
import math
import src.constants as constants
import src.mongoDBI as mongoDBI
import src.utils as utils

# --------------------------------------------------#
'''

Original ingested data stored in Weather_date_complete
Process data by week

Tables should be feature name , database name is the statistic.
Feature names :
                constants.mongo_db_tables
                
Some stats should be across the week,without distinction by day.
Some should be taken across day,then week

Valid stats should be stored in :
                constants.stats_db
                
'''


# --------------------------------------------------#

# Assume that mongoDB has data by Date
# return for key in db string
def get_key(year, week):
    return str (year) + '_' + str (week);


def get_year_week_key(year,week):
    return str (year) + '_' + str (week)


# return date in string format
def get_date_key(datetime_obj):
    return datetime_obj.strftime (constants.format)


# --------------------------------------------------------#

def get_valid_stats():
    return constants.stats_db.keys ()


def get_stat_db(stat):
    return constants.stats_db[ stat ]


# ---------------------------------------------------------#
def get_week_array_list(start, end, feature, dbi_search):
    arr_list = [ ]
    cur = start
    key_label = constants.label_date_idx
    value_label = constants.label_value

    while cur <= end:
        date_str = get_date_key (cur)
        idx = 1
        while True:
            key_contents = date_str + '_' + str (idx)
            np_arr = dbi_search.find (feature, key_label, key_contents, value_label)
            if np_arr is None:
                break;
            arr_list.append (np_arr)
            idx += 1
        cur = cur + timedelta (days=1)

    np_arr = np.array (arr_list)
    return np_arr


#
# Return list of 7 elements , one element should be an array of arrays of one day's data
# A days data is numpy array , with the shape of 0th axis being number of records per day
#
def get_day_week_array_list(start, end, feature, dbi_search):
    week_list = [ ]
    cur = start

    key_label = constants.label_date_idx
    value_label = constants.label_value

    while cur <= end:
        date_str = get_date_key (cur)
        arr_day = [ ]
        idx =  1

        while True:
            key_contents = date_str + '_' + str (idx)
            np_arr = dbi_search.find (feature, key_label, key_contents, value_label)
            if np_arr is None:
                break;
            arr_day.append (np_arr)
            idx += 1

        np_arr = np.array (arr_day)
        week_list.append (np_arr)
        cur = cur + timedelta (days=1)

    return week_list;


#
# start, end : datetime objects
# feature : table_name e.g. temp
#
def process_week_median(start, end, feature, dbi_search):
    np_arr = get_week_array_list (start, end, feature, dbi_search)
    try:
        return np.median (np_arr, axis=0)
    except :
        print ('[ERROR] Statistic calculation error' , start, end , feature)
        return None



#
# start, end : datetime objects
# feature : table_name e.g. temp
#
def process_week_min(start, end, feature, dbi_search):
    np_arr = get_week_array_list (start, end, feature, dbi_search)
    try:
        return np.min (np_arr, axis=0)
    except :
        print ('[ERROR] Statistic calculation error', start, end , feature)
        return None


#
# start, end : datetime objects
# feature : table_name e.g. temp
#
def process_week_max(start, end, feature, dbi_search):
    np_arr = get_week_array_list (start, end, feature, dbi_search)
    return np.max (np_arr, axis=0)


def process_week_variance(start, end, feature, dbi_search):
    np_arr = get_week_array_list (start, end, feature, dbi_search)
    try:
        return np.var (np_arr, axis=0)
    except :
        print ('[ERROR] Statistic calculation error', start, end , feature)
        return None



#
# start, end : datetime objects
# feature : table_name e.g. temp
#
def process_week_max_daily_variation(start, end, feature, dbi_search):
    arr = get_day_week_array_list (start, end, feature, dbi_search)
    res = [ ]
    for day_element in arr:
        # day_element is a numpy array
        res.append (np.ptp (day_element, axis=0))

    np_arr = np.array (res)
    try:
        return np.max (np_arr, axis=0)
    except :
        print ('[ERROR] Statistic calculation error', start, end , feature )
        return None



def process_week_min_daily_mean(start, end, feature, dbi_search):
    arr = get_day_week_array_list (start, end, feature, dbi_search)  
    res = [ ]
    for day_element in arr:
        # day_element is a numpy array
        res.append (np.mean (day_element, axis=0))
    
    np_arr = np.array (res)
	
    try:
        return np.min (np_arr, axis=0)
    except :
        print ('[ERROR] Statistic calculation error' , start, end , feature)
        return None



# -------------------------------------------------------#

#
# Process data for 1 week
# start, end : datetime objects
# dbi_search : mongoDBI object
# statistic : median ,etc.
# feature : temp,rel_hum, etc
#
def process_week(start, end, dbi_search, feature, statistic):
    print (statistic)

    if statistic == 'week_max_daily_variation':
        return process_week_max_daily_variation (start, end, feature, dbi_search);
    if statistic == 'week_median':
        return process_week_median (start, end, feature, dbi_search)
    if statistic == 'week_max':
        return process_week_max(start, end, feature, dbi_search)
    if statistic == 'week_variance':
        return process_week_variance (start, end, feature, dbi_search)
    if statistic == 'week_min_daily_mean':
        return process_week_min_daily_mean (start, end, feature, dbi_search)
    if statistic == 'week_min':
        return process_week_min (start, end, feature, dbi_search)

    return ;


# --------------------------------------------------------- #

#
# Input: year (Regular year)
#
def process_year(year, feature=None, statistic=None):
    if feature is None or statistic is None:
        return;
    print ('Feature ', feature, 'Statistic', statistic)
    dbi_search = mongoDBI.mongoDBI (db_name=constants.db_name_date_complete)
    print (' Database :', get_stat_db (statistic))
    buffer_obj = aggregate_buffer.aggregate_buffer (db_name=get_stat_db (statistic), feature=feature)
    num_weeks = week_util.num_weeks_in_year(year)

    for week in range (1, num_weeks + 1):
        cur_wk_start , cur_wk_end = week_util.get_year_week_start_end(year,week,False)

        # process the entire_week
        result = process_week (cur_wk_start, cur_wk_end, dbi_search, feature, statistic)
        key = get_year_week_key (year,week)
        data = {}
        data[ constants.label_year_week ] = key
        data[ constants.label_value ] = result
        buffer_obj.insert_to_db (data, label=constants.label_year_week)

    buffer_obj.flush ()
    return


# ------------------------------------------------------------- #
#
# ENTRY POINT
#

def execute_main(year = None):
    cur_year = utils.get_current_year ()
    if year is None :
        years = range (constants.gdas_start_year+1, cur_year + 1)
    else :
	years = [year] 
    statistic = constants.stats_db.keys ()
    tables = constants.features
    
    process_pool = [ ]
   
    for stat in statistic:
        for feature in  tables :
            for year in years :
                p = Process (target=process_year, args=(year, feature ,stat,))
                process_pool.append (p)
                

    for p in process_pool:
         p.start ()
    
    for p in process_pool:
         p.join ()


# ---------------------------------------------------------------- #
execute_main ()
