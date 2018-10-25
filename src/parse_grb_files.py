# To check out the data fields :
# wgrib - V <filename> | grep -i '<keyword>' -a5

import pygrib
import os
import datetime
import numpy as np;
import constants
import sys
from datetime import datetime

# CONSTANTS
# lat  90.000000 to -90.000000
# long 0.000000 to -1.000000
dim1 = 181;
dim2 = 360;


def get_dims():
    global dim1, dim2
    return dim1, dim2


def get_value(g_handle, parameter, level, typeOfLevelECMF):
    value = None
    if g_handle is None:
        return;
    for g in g_handle:
        # print g.parameterName
        # print g.level
        # print g.typeOfLevel
        if parameter in g.parameterName and g.level == level and g.typeOfLevel == typeOfLevelECMF:
            value = g.values;
            break;
    g_handle.rewind ()
    if value is None:
        pass
    return value


#
# The columns in db are :
# ['date','rel_hum','sp_hum','temp','v_wind' ,'h_wind']
# Look up constants.py
#
def get_data_fields(g_handle):
    date_obj = g_handle[ 1 ].analDate
    date = date_obj.strftime ('%Y-%m-%d')
    # Todo :  use date time for format

    typeOfLevelECMF = 'heightAboveGround'

    attribute_level = \
        {
            'rel_hum': 2,  # Relative humidity
            'temp': 2,  # Temperature
            'sp_hum': 2,  # Specific humidity
            'u_wind': 10,
            'v_wind': 10
        }

    attribute_keyword_map = \
        {
            'rel_hum': 'Relative humidity',  # Relative humidity
            'temp': 'Temperature',  # Temperature
            'sp_hum': 'Specific humidity',  # Specific humidity
            'u_wind': 'U-component of wind',
            'v_wind': 'V-component of wind'
        }

    data_fields = {}
    for k, param in attribute_keyword_map.iteritems ():
        data_fields[ k ] = get_value (g_handle, param, attribute_level[ k ], typeOfLevelECMF)

    return date, data_fields;


def parse_grb_file(file):
    # DEBUG :
    # print 'In ' + parse_grb_file.__name__ + 'File name :' + file
    g_handle = pygrib.open (file)
    date, data = get_data_fields (g_handle);
    return date, data;


# Input list of date strings ; format yyyy-mm-dd
# Return : smallest date
def get_cur_date(dates):
    cur = None
    format = '%Y-%m-%d'
    for dt_str in dates:
        tmp = datetime.strptime (dt_str, format)
        if cur is None:
            cur = tmp
        elif tmp <= cur:
            cur = tmp;
    return cur.strftime (format)


# ----------------------------------------------------#
#
# input : list of data dictionary with fields same as columns/labels mentioned above
# [ {'temp' :<data> , ...} , ... ]
# returns data for a single date
# Averages values for a single date ( from usually 4 records )
#
def process_collection_by_date_mean(date, data):
    # DEBUG :
    # print 'In ' + process_collection.__name__
    if data is None or len (data) == 0:
        return None;

    keys = data[ 0 ].keys ()

    data_map = {}
    data_map[ constants.label_date ] = date
    dim1, dim2 = get_dims ()
    for key in keys:
        count = 0
        tmp = np.zeros ([ dim1, dim2 ]);
        for d in data:
            if d[ key ] is not None:
                tmp = tmp + d[ key ]
                count += 1
        tmp /= count
        data_map[ key ] = tmp
    return data_map


# input : list of data dictionary with fields same as columns/labels mentioned above
# returns data for a single ISO week
def process_collection_week(collection, iso_year, iso_week):
    if collection is None or len (collection) == 0:
        return None;

    keys = collection[ 0 ].keys ()

    data_map = {}
    data_map[ constants.year_week_label ] = str (iso_year) + '_' + str (iso_week)
    dim1, dim2 = get_dims ()
    for key in keys:
        count = 0
        tmp = np.zeros ([ dim1, dim2 ]);
        for d in collection:
            if d[ key ] is not None:
                tmp = tmp + d[ key ]
                count += 1
        tmp /= count
        data_map[ key ] = tmp
    return data_map


#
# date : string
# data : list of dictionaries, where keys are table names
# [ {temp:<data>,...},{ ... }, ... ]
# Return : List of dictionaries
# [ { 'date_idx': 'yyyy-mm-dd', 'temp': ... } , ... ]
#
def process_collection_by_date_complete(date, data):
    if data is None or len (data) == 0:
        return None;
    data_map_list = []

    keys = data[ 0 ].keys ()
    count = 1

    for element in data:
        data_map = {}
        id = date + '_' + str(count)
        count += 1
        data_map[ constants.label_date_idx ] = id
        for table_name,value in element.iteritems():
            data_map[table_name] = value
        data_map_list.append(data_map)

    return data_map_list


# ---------------------------------------------------------------#
# ---------------------------------------------------------------#


# ------------ Entry point methods ---------------#
#
# Calling method should have already changed directory to file location
# else pass path (absolute)
#
# -----------------------------------------------#


def parse_files_by_week(file_path_dict, iso_year, iso_week, path=None):
    cwd = None
    # if path is set, navigate to that location
    if path is not None:
        cwd = os.getcwd ()
        os.chdir (path);

    collection = [ ]

    for path, file_list in file_path_dict.iteritems ():
        # DEBUG
        # print file_list
        cwd_cur = os.getcwd ()
        try:
            os.chdir (path)
            for grb_file in file_list:
                # parse the single file and get the data
                date, data = parse_grb_file (grb_file);
                collection.append (data)

            os.chdir (cwd_cur)
        except:
            pass

    result = process_collection_week (collection, iso_year, iso_week);

    # return to original location if cwd has been set earlier
    if cwd is not None:
        os.chdir (cwd);
    return result;


# ---------------------------------------------------------------#

# Input : file list of a single date
# Returns : Avg value of features( such as temperature) for a day
def parse_files_by_date_mean(file_list, path=None):
    # DEBUG :
    # print 'In ' + parse_files.__name__
    cwd = None
    # if path is set, navigate to that location
    if path is not None:
        cwd = os.getcwd ()
        os.chdir (path);

    tmp_dates = [ ]
    collection = [ ]

    for grb_file in file_list:
        # parse the single file and get the data
        date, data = parse_grb_file (grb_file);
        tmp_dates.append (date)
        collection.append (data)

    # Select the smallest date
    # Since the each folder has 4 files, with date discrepancy
    date = get_cur_date (tmp_dates)
    # process the data from files
    result = process_collection_by_date_mean (date, collection);

    # return to original location if cwd has been set earlier
    if cwd is not None:
        os.chdir (cwd);
    return result;


# ---------------------------------------------------------------#

def parse_files_by_date_complete(file_list, path=None):
    cwd = None
    # if path is set, navigate to that location
    if path is not None:
        cwd = os.getcwd ()
        os.chdir (path);

    tmp_dates = [ ]
    collection = [ ]

    for grb_file in file_list:
        # parse the single file and get the data
        date, data = parse_grb_file (grb_file);
        tmp_dates.append (date)
        collection.append (data)

    # Select the smallest date
    # Since the each folder has 4 files, with date discrepancy
    date = get_cur_date (tmp_dates)
    # process the data from files
    result = process_collection_by_date_complete (date, collection);

    # return to original location if cwd has been set earlier
    if cwd is not None:
        os.chdir (cwd);
    return result;
