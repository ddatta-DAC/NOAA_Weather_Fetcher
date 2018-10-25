import pandas as pd
from datetime import datetime
from datetime import timedelta
import os
import sys

sys.path.append('./../..')
sys.path.append('./..')
import common_utils

format = "%Y-%m-%d"
file_location = './../aux_data'
week_info_file = 'year_week_data.csv'
season_year_week_file = 'season_year_week_data.csv'

df = None
date_week_year = {}
s_df = None
s_yw_index = None


# ---------------------- #
def init():
    global df, s_df
    global week_info_file
    global season_year_week_file

    old_location = os.getcwd()
    script_dir = os.path.dirname(__file__)
    os.chdir(script_dir)
    week_info_file_path = file_location + '/' + week_info_file
    df = pd.read_csv(week_info_file_path)
    setup_date_week_year_list()

    season_year_week_info_path = file_location + '/' + season_year_week_file
    s_df = pd.read_csv(season_year_week_info_path, index_col=0)
    s_df.reset_index()
    setup_s_year_week_mapping()
    os.chdir(old_location)

    return


def setup_date_week_year_list():
    global df,date_week_year

    for i, row in df.iterrows():
        week = row['week']
        year = row['year']
        cur = datetime.strptime(row['start'], format).date()

        for i in range(7):
            date_week_year[str(cur)] = [year, week]
            cur = cur + timedelta(days=1)
    return


def setup_s_year_week_mapping():
    global s_df
    s_df = s_df.set_index(['year', 'week'])
    return


# ----- Initialize ------ #
init()
# ---------------------- #


def num_weeks_in_year(year):
    return common_utils.num_weeks_in_year(year)


# Return a list of date objects
# Corresponding to all dates of a year - week
def get_year_week_dates(year, week, return_string=False):
    global df, format

    idx = df.loc[(df['year'] == year) & (df['week'] == week)].index[0]
    start = datetime.strptime(df.loc[idx, 'start'], format).date()
    dt_list = []

    for i in range(7):
        dt = start + timedelta(days=i)
        if return_string:
            dt_list.append(str(dt))
        else:
            dt_list.append(dt)

    return dt_list


# Input :
# int year
# int week
# Return : datetime obj or string
def get_year_week_start(year, week, return_string=False):
    global df, format
    idx = df.loc[(df['year'] == year) & (df['week'] == week)].index[0]
    start = datetime.strptime(df.loc[idx, 'start'], format).date()
    if return_string:
        return str(start)
    else:
        return start


# Input :
# int year
# int week
# Return : datetime obj or string
def get_year_week_start_end(year, week, return_string=False):
    global df, format
    idx = df.loc[(df['year'] == year) & (df['week'] == week)].index[0]
    start = datetime.strptime(df.loc[idx, 'start'], format).date()
    end = datetime.strptime(df.loc[idx, 'end'], format).date()

    if return_string:
        return str(start), str(end)
    else:
        return start, end


# Input : date obj string

def get_year_week_by_date(inp_dt):
    global date_week_year
    yr_wk = date_week_year[inp_dt]
    return yr_wk[0], yr_wk[1]


# --------------------------------------------- #
# Returns season year and season week.
# Input :
#   int year
#   int week
# ----------------------------------------------#
def get_s_year_week(year, week):
    res = s_df.loc[(year, week), :]
    s_week = res['s_week']
    s_year = res['s_year']
    return s_year, s_week
