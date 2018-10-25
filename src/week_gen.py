from datetime import date
from datetime import timedelta
import pandas as pd
import sys
sys.path.append('./../..')
import common_utils

_53_wk_yrs = common_utils._53_wk_yrs

# ------------------------------------------- #
def disp(d):
    for k, v in d.iteritems():
        print k
        for w, dts in v.iteritems():
            print w, ' : ', dts


# -------------------------------------------- #

def generate_week_info():

    # Set Reference point
    ref_start_season_yr = 2017
    ref_start_date_str = '2017-10-1'
    ref_start_date = [2017, 10, 1]
    ref_start_week = 40

    tmp = date(ref_start_date[0], ref_start_date[1], ref_start_date[2])
    season_week_map = {}

    # backfill

    season_week_map[ref_start_season_yr] = {}
    cur_yr = ref_start_season_yr
    cur_wk = ref_start_week
    cur_wk_start = ref_start_week


    while cur_yr > 1996:
        beg = tmp
        end = tmp + timedelta(days=6)

        z = [str(beg), str(end)]
        season_week_map[cur_yr][cur_wk] = z

        tmp = tmp - timedelta(days=7)
        y = tmp.year

        cur_wk = cur_wk - 1

        # check for 53 week year

        if cur_wk < 1 and (y < cur_yr and y in _53_wk_yrs):
            cur_wk = 53
            cur_yr = y

        elif cur_wk < 1 and (y == cur_yr and y - 1 in _53_wk_yrs):
            cur_wk = 53
            cur_yr = y - 1

        elif cur_wk < 1:
            cur_wk = 52
            if y == cur_yr:
                cur_yr -= 1
            else:
                cur_yr = y

        if cur_yr not in season_week_map.keys():
            season_week_map[cur_yr] = {}

    # disp( season_week_map)


    # forward fill
    cur_yr = ref_start_season_yr
    cur_wk = ref_start_week
    cur_wk_start = ref_start_week
    tmp = date(ref_start_date[0], ref_start_date[1], ref_start_date[2])

    while cur_yr < 2019:
        beg = tmp
        end = tmp + timedelta(days=6)

        z = [str(beg), str(end)]
        season_week_map[cur_yr][cur_wk] = z

        tmp = tmp + timedelta(days=7)
        y = tmp.year

        cur_wk = cur_wk + 1
        # check for 53 week year

        if cur_wk > 52 and cur_yr in _53_wk_yrs:

            cur_wk += 1
            cur_yr = y

        elif cur_wk > 52:
            cur_wk = 1
            if cur_yr == y:
                cur_yr += 1
            elif cur_yr < y:
                cur_yr = y

        if cur_yr not in season_week_map.keys():
            season_week_map[cur_yr] = {}

    columns = ['year', 'week', 'start', 'end']
    df = pd.DataFrame(columns=columns)

    for year, year_data in season_week_map.iteritems():
        if year_data is None:
            continue
        for week, data in year_data.iteritems():
            data_dict = {
                'year': [year],
                'week': [week],
                'start': [data[0]],
                'end': [data[1]]
            }
            tmp = pd.DataFrame(data_dict, index=None)
            df = df.append(tmp, ignore_index=True)

    df = df.sort_values(by=['year', 'week'], ascending=[1, 1])

    op_location = './../aux_data/'
    op_file = 'year_week_data.csv'
    output = op_location + op_file

    df.to_csv(output,index=False)

# ---------------------- #
# Generate the week,year to season-week,season-year map
# ---------------------- #

def gen_seasonYearWeek_info():
    # Start point : 1997, week 21 -> s_year = 1997 , s_week = 1
    s_year = 1997
    s_week = 1
    year = 1997
    week = 20

    # DataFrsme should have 4 columns
    # year week s_year s_week
    df = None

    while True:
        #  current tuple :
        # (year,week) -> (s_year,s_week)
        tmp_df = pd.DataFrame({
            'year': [year],
            'week': [week],
            's_year' : [s_year],
            's_week' : [s_week]
        })

        if df is None:
            df = tmp_df
        else:
            df =  df.append(tmp_df,ignore_index= True)

        s_week += 1
        week += 1

        if week > common_utils.num_weeks_in_year(year):
            year += 1
            week = 1
        if s_week == common_utils.SEASON_END +1:
            s_year += 1
            s_week = 1

        if s_year > 2017:
            break

    op_location = './../aux_data'
    op_file = 'season_year_week_data.csv'
    output = op_location + '/' + op_file
    df.to_csv(output)


gen_seasonYearWeek_info()
generate_week_info()