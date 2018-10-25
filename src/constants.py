gdas_start_year = 2000
gdas_min_day = 1
gdas_max_day = 366
data_dir = "Data"
gdas_data_dir = "RawData"

db_name_date_complete = 'Weather_date_complete'

id_key = 'id'
label_key = 'parameter'
label_value = 'data'
label_date = 'date'
label_date_idx = 'date_idx'
mongo_db_tables = ['rel_hum','sp_hum','temp','v_wind' ,'u_wind']
label_year_week = 'year_week'
abs_hum_table = 'abs_hum'
dAH_table = 'd_abs_hum'
format = '%Y-%m-%d'
hist_avg_keyword = 'hist_avg'

features = ['abs_hum','rel_hum','sp_hum','temp','v_wind','u_wind']
stats_db = {
    'week_median' : 'week_median',  # Median value across entire week
    'week_max' : 'week_max',         # Max value across entire week
    'week_min' : 'week_min',         # Max value across entire week
    'week_variance' : 'week_variance', # Varinace  across week
    'week_max_daily_variation' : 'week_max_daily_variation', # Max daily variance across week
    'week_min_daily_mean' : 'week_min_daily_mean', # Min of medians of daily mean value
}
_53_wk_yrs = [1997, 2003, 2008, 2014]