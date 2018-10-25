from datetime import date, timedelta, datetime
from multiprocessing import Process
import numpy as np
import constants
import mongoDBI
import AH_calc
import utils
import os


class buffer:
    buffer = None
    max_buffer_count = 250
    dbi = None
    table = constants.abs_hum_table

    def __init__(self):
        self.dbi = mongoDBI.mongoDBI (constants.db_name_date_complete)
        self.buffer = []

    def insert_to_buffer(self, dict):
        self.buffer.append (dict)
        return;

    def write_buffer(self, flush=False):
        cur_len = len (self.buffer)
        if cur_len < self.max_buffer_count and flush == False:
            return;

        self.dbi.insert_bulk ({self.table: self.buffer})
        # Empty buffer

        self.buffer = []
        return;

    # data is a dict { date_str : numpy array }
    # 1st entry is the date or year_week id
    def insert_to_db(self, data, label=constants.label_date_idx):

        if data is None or len (data) == 0:
            return;

        for date_idx, value in data.iteritems ():
            key_label = label
            key_contents = date_idx
            value_label = constants.label_value
            value_contents = value
            dict = mongoDBI.mongoDBI.get_insert_dict (key_label, key_contents, value_label, value_contents)
            self.insert_to_buffer (dict)
            self.write_buffer ()

        return;

    def flush(self):
        self.write_buffer (True)
        # Empty buffer
        self.buffer = []
        return;


# ------------------ END OF CLASS ------------------#

# return AH
# Input dbi object, datetime object
def calculate_ah(dbi, date):
    table = constants.abs_hum_table
    key_label = constants.label_date_idx
    date_str = date.strftime (constants.format)
    idx = 1
    ah_map = {}

    while True:

        key_contents = date_str + '_' + str (idx)
        temp_arr = dbi.find ('temp', key_label, key_contents, constants.label_value)
        rh_arr = dbi.find ('rel_hum', key_label, key_contents, constants.label_value)

        # idx count over 4
        if temp_arr is None and rh_arr is None or idx > 4:
            break;

        if temp_arr is None or rh_arr is None:
            continue;

        idx += 1
        ah_arr = np.zeros (utils.get_dims ())
	 
        for i in range (ah_arr.shape[0]):
            for j in range (ah_arr.shape[1]):
                ah_arr[i][j] = AH_calc.calc_ah (temp_arr[i][ j], rh_arr[i][ j])
		
        ah_map[key_contents] = ah_arr

    return ah_map


# ---------------------------------------------------#

def process_year(year):
    dbi = mongoDBI.mongoDBI (constants.db_name_date_complete)
    buffer_obj = buffer ()

    start_date = date (year, 1, 1)
    cur = start_date
    today = date.today ()
    cur_year = utils.get_current_year ()

    end_date = date (year, 12, 31)
    if year == cur_year:
        end_date = today

    while cur <= end_date:
        ah_map = calculate_ah (dbi, cur)
        for date_idx, data in ah_map.iteritems ():
            buffer_obj.insert_to_db ({date_idx: data})
        cur = cur + timedelta (days=1)
    buffer_obj.flush ()


def execute():
    cur_year = utils.get_current_year ()
    years = range (constants.gdas_start_year, cur_year + 1)
    process_pool = []

    for year in years:
        p = Process (target=process_year, args=(year,))
        process_pool.append (p);

    for p in process_pool:
        p.start ()

    for p in process_pool:
        p.join ()

execute ()
