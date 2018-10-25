import os
import src.mongoDBI as mongoDBI
import src.constants as constants
import src.utils as utils
import glob
import src.parse_grb_files as parse_grb_files
from multiprocessing import Process
from datetime import datetime, timedelta, date

# ---------------------------------------- #

class buffer:
    buffer = None
    max_buffer_count = 25
    dbi = None

    def __init__(self  , db_name ):
        self.dbi = mongoDBI.mongoDBI ( db_name )
        self.buffer = {}
        for t in constants.mongo_db_tables:
            self.buffer[ t ] = [ ]

    def insert_to_buffer(self, table, dict):
        self.buffer[ table ].append (dict)
        return

    def write_buffer(self):

        cur_len = len (self.buffer[ constants.mongo_db_tables[ 0 ] ])

        if cur_len < self.max_buffer_count:
            return;

        self.dbi.insert_bulk (self.buffer)
        # Empty buffer
        for t in constants.mongo_db_tables:
            self.buffer[ t ] = [ ]
        return

    # data_map : dictionary
    # 1st entry is the date or year_week id
    def insert_to_db(self, data_map, label=constants.label_date):

        if data_map is None or len(data_map) == 0 :
            return

        id = data_map[ label ]
        data_map.pop (label, None)

        for table, data in data_map.iteritems ():
            # print 'in insert to db... table:'+table
            key_label = label
            key_contents = id
            value_label = constants.label_value
            value_contents = data
            dict = mongoDBI.mongoDBI.get_insert_dict (key_label, key_contents, value_label, value_contents)
            self.insert_to_buffer (table, dict)

        self.write_buffer ()
        return;

    def flush(self):
        self.dbi.insert_bulk (self.buffer)
        # Empty buffer
        for t in constants.mongo_db_tables:
            self.buffer[ t ] = [ ]
        return


# ------------------------ END OF CLASS ------------------------#


#
# Load complete data for each date into db
# Process complete data (4 per day) of each date, of the given year
#
def process_year_by_date_complete(year):
    cur_day = utils.get_current_day ()
    cur_year = utils.get_current_year ()

    buffer_obj = buffer (constants.db_name_date_complete)
    dir = str (year)

    if not os.path.exists (dir):
        return

    os.chdir (dir)
    start_day = constants.gdas_min_day
    if year == cur_year:
        end_day = cur_day
    else:
        end_day = constants.gdas_max_day

    # Process the files for a single day
    for day in range (start_day, end_day + 1):
        dir = str (day).zfill (3)
        if not os.path.exists (dir):
            continue
        try:
            os.chdir (dir)
            files = glob.glob ("gdas*z")  # get list of data
            data = parse_grb_files.parse_files_by_date_complete (files)

            for data_element in data:
                buffer_obj.insert_to_db (data_element, label=constants.label_date_idx)

            os.chdir ("../")
        except:
            pass

    os.chdir ("../")
    os.chdir ("../")
    buffer_obj.flush ()


# -------------------------------------------- #

def load_by_date_complete():
    print ('Current Working directory ', os.getcwd())
    cur_year = utils.get_current_year ()
    os.chdir (constants.data_dir)
    os.chdir (constants.gdas_data_dir)
    years = range (constants.gdas_start_year, cur_year + 1)
    process_pool = []

    for year in years:
        p = Process (target=process_year_by_date_complete, args=(year,))
        process_pool.append (p)

    for p in process_pool:
        p.start ()

    for p in process_pool:
        p.join ()

    return

load_by_date_complete()