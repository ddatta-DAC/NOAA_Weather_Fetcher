import src.mongoDBI as mongoDBI
import os
import src.constants as constants
import src.utils as utils
import glob
import src.parse_grb_files as parse_grb_files
from multiprocessing import Process
from datetime import datetime, timedelta, date


# ---------------------------------------------------------------#


class buffer:
	buffer = None
	max_buffer_count = 25
	dbi = None

	def __init__(self, db_name):
		self.dbi = mongoDBI.mongoDBI(db_name)
		self.buffer = { }
		for t in constants.mongo_db_tables:
			self.buffer[t] = []

	def insert_to_buffer(self, table, dict):
		self.buffer[table].append(dict)
		return;

	def write_buffer(self):

		cur_len = len(self.buffer[constants.mongo_db_tables[0]])

		if cur_len < self.max_buffer_count:
			return;

		self.dbi.insert_bulk(self.buffer)
		# Empty buffer
		for t in constants.mongo_db_tables:
			self.buffer[t] = []
		return;

	# data_map : dictionary
	# 1st entry is the date or year_week id
	def insert_to_db(self, data_map, label=constants.label_date):

		if data_map is None or len(data_map) == 0:
			return;

		id = data_map[label]
		data_map.pop(label, None)

		for table, data in data_map.iteritems():
			# print 'in insert to db... table:'+table
			key_label = label
			key_contents = id
			value_label = constants.label_value
			value_contents = data
			dict = mongoDBI.mongoDBI.get_insert_dict(key_label, key_contents, value_label, value_contents)
			self.insert_to_buffer(table, dict)

		self.write_buffer()
		return;

	def flush(self):
		self.dbi.insert_bulk(self.buffer)
		# Empty buffer
		for t in constants.mongo_db_tables:
			self.buffer[t] = []
		return;


# ------------------------ END OF CLASS ------------------------#

# ---------------------------------------------------------------#
# AUXILLARY FUNCTIONS
# ---------------------------------------------------------------#


#
# Input : year
# Function : 
# Inserts into db, data for each date,  daily mean
#
def process_year_by_date_mean(year):
	# print 'Processing year : ' + str(year)
	cur_day = utils.get_current_day()
	cur_year = utils.get_current_year()

	buffer_obj = buffer(constants.db_name_date_mean)
	dir = str(year)

	if not os.path.exists(dir):
		return;

	os.chdir(dir)
	start_day = constants.gdas_min_day
	if year == cur_year:
		end_day = cur_day
	else:
		end_day = constants.gdas_max_day

	# Process the files for a single day
	for day in range(start_day, end_day + 1):
		dir = str(day).zfill(3)

		if not os.path.exists(dir):
			continue;
		try:
			os.chdir(dir)
			files = glob.glob("gdas*z")
			data = parse_grb_files.parse_files_by_date_mean(files)
			buffer_obj.insert_to_db(data, label=constants.label_date)
			os.chdir("../")
		except:
			pass
			os.chdir("../")
	os.chdir("../")
	buffer_obj.flush()


#
# Process complete data (4 per day) of each date, of the given year
#
def process_year_by_date_complete(year):
	cur_day = utils.get_current_day()
	cur_year = utils.get_current_year()

	buffer_obj = buffer(constants.db_name_date_complete)
	dir = str(year)

	if not os.path.exists(dir):
		return;

	os.chdir(dir)
	start_day = constants.gdas_min_day
	if year == cur_year:
		end_day = cur_day
	else:
		end_day = constants.gdas_max_day

	# Process the files for a single day
	for day in range(start_day, end_day + 1):
		dir = str(day).zfill(3)
		if not os.path.exists(dir):
			continue;
		try:
			os.chdir(dir)
			files = glob.glob("gdas*z")  # get list of data
			data = parse_grb_files.parse_files_by_date_complete(files)

			for data_element in data:
				buffer_obj.insert_to_db(data_element, label=constants.label_date_idx)

			os.chdir("../")
		except:
			pass

	os.chdir("../")
	os.chdir("../")
	buffer_obj.flush()


# ------------------------------------------------------------------#

#
# load data by mean of a day
# all years
#

def load_all_years_by_date_mean( ):
	cur_year = utils.get_current_year()
	os.chdir(constants.data_dir)
	os.chdir(constants.gdas_data_dir)
	years = range(constants.gdas_start_year, cur_year + 1)
	process_pool = []

	for year in years:
		p = Process(target=process_year_by_date_mean, args=(year,))
		process_pool.append(p);

	for p in process_pool:
		p.start()

	for p in process_pool:
		p.join()
	return;


# ------------------------------------------------------------------#

#
# load into db by a single date
# date string format must be : yyyy-mm-dd
#

def load_by_one_date(date_str):
	os.chdir(constants.data_dir)
	os.chdir(constants.gdas_data_dir)
	format = '%Y-%m-%d'
	tmp = datetime.strptime(date_str, format)
	today = datetime.now()
	# check if date is in future, if so return

	if today < tmp:
		print
		'Date given is in future!!!'
		return;

	buffer_obj = buffer()
	day = tmp.timetuple().tm_yday
	year = tmp.year
	dir = str(year)
	os.chdir(dir)
	dir = str(day).zfill(3)
	os.chdir(dir)
	files = glob.glob("gdas*z")

	data = parse_grb_files.parse_files(files)
	buffer_obj.insert_to_db(data, label=constants.label_date)
	buffer_obj.flush()
	os.chdir("../")
	os.chdir("../")
	return


# ---------------------------------------------------------------#

#
# Load into db aggregate data of 7 days
# start_date : Gregorian date ,start. Type : Datetime Object
# end_date : Gregorian date ,end . Type : Datetime Object
# week_number : ISO week number , refer ISO calendar. Type : int
# year : ISO year , refer ISO calendar. Type : int
# DB : Weather_week_mean
#

def load_by_week_mean(start_date, iso_year, iso_week):
	today = datetime.now()
	format = '%Y-%m-%d'
	start_date = datetime.strptime(start_date, format)

	# check if date is in future, if so return
	if today < start_date:
		# DEBUG
		# print 'Date given is in future!!!'
		return;

	file_path_dict = { }

	for weekday in range(0, 7, 1):
		cur = start_date + timedelta(days=weekday)

		if cur > today:
			break;

		day = cur.timetuple().tm_yday
		year = cur.year
		yr_dir = str(year)
		try:
			os.chdir(yr_dir)
			day_dir = str(day).zfill(3)
			try:
				os.chdir(day_dir)
				files = glob.glob("gdas*z")
				path = yr_dir + '/' + day_dir + '/.'
				file_path_dict[path] = files
				os.chdir("../")

			except:
				pass

			os.chdir("../")

		except:
			pass

	buffer_obj = buffer(constants.db_name_week_mean)
	data = parse_grb_files.parse_files_by_week(file_path_dict, iso_year=iso_year, iso_week=iso_week)
	buffer_obj.insert_to_db(data, label=constants.year_week_label)
	buffer_obj.flush()
	return;


# ---------------------------------------------------------------#

#
# Load complete data for each date into db
#
def load_by_date_complete( ):
	cur_year = utils.get_current_year()
	os.chdir(constants.data_dir)
	os.chdir(constants.gdas_data_dir)
	years = range(constants.gdas_start_year, cur_year + 1)
	process_pool = []

	for year in years:
		p = Process(target=process_year_by_date_complete, args=(year,))
		process_pool.append(p)

	for p in process_pool:
		p.start()

	for p in process_pool:
		p.join()

	return;
