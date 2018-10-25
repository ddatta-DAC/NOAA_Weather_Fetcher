##########################################################
#
#	Script to download the files from GDAS 
#	ftp://ladsweb.nascom.nasa.gov/allData/5/GDAS_0ZF/
#	Stores files in : RawData.
#	Directory structure of source maintained.
#	
#	Two functions for user :
#	1. initial_ingest
#	2. update [Todo]
#
###########################################################

import os
import ftplib
import src.constants as constants
import src.utils as utils
from multiprocessing import Process
from glob import glob

def setup_dir(data_dir):
    if not os.path.exists (data_dir):
        os.makedirs (data_dir, 0o777)
    return;


# Get ftp handle
# hard-coded user and password , suffices for this application

def get_ftp_handle():
    user = "anonymous"
    passwd = "anonymous"
    ftp_link = "ladsweb.nascom.nasa.gov"
    loc = "allData/6/GDAS_0ZF"
    print (ftp_link)
    ftp = ftplib.FTP (ftp_link)
    ftp.login (user, passwd)
    ftp.cwd (loc)
    return ftp


def get_current_year_day():
    year = utils.get_current_year ()
    day = utils.get_current_day ()
    return year, day


# If end_year is not specified download for a single year
def download_from_source(start_year, start_day, end_year=None):
    max_end_day = constants.gdas_max_day
    cur_year, cur_day = get_current_year_day ()
    ftp = get_ftp_handle ()

    if end_year is None:
        end_year = start_year

    for year in range (start_year, end_year + 1):
        dir = str (year)

        setup_dir (dir)
        os.chdir (dir)
        ftp.cwd (dir)

        if year == cur_year:
            end_day = cur_day
        else:
            end_day = max_end_day

        for day in range (start_day, end_day + 1):
            unable_to_cd = False
            dir = str (day).zfill (3)

            print (' Attempting : ' + str (year) + '/' + str (dir))
            try:
                ftp.cwd (dir)
                setup_dir (dir)
                os.chdir (dir)
                file_list = ftp.nlst ()
                print (file_list)
                if len (file_list) != 0:
                    # Download each file
                    for f in file_list:
                        ftp.retrbinary ('RETR ' + f, open (f, 'wb').write)

            except:
                unable_to_cd = True

            if not unable_to_cd:
                ftp.cwd ("../")
                os.chdir ("../")

        ftp.cwd ("../")
        os.chdir ("../");

    ftp.quit ()
    return


def initial_ingest(year=None):

    os.chdir (constants.data_dir)
    setup_dir (constants.gdas_data_dir)
    os.chdir (constants.gdas_data_dir)
    # download_from_source(constants.gdas_start_year,1)

    if year is None:
        years = [constants.gdas_start_year, 2018]
    else:
        years = [year]

    pool = []

    for year in years:
        p = Process (target=download_from_source, args=(year, 1))
        pool.append (p)

    for p in pool:
        p.start ()

    for p in pool:
        p.join ()

    return

# ------------------------------------ #

def update():
    prev_cwd = os.getcwd()
    os.chdir (constants.data_dir)
    os.chdir (constants.gdas_data_dir)
    _cwd  = os.getcwd()

    d = '.'
    yr_list = filter ( lambda x : os.path.isdir (os.path.join (d, x)), os.listdir (d))
    yr_list = [ int(x) for x in yr_list ]
    _last_yr = max(yr_list)

    os.chdir(str(_last_yr))
    _cwd = os.getcwd ()
    d = '.'
    day_list = filter (lambda x: os.path.isdir (os.path.join (d, x)), os.listdir (d))
    print (day_list)
    if len(day_list) == 0 :
        day_list.append('001')

    day_list = [int(x) for x in day_list]
    _last_day = max(day_list)

    cur_yr, cur_day = get_current_year_day()
    if cur_yr != _last_yr :
        _end_year = cur_yr
    else :
        _end_year = _last_yr

    download_from_source(_last_yr, _last_day, end_year= _end_year)

update()
