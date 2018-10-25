import argparse
import src.load_db as load_db

parser = argparse.ArgumentParser ()

parser.add_argument ('-date', type=str,
                     help='Input date in YYYY-MM-DD format')

parser.add_argument ('-date_complete', type=bool,
                     help='Process all data on disk, argument must be True')

parser.add_argument ('-week_all_mean', type=bool,
                     help='Process all data on disk,by Week [ Recommended ] , argument must be True')
parser.add_argument ('-date_all_mean', type=bool,
                     help='Process all data on disk,by Week [ Recommended ] , argument must be True')

parser.print_help ()
args = parser.parse_args ()
date = args.date
date_complete = args.date_complete
date_all_mean = args.date_all_mean
week_all_mean = args.week_all_mean

if date is not None:
    load_db.load_by_one_date (date)

elif date_complete is True:
    load_db.load_by_date_complete()

elif date_all_mean is True:
    load_db.load_all_years ()

else:
    parser.print_help ()
