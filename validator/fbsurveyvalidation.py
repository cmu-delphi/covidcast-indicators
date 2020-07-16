import sys
import re
import pandas as pd
from datetime import date

DATA_SOURCE = "fb-survey"

#def validate_daily(df_to_test, nameformat, covidcast_reference_dfs, generation_date, max_check_lookbehind, sanity_check_rows_per_day, sanity_check_value_diffs, check_vs_working):
def validate_daily(df_to_test, nameformat, generation_date = date.today(), max_check_lookbehind = 7, sanity_check_rows_per_day = True, sanity_check_value_diffs = True, check_vs_working = True):
    
    # Perform some automated format and sanity checks of =df.to.test=
    if(type(max_check_lookbehind) != int | len(str(max_check_look_behind) != 1)):
        sys.exit(" =max_check_lookbehind= must be length 1, integer type")

    if( not isinstance(generation_date, datetime.date) or generation_date > date.today()):
        sys.exit("=generation.date= must be a length 1 Date that is not in the future.")
    # example: 20200624_county_smoothed_nohh_cmnty_cli
    filename_regex = re.compile(r'^(\d{8})_([a-z]+)_(raw|smoothed)_(\w*)([ci]li).csv$')
    pattern_found = filename_regex.match(nameformat)
    if (not nameformat or not pattern_found):
        sys.exit('=nameformat= not recognized as a daily format')



def main():
    print("Inside main")
    df_to_test = pd.read_csv(
                            "data/20200613_county_raw_cli.csv", 
                             dtype={'geo_id': str, 'val': float, 'se': float, 'sample_size': float, 'effective_sample_size': float
                            })

    print(df_to_test.head())
    print(df_to_test.describe())
    
    result = df_to_test.dtypes
    print(result)

    sys.exit()

    #validate_daily(df_to_test, nameformat, generation_date, max_check_lookbehind, sanity_check_rows_per_day, sanity_check_value_diffs, check_vs_working)
    print(date.today())

def check_missing_dates(daily_filenames, sdate, edate):
    number_of_dates = edate - sdate + timedelta(days=1)
    #print(number_of_dates)

    date_seq = {sdate + timedelta(days=x) for x in range(number_of_dates.days + 1)}
    #print(date_seq)

    unique_dates = set()
    unique_dates_obj = set()

    for daily_filename in daily_filenames:
        unique_dates.add(daily_filename[0:8])

    for unique_date in unique_dates:
        newdate_obj = datetime.strptime(unique_date, '%Y%m%d')
        unique_dates_obj.add(newdate_obj)

    check_dateholes = date_seq.difference(unique_dates_obj)
    
    if check_dateholes:
        print("Missing dates are observed; if these dates are already in the API they would not be updated")
        print(check_dateholes)

def fbsurvey_validation(daily_filnames, sdate, edate):
    
    check_missing_dates(daily_filenames, sdate, edate)

    # Examples:
    # raw_cli
    # raw_ili
    # raw_wcli
    # raw_wili
    # raw_hh_cmnty_cli
    # raw_nohh_cmnty_cli
    filename_regex = re.compile(r'^(\d{8})_([a-z]+)_(raw|smoothed)_(\w*)([ci]li).csv$')
    for f in daily_filnames:
        # example: 20200624_county_smoothed_nohh_cmnty_cli
        m = filename_regex.match(f)
        survey_date = datetime.strptime(m.group(1), '%Y%m%d').date()
        geo_type = m.group(2)

        if m.group(4):
            signal = "_".join([m.group(3), m.group(4), m.group(5)])
        else:
            signal = "_".join([m.group(3), m.group(5)])

        if (not nameformat or not pattern_found):
            sys.exit('=nameformat= not recognized as a daily format') 
        
        df_to_validate = fetch_daily_data(DATA_SOURCE, survey_date, geo_type, signal)
