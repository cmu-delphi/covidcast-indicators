import sys
import re
import pandas as pd
from pathlib import Path
from itertools import product
from datetime import date, datetime, timedelta
from datafetcher import *

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
                             dtype={'geo_id': str, 
                                    'val': float, 
                                    'se': float, 
                                    'sample_size': float, 
                                    'effective_sample_size': float
                            })

    print(df_to_test.head())
    print(df_to_test.describe())
    
    result = df_to_test.dtypes
    print(result)

    sys.exit()

    #validate_daily(df_to_test, nameformat, generation_date, max_check_lookbehind, sanity_check_rows_per_day, sanity_check_value_diffs, check_vs_working)
    print(date.today())

def check_bad_geo_id(df_to_test, geo_type):
    if geo_type not in negated_regex_dict:
        print("Unrecognized geo type:", geo_type )
        sys.exit()
    
    def find_all_unexpected_geo_ids(df_to_test, negated_regex):
        unexpected_geos = [ugeo[0] for ugeo in df_to_test['geo_id'].str.findall(negated_regex) if len(ugeo) > 0]
        if(len(unexpected_geos) > 0):
            print("Non-conforming geo_ids exist!")
            print(unexpected_geos)
            sys.exit()
    
    negated_regex_dict = {
    'county': '^(?!\d{5}).*$',
    'hrr': '^(?!\d{1,3}).*$',
    'msa': '^(?!\d{5}).*$',
    'state': '^(?![A-Z]{2}).*$',
    'national': '(?!usa).*$'
    }

    find_all_unexpected_geo_ids(df_to_test, negated_regex_dict[geo_type])

def check_missing_dates(daily_filenames, sdate, edate):
    number_of_dates = edate - sdate + timedelta(days=1)
    date_seq = {sdate + timedelta(days=x) for x in range(number_of_dates.days)}
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
    
    return

def check_bad_val(df_to_test):
    if (not df_to_test[(df_to_test['val'] > 100)].empty):
        print("val column can't have any cell greater than 100")
        sys.exit()
    if (df_to_test.isnull().values.any()):
        print("val column can't have any cell set to null")
        sys.exit()
    if (not df_to_test[(df_to_test['val'] < 0)].empty):
        print("val column can't have any cell smaller than 0")
        sys.exit()

def check_bad_se(df):
    if (df['se'].isnull().values.any()):
        print("se must not be NA")
        sys.exit()
    
    df.eval('se_upper_limit = (val * effective_sample_size + 50)/(effective_sample_size + 1)', inplace=True)

    df['se']= df['se'].round(3)
    df['se_upper_limit'] = df['se_upper_limit'].round(3)

    result = df.query('~((se > 0) & (se < 50) & (se <= se_upper_limit))')

    if not result.empty:
        print("se must be in (0,min(50,val*(1+eps))]")
        sys.exit()

def check_bad_sample_size(df):
    if(df['sample_size'].isnull.values.any() | df['effective_sample_size'].isnull.values.any()):
        print("sample size can't be NA")
        sys.exit()
    
    qresult = df.query('(sample_size < 100) | (effective_sample_size < 100)')

    if not qresult.empty:
        print("sample size must be >= 100")
        sys.exit()


def check_min_allowed_max_date(generation_date, max_date, max_weighted_date):
    if (max_weighted_date < generation_date - timedelta(days=4)
        or max_date < generation_date - timedelta(days=1)):
        sys.exit("latest date of generated file seems too long ago")
    return


def fbsurvey_validation(daily_filenames, sdate, edate):

    meta = covidcast.metadata()
    fb_meta = meta[meta['data_source']==DATA_SOURCE]
    unique_signals = fb_meta['signal'].unique().tolist()
    unique_geotypes = fb_meta['geo_type'].unique().tolist()

    ##### Currently metadata returns --*community*-- signals that don't get generated 
    ##### in the new fb-pipeline. Seiving them out for now.
    # Todo - Include weighted whh_cmnty_cli and wnohh_cmnty_cli
    for sig in unique_signals:
        if "community" in sig:
            unique_signals.remove(sig)
    

    geo_sig_cmbo = list(product(unique_geotypes, unique_signals))
    print(geo_sig_cmbo)
    print("Number of mixed types:", len(geo_sig_cmbo))

    for cmb in geo_sig_cmbo:
        print(cmb)

    ## The following dates refer to the newly generated files from latest pipeline execution
    ######----start_date-----#######
    ######----end_date------#######
    #start_date = date(2020, 6, 16)
    #end_date = date(2020, 6, 19)
    delta_days =  (edate + timedelta(days=1)) - sdate
    print("Number of days: ", delta_days.days)
    date_list = [sdate + timedelta(days=x) for x in range(delta_days.days)]
    print(date_list)
    date_slist = [dt.strftime("%Y%m%d") for dt in date_list]
    print(date_slist)

    data_folder = Path("data/")

    filenames = read_relevant_date_filenames(data_folder, date_slist)

    # Multi-indexed dataframe for a given (signal, geo_type)

    kroc = 0
    for recent_df in read_geo_sig_cmbo_files(geo_sig_cmbo, data_folder, filenames, date_slist):
        print(recent_df)
        kroc += 1
        if kroc == 2:
            break
    sys.exit()



    
    
    
    
    check_missing_dates(daily_filenames, sdate, edate)

    # Examples:
    # raw_cli
    # raw_ili
    # raw_wcli
    # raw_wili
    # raw_hh_cmnty_cli
    # raw_nohh_cmnty_cli
    i = 1
    filename_regex = re.compile(r'^(\d{8})_([a-z]+)_(raw\S*|smoothed\S*)[_?](w?)([ci]li).csv$')
    for f in daily_filenames:
        # example: 20200624_county_smoothed_nohh_cmnty_cli
        
        df_to_test = pd.read_csv(
                             data_folder / f, 
                             dtype={'geo_id': str, 'val': float, 'se': float, 'sample_size': float, 'effective_sample_size': float
                            })


        m = filename_regex.match(f)
        survey_date = datetime.strptime(m.group(1), '%Y%m%d').date()
        geo_type = m.group(2)

        if m.group(4):
            signal = "".join([m.group(4), m.group(5)])
            signal = "_".join([m.group(3), signal])
            max_weighted_date = survey_date
        else:
            signal = "_".join([m.group(3), m.group(5)])
            max_date = survey_date

        if (not m.group(0)):
            sys.exit('=nameformat= not recognized as a daily format') 
        
        try:
            df_ref = fetch_daily_data(DATA_SOURCE, survey_date, geo_type, signal)
        except APIDataFetchError as e:
            print("APIDataFetchError:", e)
            print("\n")


        print(df_to_validate)

        i += 1
        if i == 2:
            break

    check_min_allowed_max_date(generation_date, max_date, max_weighted_date)
    check_bad_geo_id(df_to_test, geo_type)
    check_bad_val(df_to_test)
    check_bad_se(df_to_test)
    check_bad_sample_size(df_to_test)

