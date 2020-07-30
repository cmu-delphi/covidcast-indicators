from os import listdir, stat
from os.path import isfile, join 
import platform
import covidcast
import pandas as pd
from datetime import date, datetime, timedelta
from errors import *


def read_filenames(path):
    daily_filenames = [f for f in listdir(path) if isfile(join(path, f))]
    return daily_filenames

def read_relevant_date_filenames(data_path, date_slist):
    all_files = listdir(data_path)
    filenames = list()

    for fl in all_files:
        for dt in date_slist:
            if fl.find(dt) != -1:
                filenames.append(fl)
    return filenames    

def read_geo_sig_cmbo_files(geo_sig_cmbo, data_folder, filenames, date_slist):
    for geo_sig in geo_sig_cmbo:
        df_list = list()

        files = list(filter(lambda x: geo_sig[0] in x and geo_sig[1] in x, filenames))
        if(len(files) == 0):
            print("FILE_NOT_FOUND: File with geo_type:", geo_sig[0], " and signal:", geo_sig[1], " does not exist!")
        for f in files:
            df = pd.read_csv(
                            data_folder / f, 
                            dtype={'geo_id': str, 
                                    'val': float,
                                    'se': float,
                                    'sample_size': float,
                                    'effective_sample_size': float
                            })
            for dt in date_slist:
                if f.find(dt) != -1:
                    gen_dt = datetime.strptime(dt, '%Y%m%d')
                    df['time_value'] = gen_dt
            df_list.append(df)   
        yield pd.concat(df_list), geo_sig[0], geo_sig[1]

def fetch_daily_data(data_source, survey_date, geo_type, signal):
    data_to_validate = covidcast.signal(data_source, signal, survey_date, survey_date, geo_type)
    if not isinstance(data_to_validate, pd.DataFrame):
        custom_msg = "Error fetching data on" + str(survey_date)+ \
                     "for data source:" + data_source + \
                     ", signal-type:"+ signal + \
                     ", geography-type:" + geo_type
        raise APIDataFetchError(custom_msg)
    return data_to_validate
    

def new_stuff():
    survey_sdate = "2020-06-13"
    survey_edate = "2020-06-20"
    dtobj_sdate = datetime.strptime(survey_sdate, '%Y-%m-%d')
    dtobj_edate = datetime.strptime(survey_edate, '%Y-%m-%d')
    print(dtobj_sdate.date())
    print(dtobj_edate.date())

    number_of_dates = dtobj_edate - dtobj_sdate + timedelta(days=1)
    print(number_of_dates)

    date_seq = {dtobj_sdate + timedelta(days=x) for x in range(number_of_dates.days + 1)}
    print(date_seq)

    # 1) Lets first fetch all daily filenames


    data = covidcast.signal("fb-survey", "raw_ili", date(2020, 6, 19), date(2020, 6, 19),
                            "state")


    unique_dates = set()
    unique_dates_obj = set()

    for daily_filename in daily_filenames:
        unique_dates.add(daily_filename[0:8])

    for unique_date in unique_dates:
        newdate_obj = datetime.strptime(unique_date, '%Y%m%d')
        unique_dates_obj.add(newdate_obj)

    check_dateholes = date_seq.difference(unique_dates_obj)
    if check_dateholes:
        print("Date holes exist!")
        print(check_dateholes)
  





#print(data)
#print(data.dtypes)

#print(type(data))

#meta = covidcast.metadata()
#meta.to_csv('meta_out.csv')
#print(meta)