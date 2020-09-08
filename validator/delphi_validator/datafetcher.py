from os import listdir, stat
from os.path import isfile, join 
import platform
import covidcast
import pandas as pd
from datetime import date, datetime, timedelta
from .errors import APIDataFetchError
import re
from typing import List
import json

filename_regex = re.compile(r'^(?P<date>\d{8})_(?P<geo_type>\w+?)_(?P<signal>\w+)\.csv$')


def get_filenames_with_geo_signal(path, data_source, date_slist: List[str]):
    meta = covidcast.metadata()
    source_meta = meta[meta['data_source']==data_source]
    unique_signals = source_meta['signal'].unique().tolist()
    unique_geotypes = source_meta['geo_type'].unique().tolist()

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

    filenames = read_relevant_date_filenames(data_folder, date_slist[0])
    return filenames, geo_sig_cmbo


def read_filenames(path):
    daily_filenames = [ (f, filename_regex.match(f)) for f in listdir(path) if isfile(join(path, f))]
    return daily_filenames

def read_relevant_date_filenames(data_path, date_slist):
    all_files = [f for f in listdir(path) if isfile(join(data_path, f))]
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

def load_csv(path):
    return pd.read_csv(
        path,
        dtype={
            'geo_id': str,
            'val': float,
            'se': float,
            'sample_size': float,
        })

def fetch_daily_data(data_source, survey_date, geo_type, signal):
    data_to_validate = covidcast.signal(data_source, signal, survey_date, survey_date, geo_type)
    if not isinstance(data_to_validate, pd.DataFrame):
        custom_msg = "Error fetching data on" + str(survey_date)+ \
                     "for data source:" + data_source + \
                     ", signal-type:"+ signal + \
                     ", geography-type:" + geo_type
        raise APIDataFetchError(custom_msg)
    return data_to_validate

