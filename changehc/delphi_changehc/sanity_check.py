# standard packages
import os
from datetime import datetime, timedelta

#  third party
from delphi_utils import read_params
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# first party
from delphi_changehc.config import Config
from delphi_changehc.download_ftp_files import download_covid, download_cli
from delphi_changehc.load_data import load_chng_data

# Function copied and pasted from run.py
def retrieve_files(params, filedate):
    """Return filenames of relevant files, downloading them if necessary."""

    ## download recent files from FTP server
    download_covid(params["cache_dir"], params["ftp_conn"])
    download_cli(params["cache_dir"], params["ftp_conn"])

    denom_file = "%s/%s_All_Outpatients_By_County.dat.gz" % (params["cache_dir"],filedate)
    covid_file = "%s/%s_Covid_Outpatients_By_County.dat.gz" % (params["cache_dir"],filedate)
    flu_file = "%s/%s_Flu_Patient_Count_By_County.dat.gz" % (params["cache_dir"],filedate)
    mixed_file = "%s/%s_Mixed_Patient_Count_By_County.dat.gz" % (params["cache_dir"],filedate)
    flu_like_file = "%s/%s_Flu_Like_Patient_Count_By_County.dat.gz" % (params["cache_dir"],filedate)
    covid_like_file = "%s/%s_Covid_Like_Patient_Count_By_County.dat.gz" % (params["cache_dir"],filedate)

    file_dict = {"denom": denom_file}
    file_dict["covid"] = covid_file
    file_dict["flu"] = flu_file
    file_dict["mixed"] = mixed_file
    file_dict["flu_like"] = flu_like_file
    file_dict["covid_like"] = covid_like_file
    return file_dict

params = read_params()

dropdate_dt = (datetime.now() - timedelta(days=1,hours=16))
dropdate_dt = dropdate_dt.replace(hour=0,minute=0,second=0,microsecond=0)
filedate = dropdate_dt.strftime("%Y%m%d")

file_dict = retrieve_files(params, filedate)

# See where an individual column is higher than denominators
denom_data = load_chng_data(file_dict["denom"], filedate, "fips",
                     Config.DENOM_COLS, Config.DENOM_DTYPES, Config.DENOM_COL)
covid_data = load_chng_data(file_dict["covid"], filedate, "fips",
                     Config.COVID_COLS, Config.COVID_DTYPES, Config.COVID_COL)
flu_data = load_chng_data(file_dict["flu"], filedate, "fips",
                 Config.FLU_COLS, Config.FLU_DTYPES, Config.FLU_COL)
mixed_data = load_chng_data(file_dict["mixed"], filedate, "fips",
                 Config.MIXED_COLS, Config.MIXED_DTYPES, Config.MIXED_COL)
flu_like_data = load_chng_data(file_dict["flu_like"], filedate, "fips",
                 Config.FLU_LIKE_COLS, Config.FLU_LIKE_DTYPES, Config.FLU_LIKE_COL)
covid_like_data = load_chng_data(file_dict["covid_like"], filedate, "fips",
                 Config.COVID_LIKE_COLS, Config.COVID_LIKE_DTYPES, Config.COVID_LIKE_COL)

data = denom_data.merge(covid_data, how="outer", left_index=True, right_index=True)
data = data.merge(flu_data, how="outer", left_index=True, right_index=True)
data = data.merge(mixed_data, how="outer", left_index=True, right_index=True)
data = data.merge(flu_like_data, how="outer", left_index=True, right_index=True)
data = data.merge(covid_like_data, how="outer", left_index=True, right_index=True)
data.fillna(0, inplace=True)

if not os.path.exists("sanity_output"):
    os.makedirs("sanity_output")

for k in [Config.COVID_COL,Config.FLU_COL,Config.MIXED_COL,Config.FLU_LIKE_COL,Config.COVID_LIKE_COL]:
    mask = np.where(data[k] > data[Config.DENOM_COL])[0]
    if len(mask) > 0:
        print("%s column has %d rows greater than %s" %(k, len(mask), Config.DENOM_COL))
        data.iloc[mask].to_csv("sanity_output/%s_high_counts.csv"%(k))