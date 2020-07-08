## Needed to process the geo files to get from xls file to a simpler csv.
## pip install xlrd

## Author: James Sharpnack @jsharpna


import pandas as pd
from os import path
import json


INPUT_DIR = "."
OUTPUT_DIR = "../../delphi_utils/data"
MSA_FILE = "03_20_MSAs.xls"
FIPS_MSA_OUT_FILE = "fips_msa_cross.csv"
ZIP_CODE_FILE = "02_20_uszips.csv"
ZIP_FIPS_OUT_FILE = "zip_fips_cross.csv"
STATE_OUT_FILE = "state_codes.csv"

def convert_fips(x):
    """Ensure fips is a string of length 5."""
    return str(x).zfill(5)

def convert_fips_to_stcode(fips_ser):
    """convert fips string Series to state code string Series"""
    return fips_ser.str[:2]

def wei_iter(df):
    """
    Generator that runs through fips/zip dataframe and spits out dictionary
    """
    for _, row in df.iterrows():
        wei_js = row['county_weights']
        wei_js = json.loads(wei_js.replace("'", '"'))
        wei_sum = sum(wei_js.values())
        if (wei_sum == 0) and (len(wei_js)==1):
            yield {'zip': convert_fips(row['zip']),
                   'fips': convert_fips(row['fips']),
                   'weight': 1.}
        else:
            for fips, wei in wei_js.items():
                yield {'zip': convert_fips(row['zip']),
                       'fips': convert_fips(fips),
                       'weight': wei / wei_sum}

def proc_zip_fips():
    """ ZIP FIPS Cross """
    zip_df = pd.read_csv(ZIP_CODE_FILE)
    zip_reduced = pd.DataFrame(a for a in wei_iter(zip_df))
    zip_reduced.to_csv(path.join(OUTPUT_DIR,ZIP_FIPS_OUT_FILE), index=False)

    zip_df['st_code'] = convert_fips_to_stcode(zip_df['fips'].astype(str).str.zfill(5))
    state_df = zip_df[['st_code','state_id','state_name']].drop_duplicates()
    assert state_df.shape[0] == 52, "More than 52 states?"
    state_df.to_csv(path.join(OUTPUT_DIR,STATE_OUT_FILE), index=False)
    return True

def proc_fips_msa():
    """ FIPS MSA Cross """
    msa_cols = {'CBSA Code': int,
                'Metropolitan/Micropolitan Statistical Area': str,
                'FIPS State Code': str,
                'FIPS County Code': str}
    msa_df = pd.read_excel(MSA_FILE, skiprows=2, skipfooter=4, usecols=msa_cols.keys(), dtype=msa_cols)
    metro_bool = msa_df['Metropolitan/Micropolitan Statistical Area'] == 'Metropolitan Statistical Area'
    msa_df = msa_df[metro_bool]
    msa_df['fips'] = msa_df['FIPS State Code'].str.cat(msa_df['FIPS County Code']).astype(int)
    msa_df.rename(columns={'CBSA Code':'msa'},inplace=True)
    msa_df = msa_df[['fips','msa']]
    msa_df.to_csv(path.join(OUTPUT_DIR,FIPS_MSA_OUT_FILE), index=False)
    return True

if __name__ == "__main__":

    # proc_zip_fips()
    proc_fips_msa()
