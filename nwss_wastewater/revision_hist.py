#!/fasterHome/anaconda3/envs/baseEmacs python3
# jank script to download data and record rough revision history
import os
import numpy as np
import pandas as pd
from sodapy import Socrata

os.chdir("../nwss_wastewater")

token = os.environ.get("SODAPY_APPTOKEN")
sig_digits = 7


# because why would you insist on the correct number of sigfigs?
def sig_digit_round(value, N):
    in_value = value
    value = np.asarray(value).copy()
    zero_mask = (value == 0) | np.isinf(value) | np.isnan(value)
    value[zero_mask] = 1.0
    sign_mask = value < 0
    value[sign_mask] *= -1
    exponent = np.ceil(np.log10(value))
    result = 10**exponent * np.round(value * 10 ** (-exponent), N)
    result[sign_mask] *= -1
    result[zero_mask] = in_value[zero_mask]
    return result


def merge_new_results(historic, new_data, base_columns, save_to):
    merged = historic.merge(new_data, how="outer", on=base_columns)
    merged["issue"] = merged[["issue_x", "issue_y"]].max(axis=1)
    merged_final = merged.drop(["issue_x", "issue_y"], axis=1)
    merged_final.to_csv(save_to)


# the concentration dataset
client = Socrata("data.cdc.gov", token)
results = client.get("g653-rqe2", limit=10**10)
df = pd.DataFrame.from_records(results)
df.pcr_conc_smoothed = pd.to_numeric(df.pcr_conc_smoothed)
base_columns = df.columns.tolist()
df["issue"] = pd.Timestamp("now")
data_so_far = pd.read_csv("concentration.csv", index_col=0, parse_dates=[-1])

data_so_far.pcr_conc_smoothed = sig_digit_round(
    data_so_far.pcr_conc_smoothed, sig_digits
)
df.pcr_conc_smoothed = sig_digit_round(df.pcr_conc_smoothed, sig_digits)
# merging, keeping the most recent date we've seen that exact row, with any difference generating a new row
merge_new_results(data_so_far, df, base_columns, "concentration.csv")


# and now for the metric data
client = Socrata("data.cdc.gov", token)
results_metric = client.get("2ew6-ywp6", limit=10**10)

df_metric = pd.DataFrame.from_records(results_metric)
df_metric.transpose()
df_metric.dtypes
df_metric.wwtp_id = pd.to_numeric(df_metric.wwtp_id)
df_metric.population_served = pd.to_numeric(df_metric.population_served)
df_metric.detect_prop_15d = pd.to_numeric(df_metric.detect_prop_15d)
df_metric.percentile = pd.to_numeric(df_metric.percentile)
df_metric.ptc_15d = pd.to_numeric(df_metric.ptc_15d)
df_metric.sample_location_specify = pd.to_numeric(df_metric.sample_location_specify)
base_columns = df_metric.columns.tolist()
base_columns
df_metric["issue"] = pd.Timestamp("now")
data_so_far = pd.read_csv("metric.csv", index_col=0, parse_dates=[-1])
data_so_far.transpose()
merge_new_results(data_so_far, df_metric, base_columns, "metric.csv")
