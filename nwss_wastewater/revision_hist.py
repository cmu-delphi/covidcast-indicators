#!/usr/bin/env python3
# jank script to download data and record rough revision history
import os
import numpy as np
import pandas as pd
from sodapy import Socrata

token = os.environ.get("SODAPY_APPTOKEN")
client = Socrata("data.cdc.gov", token)
results = client.get("g653-rqe2", limit=10**10)

results2 = client.get("g653-rqe2", limit=10**10)

df = pd.DataFrame.from_records(results)
df["issue"] = pd.Timestamp("now")
data_so_far = pd.read_csv("g653-rqe2.csv", index_col=0)
# merging, keeping the most recent date we've seen that exact row, with any difference generating a new row
merged = data_so_far.merge(
    df, how="outer", on=["key_plot_id", "date", "pcr_conc_smoothed", "normalization"]
)
merged["issue"] = merged[["issue_x", "issue_y"]].max(axis=1)
merged.to_csv("g653-rqe2.csv")

client = Socrata("data.cdc.gov", token)
results_metric = client.get("2ew6-ywp6", limit=10**10)

df_metric = pd.DataFrame.from_records(results_metric)
df_metric.transpose()
