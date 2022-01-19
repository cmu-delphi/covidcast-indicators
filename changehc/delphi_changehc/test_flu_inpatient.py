#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 18 08:40:49 2022

@author: bwilder
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import datetime

start_date = datetime.date(2020, 1, 8)
end_date = datetime.date(2022, 1, 8)
dfs = []
for i in range((end_date - start_date).days):
    print(i)
    date = start_date + datetime.timedelta(days=i)
    try:
        data_date = pd.read_csv("../receiving/%s%s%s_state_smoothed_inpatient_flu.csv"%(date.year, str(date.month).zfill(2), str(date.day).zfill(2)))
    except:
        print('missing: {}'.format(date))
    data_date['date'] = pd.to_datetime(date)
    dfs.append(data_date)

data = pd.concat(dfs)
data = data.reset_index()

plt.plot(data.date[data.geo_id == 'vt'], data.val[data.geo_id == 'vt']/100)


raw_flu = pd.read_csv('../cache/20220116_Counts_Products_Flu_Inpatient.dat.gz', header=None)
raw_flu = raw_flu.rename(columns={0: 'state', 1: 'date', 2 : 'total'})
raw_denom = pd.read_csv('../cache/20220116_Counts_Products_Denom_Inpatient_By_State.dat.gz', header=None)
raw_denom = raw_denom.rename(columns={0: 'date', 1: 'state', 2 : 'total'})
raw_flu.total.replace('3 or less', 1, inplace=True)
raw_denom.total.replace('3 or less', 1, inplace=True)
raw_flu['total'] = raw_flu['total'] .astype(int)
raw_denom['total'] = raw_denom['total'] .astype(int)
raw_flu['date'] = raw_flu['date'].astype(str)
raw_denom['date'] = raw_denom['date'].astype(str)
raw_flu = raw_flu[raw_flu.date <= '20220108']
raw_denom = raw_denom[raw_denom.date <= '20220108']
raw_flu['date'] = pd.to_datetime(raw_flu['date'], errors='coerce')
raw_denom['date'] = pd.to_datetime(raw_denom['date'], errors='coerce')
raw_denom = raw_denom.rename(columns={'total' : 'denom'})
raw_flu['state'] = raw_flu.state.astype(float)
raw_flu = raw_flu[~raw_flu.state.isna()]
raw_flu['state'] = raw_flu['state'].astype(int).astype(str).str.zfill(2)
merged =  raw_denom.merge(raw_flu, on=['state', 'date'], how='left')
merged = merged.fillna(0)
merged = merged.sort_values('date')

subset = merged[merged.state == '50']
plt.plot(subset.date, subset.total/subset.denom)