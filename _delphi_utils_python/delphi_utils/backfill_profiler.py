#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 23 22:39:07 2021

@author: jingjingtang

Derive a detailed definition of "backfill profile", create a "Backfill 
Profiler" tool for calculating it for any source which can Plot the full 
backfill value distribution (+mean (bias) and stderr) for each lag till 
"Finalized" date:
- Aggregated over reference date & locations, but also separately over 
  ref date and over locations.
- These "backfill curves" are proportions indexed by: lag, reference date, 
  location
    
"""
import os
from math import log10, ceil, floor
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

import numpy as np
import pandas as pd
import seaborn as sns
from scipy import stats

def check_create_dir(save_dir:str):
    """
    Create the directory for saving figures if it is not existed

    Parameters
    ----------
    save_dir : str
        Directory for saving analysis figures.

    Returns
    -------
    None.

    """
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    return

def to_covidcast_df(df:pd.DataFrame, lag_col:str, time_value_col:str, 
                    geo_value_col:str, sample_size_col:str, value_col:str):
    """
    Conform the input dataframe into COVIDcast format. The  lag_column, 
    time_value_column, geo_value_column, sample_size_column, value_column are 
    necesssary. More detailed description of the values in these columns can
    ben found in `help(covidcast.signal)`

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe need to be conformed.
    lag_col : str
        Column name for lag
    time_value: str
        Column name for time_value
    geo_value: str
        Column name for geo_value
    sample_size_col: str
        Column name for sample_size
    value_col: str
        Column name for value

    Returns
    -------
    df: pd.DataFrame

    """
    for col in [lag_col,  sample_size_col, value_col]:
        try:
            df[col] = df[col.astype(float)]
        except ValueError:
            raise ValueError("Values in %s are invalid."%col)
            
    try:
        assert df[time_value_col] == 'datetime64[ns]'
    except AssertionError:
        raise ValueError("The values in the time_value_col should be in \
                         datetime64[ns] format.")
        
    df.rename({lag_col: "lag", time_value_col: "time_value",
               geo_value_col: "geo_value", sample_size_col: "sample_size",
               value_col: "value"}, inplace=True)
    return df


def to_backfill_df(df, signal, data_type = "completeness",
                   value_type="total_count", ref_lag=60):
    """
    Conform the dataset in COVIDcast API format into a backfill dataset.
    
    The output dataset has:
    - time_value: Reference date of the estimate which is the date the estiamte
      is for
    - geo_value: Identifies the location, such as a county FIPS code
    - lag: Integer difference between issue date and time_value, in days.
    - value: The quantity requested
    - value_type: Either "Total counts" (the denominator, usually the total
      number of counts) or "COVID counts" (the numerator, the number of 
      quantity interested, such as the number of positive COVID antigen tests)
    - data_type: Either "completeness" (the percentage of currently reported 
      counts against the number of reported counts ref_lag days later) 
      or "log_dailychange" (log10 of the daily change rate)
    - ref_lag: The fixed lag between the reference date of the estimates and 
      the issue date used to be compared when considering "completeness"
        

    Parameters
    ----------
    df : pd.DataFrame
        Dataset in COVIDcast API format
    signal : str
        String identifying the signal from that source to query, such as 
        "smoothed_cli"
    data_type : str, optional
        Either "completeness" or "log_dailychange". 
        The default is "completeness".
    value_type : str, optional
        Either "total_count" or "value_count". 
        The default is "total_count".
    ref_lag : int, optional
        The fixed lag between the reference date of the estimates and 
      the issue date used to be compared when considering "completeness". 
      The default is 60.

    Returns
    -------
    backfill_df : pd.DataFrame
        Dataframe as described above.

    """
    df["count"] = df["sample_size"] * df["value"] / 100
    
    if value_type == "total_count":
        val_col = "sample_size"
        val_name = "Total counts"
    else:
        val_col = "count"
        val_name = "COVID counts"
    
    pivot_df = pd.pivot_table(df, values=val_col, 
                              index=["geo_value", "time_value"],
                              columns="lag").reset_index()

    if data_type == "completeness":
        if ref_lag >= df["lag"].max():
            print("Not enough days available. ref_lag larger than the \
                  maxmimum lag included in the provided dataset.")
            return None

        for i in range(df["lag"].min(), df["lag"].max() + 1):
            if i == ref_lag:
                continue
            pivot_df[i] = pivot_df[i] / pivot_df[60] * 100
        pivot_df[ref_lag] = pivot_df[ref_lag] / pivot_df[ref_lag] * 100
    
    else:
        for i in range(df["lag"].max(), df["lag"].min(), -1):
            pivot_df[i] = (pivot_df[i] - pivot_df[i-1]) / pivot_df[i-1]
        pivot_df.drop(df["lag"].min(), axis=1, inplace=True)
        
        
    backfill_df = pd.melt(pivot_df, id_vars=["geo_value", "time_value"], 
                          var_name="lag", value_name="value")
    
    if data_type == "completeness":
        backfill_df["value_type"] = val_name
        backfill_df["data_type"] = "completeness"
        backfill_df["ref_lag"] = ref_lag
    else:
        backfill_df.loc[backfill_df["value"] == 0, "value"] = np.nan
        backfill_df["value_type"] = val_name
        backfill_df["data_type"] = "log_dailychange"
        backfill_df["ref_lag"] = -1
        backfill_df["value"] = backfill_df["value"].apply(lambda x:log10(x))
    
    return backfill_df



def create_heatmap_over_refdate(save_dir:str, backfill_df:pd.DataFrame, 
                                source:str, signal:str, 
                                start_date:datetime, end_date:datetime, 
                                geo_values=None, max_lag=90):
    """
    Create heatmaps of the backfill estimates by lags and reference date for
    each location specified in the geo_values.
    The reference date will show as the y-axis while the lag will be the 
    x-axis. The color filled represents the value of the backfill estimates.
    The created heatmaps will be stored in the save_dir as multiple png files.

    Parameters
    ----------
    save_dir : str
        Directory for saving the generated figures.
    backfill_df : pd.DataFrame
        The dataframe that contains information on either completeness 
        or daily change rate.
    source : str
        String identifying the data source to query, such as "fb-survey".
    signal : str
        String identifying the signal from that source to query, such as 
        "smoothed_cli".
    start_date : datetime
        Display data beginning on this date.
    end_date : datetime
        Display data up to this date, inclusive.
    geo_values : list of str, optional
        The list of locations for which the heatmaps will be created. 
        The default is None, which means all the locations included in the 
        backfill_df will be considered. Otherwise, only the locations in the
        geo_values list will be considered.
    max_lag : int, optional
        The maximum lag that will be displayed in the heatmaps. 
        The default is 90.

    Returns
    -------
    None.

    """

    check_create_dir(save_dir)
    
    filtered_backfill_df = backfill_df[backfill_df["lag"]<=max_lag]
    
    pivot_df = pd.pivot_table(filtered_backfill_df, 
                              values="value", 
                              index=["geo_value", "time_value"],
                              columns="lag").reset_index()
    MIN_LAG = filtered_backfill_df["lag"].min()
    value_type = filtered_backfill_df["value_type"].values[0]
    data_type = filtered_backfill_df["data_type"].values[0]
    
    if not geo_values:
        geo_values = pivot_df["geo_value"].unique()  
    max_lag =min(backfill_df["lag"].min(), max_lag)
    
    if data_type == "completeness":
        vmax = int((filtered_backfill_df["value"].max()//20 + 1) * 20)
        vmin = int((filtered_backfill_df["value"].min()//20) * 20)
        cbar_freq = int((vmax-vmin)/20)
        cmap = "tab20c_r"
        cbar_label = "Percentage"
        ref_lag = backfill_df["ref_lag"].values[0]
        fig_title = 'Percentage of Completion \
                \nAgainst values reported %d days later\
                \n%s %s, %s'%(ref_lag, source, value_type, "%s")
    else:
        vmax = ceil(filtered_backfill_df["value"].max())
        vmin = floor(filtered_backfill_df["value"].min())
        cbar_freq = int(vmax/20)
        cmap = "YlGnBu"
        cbar_label = "log10(Daily Change Rate)"
        ref_lag = 0
        fig_title = 'Daily Change Rate\n%s, %s, %s'%(source, value_type, "%s")
        
    n_days = (end_date - start_date).days + 1 - ref_lag
    time_index = np.array([(start_date + timedelta(i)).date() for i in range(n_days)])
    
    for geo in geo_values:    
        sns.set(font_scale=1.2)
        heatmap_df = pivot_df.loc[(pivot_df["geo_value"] == geo) 
                                  & (pivot_df["time_value"].isin(time_index)),
                                  ["time_value"] + list(range(MIN_LAG, max_lag+1))]
        heatmap_df.set_index("time_value", inplace=True)
        plt.figure(figsize = (18, 15))
        plt.style.context("ggplot")
        heatmap_df = heatmap_df.reindex(time_index)
        selected_ytix = [x.weekday() == 6 for x in time_index] # Show Sundays on y_axis
        ax = sns.heatmap(heatmap_df, annot=False, cmap=cmap, cbar=True,
                         vmax=vmax, vmin=vmin, center=0,
                         cbar_kws=dict(label=cbar_label,
                                       ticks=list(range(0, vmax+1, cbar_freq)))) 
        xtix = ax.get_xticks()
        plt.xlabel("Lag", fontsize = 25)
        plt.ylabel("Reference Date", fontsize = 25)
        plt.title(fig_title%geo, fontsize = 30, loc="left")
        plt.xticks(fontsize=20)
        plt.yticks(fontsize=15)
        ax.set_yticks(np.arange(0.5, n_days + 0.5)[selected_ytix])
        ax.set_yticklabels(time_index[selected_ytix])
        ax.set_xticks(xtix[::5])
        plt.savefig(save_dir+"/"+str(geo)+".png")
    return 

def create_lineplot_over_loations(save_dir:str, backfill_df:pd.DataFrame, 
                                  source:str, signal:str, fig_name:str, 
                                  start_date:datetime, end_date:datetime, 
                                  geo_values=None, max_lag=90):
    """
    Create a lineplot of the backfill estimates by lag and location for
    across a certain range of reference dates.
    The backfill estimates will show as the y-axis while the lag will be the 
    x-axis. Each line represents the mean across reference dates for a specific 
    location with 95% confidence interval shown as the band. The created 
    lineplot will be stored in the save_dir with specified figure name as a 
    png files.
    
    Parameters
    ----------
    save_dir : str
        Directory for saving the generated figures.
    backfill_df : pd.DataFrame
        The dataframe that contains information on either completeness 
        or daily change rate.
    source : str
        String identifying the data source to query, such as "fb-survey".
    fig_name : str
        The file name of the figure, no suffix needed.
    start_date : datetime
        Display data beginning on this date.
    end_date : datetime
        Display data up to this date, inclusive.
    geo_values : list of str, optional
        The list of locations for which the heatmaps will be created. 
        The default is None, which means all the locations included in the 
        backfill_df will be considered. Otherwise, only the locations in the
        geo_values list will be considered.
    max_lag : int, optional
        The maximum lag that will be displayed in the heatmaps. 
        The default is 90.

    Returns
    -------
    None.

    """

    check_create_dir(save_dir)
    
    if not geo_values:
        geo_values = backfill_df["geo_value"].unique() 
    max_lag =min(backfill_df["lag"].min(), max_lag)
    
    line_df = backfill_df.loc[(backfill_df["lag"] <=max_lag)
                              & (backfill_df["time_value"]<=end_date)
                              & (backfill_df["time_value"]>=start_date)]
    data_type = line_df["data_type"].values[0]
    
    if data_type == "completeness":
        vmax = int((line_df["value"].max() //20 + 1) * 20)
        vmin = int((line_df["value"].min() //20) * 20)
        ref_lag = line_df["ref_lag"].values[0]
        value_type = line_df["value_type"].values[0]
        fig_title = 'Percentage of Completion\
                \nAgainst values reported %d days later\
                \n%s %s, Mean with 95%% CI \
                \nReference Date: From %s to %s'%(ref_lag, source, value_type,  
                                          start_date.date(), end_date.date())
        ylabel = "%Reported"
    else:
        vmax = ceil(line_df["value"].max())
        vmin = floor(line_df["value"].min())
        ref_lag = line_df["ref_lag"].values[0]
        value_type = line_df["value_type"].values[0]
        fig_title = 'Daily Change Rate\
                \n%s %s, Mean with 95%% CI \
                \nReference Date: From %s to %s'%(source, value_type, 
                                          start_date.date(), end_date.date())
        ylabel = "log10(Daily Change Rate)"
            
    line_df = line_df[line_df["geo_value"].isin(geo_values)].dropna()
    
    plt.figure(figsize = (10, 10))
    sns.lineplot(data=line_df, x="lag", y="value", hue="geo_value", 
                 ci=95, err_style="band")
    plt.xlabel("Lag", fontsize=20)
    plt.ylabel(ylabel, fontsize=20)
    plt.title(fig_title, fontsize=25, loc="left")
    plt.legend(loc="upper left")
    if data_type == "completeness":
        plt.axhline(90, linestyle = "--")
        plt.axhline(100, linestyle = "--")
    plt.yticks(np.arange(0, vmax, 10), fontsize=15)
    plt.xticks(fontsize=15)
    plt.ylim(vmin, vmax)
    plt.savefig(save_dir+"/"+fig_name+".png", bbox_inches='tight')
    return 

def create_violinplot_over_lag(save_dir: str, backfill_df:pd.DataFrame, 
                               source:str, signal:str, 
                               start_date:datetime, end_date:datetime, 
                               geo_values=None, max_lag=90):
    """
    Create violinplots of the backfill estimates by lags and location across
    a certain rane of reference date for each location specified in geo_values.
    Each violin shows the distribution of quantitative backfill estimates 
    across reference dates for a specific location and lag. The created 
    violinplots will be stored in the save_dir as multiple png files.

    Parameters
    ----------
    save_dir : str
        Directory for saving the generated figures.
    backfill_df : pd.DataFrame
        The dataframe that contains information on either completeness 
        or daily change rate.
    source : str
        String identifying the data source to query, such as "fb-survey".
    signal : str
        String identifying the signal from that source to query, such as 
        "smoothed_cli".
    start_date : datetime
        Display data beginning on this date.
    end_date : datetime
        Display data up to this date, inclusive.
    geo_values : list of str, optional
        The list of locations for which the heatmaps will be created. 
        The default is None, which means all the locations included in the 
        backfill_df will be considered. Otherwise, only the locations in the
        geo_values list will be considered.
    max_lag : int, optional
        The maximum lag that will be displayed in the heatmaps. 
        The default is 90.

    Returns
    -------
    None.

    """
    

    check_create_dir(save_dir)
    
    if not geo_values:
        geo_values = backfill_df["geo_value"].unique()  
    max_lag =min(backfill_df["lag"].max(), max_lag)
    selected_lags = list(range(0, max_lag + 10, 10))
    
    line_df = backfill_df.loc[(backfill_df["lag"].isin(selected_lags))
                              & (backfill_df["time_value"]<=end_date)
                              & (backfill_df["time_value"]>=start_date)]
    data_type = line_df["data_type"].values[0]
    
    if data_type == "completeness":
        vmax = int((line_df["value"].max() //20 + 1) * 20)
        vmin = int((line_df["value"].min() //20) * 20)
        ref_lag = line_df["ref_lag"].values[0]
        value_type = line_df["value_type"].values[0]
        fig_title = 'Percentage of Completion\
                \nAgainst values reported %d days later\
                \n%s %s, Mean with 95%%%% CI \
                \n%s, Reference Date: From %s to %s'%(ref_lag, source, 
                value_type, "%s", start_date.date(), end_date.date())
        ylabel = "%Reported"
    else:
        vmax = ceil(line_df["value"].max())
        vmin = floor(line_df["value"].min())
        ref_lag = line_df["ref_lag"].values[0]
        value_type = line_df["value_type"].values[0]
        fig_title = 'Daily Change Rate\
                \n%s %s, Mean with 95%%%% CI \
                \n%s, Reference Date: From %s to %s'%(source, value_type, "%s",
                                          start_date.date(), end_date.date())
        ylabel = "log10(Daily Change Rate)"
            
    line_df = line_df[line_df["geo_value"].isin(geo_values)]
    
    for geo in line_df["geo_value"].unique():
        plt.figure(figsize = (10, 10))
        plt.style.context("ggplot")
        sublinedf = line_df[line_df["geo_value"] == geo]
        sns.violinplot(data=sublinedf, x="lag", y="value", cut=0)
        plt.xlabel("Lag", fontsize=20)
        plt.ylabel(ylabel, fontsize=20)
        plt.ylim(vmin, vmax)
        plt.title(fig_title%geo, fontsize=25, loc="left")
        plt.savefig(save_dir+"/"+geo+".png", bbox_inches='tight')
    return 

def create_summary_plots(save_dir, backfill_df, 
                         source, signal, 
                         start_date, end_date, 
                         geo_values=None, max_lag=90):
    """
    Create two summary plots for the backfill dateframe. 
    - A lineplot shows the mean and 95% confidence interval of backfill 
    estimates by lag across a certain range of reference dates and all 
    locations in geo_values.
    - A lineplot shows the mean and 95% confidence interval of backk fill 
    estimates by reference date across all locations in geo_values and all 
    lags
    

    Parameters
    ----------
    save_dir : str
        Directory for saving the generated figures.
    backfill_df : pd.DataFrame
        The dataframe that contains information on either completeness 
        or daily change rate.
    source : str
        String identifying the data source to query, such as "fb-survey".
    signal : str
        String identifying the signal from that source to query, such as 
        "smoothed_cli".
    start_date : datetime
        Display data beginning on this date.
    end_date : datetime
        Display data up to this date, inclusive.
    geo_values : list of str, optional
        The list of locations for which the heatmaps will be created. 
        The default is None, which means all the locations included in the 
        backfill_df will be considered. Otherwise, only the locations in the
        geo_values list will be considered.
    max_lag : int, optional
        The maximum lag that will be displayed in the heatmaps. 
        The default is 90.

    Returns
    -------
    None.

    """
    
    check_create_dir(save_dir)
    
    if not geo_values:
        geo_values = backfill_df["geo_value"].unique()      
    max_lag =min(backfill_df["lag"].max(), max_lag)
    end_date = end_date-timedelta(days=max_lag)
    
    line_df = backfill_df.loc[(backfill_df["lag"] <=max_lag)
                              & (backfill_df["time_value"]<=end_date)
                              & (backfill_df["time_value"]>=start_date)
                              ]
    data_type = line_df["data_type"].values[0]
    value_type = line_df["value_type"].values[0]
    
    n_days = (end_date - start_date).days + 1
    time_index = np.array([(start_date + timedelta(i)).date() for i in range(n_days)])
    
    if data_type == "completeness":
        ref_lag = line_df["ref_lag"].values[0]
        fig_title_over_lag = 'Percentage of Completion\
            \nAgainst values reported %d days later\
            \n%s %s, Mean with 95%% CI \
            \nAll locations \
            \nReference Date: From %s to %s'%(ref_lag, source, value_type, 
            start_date.date(), end_date.date())
        fig_title_over_date = 'Percentage of Completion\
            \nAgainst values reported %d days later\
            \n%s %s, Mean with 95%% CI\
            \nAll locations, lag <= %d'%(ref_lag, source, value_type, max_lag)
        ylabel = "%Reported"
    else:
        ref_lag = line_df["ref_lag"].values[0]
        fig_title_over_lag = 'Daily Change Rate\
            \n%s %s, Mean with 95%% CI \
            \nAll locations \
            \nReference Date: From %s to %s'%(source, value_type, 
            start_date.date(), end_date.date())
        fig_title_over_date = 'Daily Change Rate\
            \n %s %s, Mean with 95%% CI \
            \nAll locations, lag <= %d'%(source, value_type, max_lag)
        ylabel = "log10(Daily Change Rate)"

    line_df = line_df[line_df["geo_value"].isin(geo_values)].dropna()
    
    # Lineplot over lags
    plt.figure(figsize = (10, 10))
    plt.style.context("ggplot")
    sns.lineplot(data=line_df, x="lag", y="value", ci=95, err_style="band")
    plt.xlabel("Lag", fontsize=20)
    plt.ylabel(ylabel, fontsize=20)
    plt.title(fig_title_over_lag, fontsize=25, loc="left")
    plt.legend(loc="upper left")
    plt.yticks(fontsize=15)
    plt.xticks(fontsize=15)
    plt.savefig(save_dir+"/lineplot_over_lag.png", bbox_inches='tight')
    
    
    
    # Lineplot over dates
    plt.figure(figsize = (10, 10))
    plt.style.context("ggplot")
    sns.lineplot(data=line_df, x="time_value", y="value", ci=95, err_style="band")
    # selected_xtix = [x.day == 1 for x in time_index]  #First day of each month
    for x in time_index:
        if x.day==1:
            plt.axvline(x, linestyle="--")
    plt.xlabel("Date", fontsize=20)
    plt.ylabel(ylabel, fontsize=20)
    plt.title(fig_title_over_date, fontsize=25, loc="left")
    plt.legend(loc="upper left")
    plt.yticks(fontsize=15)
    plt.xticks(fontsize=15, rotation=45)
    plt.savefig(save_dir+"/lineplot_over_date.png", bbox_inches='tight')
    return 
    

def backfill_mean_check(backfill_df:pd.DataFrame, lag:int, geo_value, 
                        test_start_date:datetime, test_end_date:datetime, 
                        train_start_date:datetime, train_end_date:datetime):
    """
    Conduct a two-sided t-test for the null hypothesis that 2 independent 
    samples have identical average (expected) values. This test assumes that 
    the populations have unknown variance.
    Calculate the T-test for the means of the backfill estimates of two time 
    period. Use historical data for training, should include at least 28 days.
    Use the dates of interest for testing, should include at least 7 days.

    Parameters
    ----------
    backfill_df : pd.DataFrame
        The dataframe that contains information on either completeness 
        or daily change rate.
    lag : int
        DESCRIPTION.
    geo_value : TYPE
        The location that is considered.
    test_start_date : datetime
        Testing period beginning on this date.
    test_end_date : datetime
        Testing period up to this date, inclusive.
    train_start_date : datetime
        Training period beginning on this date.
    train_end_date : datetime
        Training period up to this date, inclusive.

    Returns
    -------
    t : float
        The calculated t-statistic..
    p : float
        The two-tailed p-value.

    """
    if geo_value not in backfill_df["geo_value"]:
        print("Invalid geo_value.")
        return
    test_data = backfill_df.loc[(backfill_df["geo_value"] == geo_value)
                                & (backfill_df["time_value"]<=test_end_date)
                                & (backfill_df["time_value"]>=test_start_date)
                                & (backfill_df["lag"] == lag)].dropna()
    train_data = backfill_df.loc[(backfill_df["geo_value"] == geo_value)
                                & (backfill_df["time_value"]<=train_end_date)
                                & (backfill_df["time_value"]>=train_start_date)
                                & (backfill_df["lag"] == lag)].dropna()
    if train_data.shape[0] <= 28:
        print("The training period is not long enough, should be longer than \
              28 days")
        return
    if test_data.shape[0] <= 7:
        print("The testing period is not long enough, should be longer than \
              7 days")
        return

    t, p = stats.ttest_ind(train_data["value"].values, 
                     test_data["value"].values, 
                     equal_var=False)
    return t, p
    

def create_mean_check_df(save_dir:str, backfill_df:pd.DataFrame, 
                      test_start_date:datetime, test_end_date:datetime, 
                      train_start_date:datetime, train_end_date:datetime,
                      lags=None, geo_values=None):
    """
    Create a csv file for the results of the two-sided t-tests as described
    in backfill_mean_check function. The t-tests will be conducted for each
    lag and each location.

    Parameters
    ----------
    save_dir : str
        Directory for saving the analysis result.
    backfill_df : pd.DataFrame
        The dataframe that contains information on either completeness 
        or daily change rate.
    test_start_date : datetime
        Testing period beginning on this date.
    test_end_date : datetime
        Testing period up to this date, inclusive.
    train_start_date : datetime
        Training period beginning on this date.
    train_end_date : datetime
        Training period up to this date, inclusive.
    lags : list of int, optional
        The list of lags for which the statistical analysis will consider. 
        The default is None, which means all the lags included in the backfill
        dataframe will be considered. Otherwise, only the lags specified in 
        lags list will be considered.
    geo_values : TYPE, optional
        The list of locations for which the statistical analysis will consider. 
        The default is None, which means all the locations included in the 
        backfill dataframe will be considered. Otherwise, only the locations 
        in the geo_values list will be considered.

    Returns
    -------
    summary_df: pd.DataFrame

    """
    summary_df = pd.DataFrame(columns=["geo_value", "lag", 
                                       "p", "t_statistics"])
    i = 0
    if not geo_values:
        geo_values = backfill_df["geo_value"].unique() 
    if not lags:
        lags = backfill_df.loc[(backfill_df["time_value"]<=test_end_date)
                               & (backfill_df["time_value"]>=test_start_date),
                               "lag"].unique() 

    for geo in geo_values:
        for lag in lags:
            t, p = backfill_mean_check(backfill_df, lag, geo, 
                                       test_start_date, test_end_date, 
                                       train_start_date, train_end_date)
            summary_df.loc[i] = [geo, lag, t, p]
            i += 1
    summary_df.to_csv(save_dir+"/mean_check_results.csv", index=False)
    return summary_df
    



    
