#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Functions for the backfill profiler.

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
from math import log10, ceil, tanh
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

import numpy as np
import pandas as pd
import seaborn as sns


def check_create_dir(save_dir:str):
    """Create the directory for saving figures if it is not existed.

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
    return save_dir

def to_covidcast_df(df:pd.DataFrame, lag_col:str, time_value_col:str,
                    geo_value_col:str, sample_size_col:str, value_col:str):
    """Conform the input dataframe into COVIDcast format.

    The lag_column, time_value_column, geo_value_column, sample_size_column,
    value_column are necesssary. More detailed description of the values in
    these columns can ben found in `help(covidcast.signal)`.

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
            df[col] = df[col].astype(float)
        except ValueError as e:
            raise ValueError("Values in %s are invalid."%col) from e

    try:
        assert df[time_value_col].dtype == 'datetime64[ns]'
    except AssertionError as e:
        raise ValueError("The values in the time_value_col should be in \
                         datetime64[ns] format.") from e

    df.rename({lag_col: "lag", time_value_col: "time_value",
               geo_value_col: "geo_value", sample_size_col: "sample_size",
               value_col: "value"}, axis=1, inplace=True)
    return df


def to_backfill_df(df, data_type = "completeness",
                   value_type="total_count", ref_lag=60):
    """Conform the dataset in COVIDcast API format into a backfill dataset.

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
      or "dailychange" (log10 of the daily change rate)
    - ref_lag: The fixed lag between the reference date of the estimates and
      the issue date used to be compared when considering "completeness"


    Parameters
    ----------
    df : pd.DataFrame
        Dataset in COVIDcast API format
    data_type : str, optional
        Either "completeness" or "dailychange".
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
    elif value_type == "covid_count":
        val_col = "count"
        val_name = "COVID counts"
    else:
        val_col = "value"
        val_name = "COVID Ratio"

    pivot_df = pd.pivot_table(df, values=val_col,
                              index=["geo_value", "time_value"],
                              columns="lag").reset_index()

    if data_type == "completeness":
        try:
            assert ref_lag <= df["lag"].max()
        except AssertionError as e:
            raise ValueError("Not enough days available. ref_lag larger than \
                             the maxmimum lag included in the provided \
                             dataset.") from e

        for i in range(df["lag"].min(), df["lag"].max() + 1):
            if i == ref_lag:
                continue
            pivot_df[i] = pivot_df[i] / pivot_df[ref_lag] * 100
        pivot_df[ref_lag] = pivot_df[ref_lag] / pivot_df[ref_lag] * 100

    else:
        for i in range(df["lag"].max(), df["lag"].min(), -1):
            pivot_df[i] = (pivot_df[i] - pivot_df[i-1]) / pivot_df[i-1]
        pivot_df.drop(df["lag"].min(), axis=1, inplace=True)


    backfill_df = pd.melt(pivot_df, id_vars=["geo_value", "time_value"],
                          var_name="lag", value_name="value")
    
    backfill_df["issue_date"] = [x \
                  + timedelta(days=y) for x,y in zip(backfill_df["time_value"],
                                                     backfill_df["lag"])]

    if data_type == "completeness":
        backfill_df["value_type"] = val_name
        backfill_df["data_type"] = "completeness"
        backfill_df["ref_lag"] = ref_lag
    else:
        backfill_df["value_type"] = val_name
        backfill_df["data_type"] = "dailychange"
        backfill_df["ref_lag"] = -1
        

    return backfill_df



def create_heatmap_by_refdate(save_dir:str, backfill_df:pd.DataFrame,
                                source:str, start_date:datetime,
                                end_date:datetime, geo_values=[],
                                max_lag=90):
    """Create heatmaps of the backfill estimates.

    The heatmaps show backfill estimates by lags and reference date for
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
    start_date : datetime
        Display data beginning on this date.
    end_date : datetime
        Display data up to this date, inclusive. In order to have informative
        visualization, the end_date should be at least ref_lag days latter than
        the start_date.
    geo_values : list of str, optional
        The list of locations for which the heatmaps will be created.
        The default is an empty list, which means all the locations included in the
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

    filtered_backfill_df = backfill_df[(backfill_df["lag"]<=max_lag)
                                       & (backfill_df["time_value"] >= start_date)
                                       & (backfill_df["time_value"] <= end_date)]
    
    min_lag = filtered_backfill_df["lag"].min()
    value_type = filtered_backfill_df["value_type"].values[0]
    data_type = filtered_backfill_df["data_type"].values[0]

    if len(geo_values) == 0:
        geo_values = filtered_backfill_df["geo_value"].unique()
    max_lag =min(filtered_backfill_df["lag"].max(), max_lag)

    if data_type == "completeness":
        vmax = int((filtered_backfill_df["value"].quantile(.99)//20 + 1) * 20)
        vmin = int((filtered_backfill_df["value"].quantile(.01)//20) * 20)
        cbar_freq = ceil((vmax-vmin)/20) + 1
        cmap = "magma"
        cbar_label = "Percentage"
        ref_lag = backfill_df["ref_lag"].values[0]
        fig_title = 'Percentage of Completion \
                \nAgainst values reported %d days later\
                \n%s %s, %s'%(ref_lag, source, value_type, "%s")
    elif data_type == "dailychange":
        scale = 2.0 / filtered_backfill_df["value"].quantile(0.95)
        scale = 10 ** round(log10(scale))
        filtered_backfill_df["value"] = filtered_backfill_df["value"].apply(lambda x: scale * x).apply(tanh)
        vmax = (filtered_backfill_df["value"].quantile(.99) // 0.1 + 1) * .1
        vmin = (filtered_backfill_df["value"].quantile(.01) // .1) * .1
        cbar_freq = ceil((vmax-vmin)/.1) + 1
        cmap = "coolwarm"
        cbar_label = "Daily Change"
        ref_lag = 0
        fig_title = 'Daily Change Rate\n%s, %s, %s'%(source, value_type, "%s")

    n_days = (end_date - start_date).days + 1 - ref_lag
    time_index = np.array([(start_date + timedelta(i)).date() for i in range(n_days)])

    pivot_df = pd.pivot_table(filtered_backfill_df,
                              values="value",
                              index=["geo_value", "time_value"],
                              columns="lag").reset_index()
    

    for geo in geo_values:
        sns.set(font_scale=1.2)
        heatmap_df = pivot_df.loc[(pivot_df["geo_value"] == geo)
                                  & (pivot_df["time_value"].isin(time_index)),
                                  ["time_value"] + list(range(min_lag, max_lag+1))]
        if data_type == "dailychange":
            heatmap_df.insert(loc=1, column=0, value=np.nan)
        heatmap_df.set_index("time_value", inplace=True)
        plt.figure(figsize = (18, 15))
        plt.style.context("ggplot")
        heatmap_df = heatmap_df.reindex(time_index)
        # Show Sundays on y_axis
        selected_ytix = [x.weekday() == 6 for x in time_index]
        ax = sns.heatmap(heatmap_df, annot=False, cmap=cmap, cbar=True,
                         vmax=vmax, vmin=vmin, center=0,
                         cbar_kws=dict(label=cbar_label))
        cbar = ax.collections[0].colorbar
        if data_type == "completeness":
            cbar.set_ticks(np.linspace(vmin, vmax, num=cbar_freq))
        else:
            cbar.set_ticks([tanh(10*x) for x in [-1, -0.2, -0.1, -0.05,
                                                       -0.02, 0, 0.02, 0.05,
                                                       0.1, 0.2, 1]])
            cbar.set_ticklabels(["<-100%", "-20%", "-10%", "-5%", "-2%", "0%",
                                 "+2%", "+5%", "+10%", "+20%", ">+100%"])
        
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
        
        


def create_lineplot_by_loations(save_dir:str, backfill_df:pd.DataFrame,
                                  source:str, fig_name:str,
                                  start_date:datetime, end_date:datetime,
                                  geo_values=[], max_lag=90):
    """Create a lineplot of the backfill estimates across locations.

    The lineplot show the backfill estimates by lag and location for
    across a certain range of reference dates. The backfill estimates will
    show as the y-axis while the lag will be the x-axis. Each line represents
    the mean across reference dates for a specific location with 95% confidence
    interval shown as the band. The created lineplot will be stored in the
    save_dir with specified figure name as a png files.

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
        The default is an empty list, which means all the locations included
        in the backfill_df will be considered. Otherwise, only the locations
        in the geo_values list will be considered.
    max_lag : int, optional
        The maximum lag that will be displayed in the heatmaps.
        The default is 90.

    Returns
    -------
    None.

    """
    check_create_dir(save_dir)

    if len(geo_values) == 0:
        geo_values = backfill_df["geo_value"].unique()
    max_lag =min(backfill_df["lag"].max(), max_lag)

    line_df = backfill_df.loc[(backfill_df["lag"] <=max_lag)
                              & (backfill_df["time_value"]<=end_date)
                              & (backfill_df["time_value"]>=start_date)]
    data_type = line_df["data_type"].values[0]

    if data_type == "completeness":
        vmax = int((line_df["value"].quantile(.99) //20 + 1) * 20)
        vmin = int((line_df["value"].min() //20) * 20)
        cbar_freq = ceil((vmax - vmin)/10) + 1
        ref_lag = line_df["ref_lag"].values[0]
        value_type = line_df["value_type"].values[0]
        legend_loc = "lower right"
        fig_title = 'Percentage of Completion\
                \nAgainst values reported %d days later\
                \n%s %s, Mean with 95%% CI \
                \nReference Date: From %s to %s'%(ref_lag, source, value_type,
                                          start_date.date(), end_date.date())
        ylabel = "%Reported"
    elif data_type == "dailychange":
        scale = 2.0 / line_df["value"].quantile(0.95)
        scale = 10 ** round(log10(scale))
        line_df["value"] = line_df["value"].apply(lambda x: scale * x).apply(tanh)
        vmax = (line_df["value"].quantile(.99) // 0.1 + 1) * .1
        vmin = (line_df["value"].quantile(.01) // .1) * .1
        cbar_freq = ceil((vmax-vmin)/.1) + 1
        ref_lag = line_df["ref_lag"].values[0]
        value_type = line_df["value_type"].values[0]
        legend_loc = "upper right"
        fig_title = 'Daily Change Rate\
                \n%s %s, Mean with 95%% CI \
                \nReference Date: From %s to %s'%(source, value_type,
                                          start_date.date(), end_date.date())
        ylabel = "Daily Change"

    line_df = line_df[line_df["geo_value"].isin(geo_values)].dropna()

    plt.figure(figsize = (10, 10))
    sns.lineplot(data=line_df, x="lag", y="value", hue="geo_value",
                 ci=95, err_style="band")
    plt.xlabel("Lag", fontsize=20)
    plt.ylabel(ylabel, fontsize=20)
    plt.title(fig_title, fontsize=25, loc="left")
    plt.legend(loc=legend_loc)
    if data_type == "completeness":
        plt.axhline(90, linestyle = "--")
        plt.axhline(100, linestyle = "--")
        plt.yticks(np.linspace(vmin, vmax, num=cbar_freq), fontsize=15)
        plt.ylim(vmin, vmax)
    else:
        plt.yticks([tanh(10*x) for x in [-1, -0.2, -0.1, -0.05, -0.02, 0, 0.02, 0.05, 0.1, 0.2, 1]], 
                   ["<-100%", "- 20%", "- 10%", "- 5%", "- 2%", "0%", "2%", "5%", "10%", "20%", ">100%"])
        plt.ylim(vmin, vmax)
    plt.xticks(np.linspace(0, 90, num=10), fontsize=15)
    
    plt.savefig(save_dir+"/"+fig_name+".png", bbox_inches='tight')

def create_lineplot_by_issuedate(save_dir:str, backfill_df:pd.DataFrame,
                                  source:str, start_date:datetime,
                                  end_date:datetime, geo_values=[],
                                  max_lag=90):
    """Create a lineplot of the backfill estimates across issue date.

    The lineplot show the backfill estimates by issue date and location
    across a certain range of reference dates. The backfill estimates will
    show as the y-axis while the issue date will be the x-axis. Each line
    represents the mean across reference dates for a specific location with
    95% confidence interval shown as the band. The created lineplot will be
    stored in the save_dir with specified figure name as a png files.

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
        The default is an empty list, which means all the locations included
        in the backfill_df will be considered. Otherwise, only the locations
        in the geo_values list will be considered.
    max_lag : int, optional
        The maximum lag that will be displayed in the heatmaps.
        The default is 90.

    Returns
    -------
    None.

    """
    check_create_dir(save_dir)

    if len(geo_values) == 0:
        geo_values = backfill_df["geo_value"].unique()
    max_lag =min(backfill_df["lag"].max(), max_lag)

    line_df = backfill_df.loc[(backfill_df["lag"] <=max_lag)
                              & (backfill_df["time_value"]<=end_date)
                              & (backfill_df["time_value"]>=start_date)]
    data_type = line_df["data_type"].values[0]

    if data_type == "completeness":
        ref_lag = line_df["ref_lag"].values[0]
        value_type = line_df["value_type"].values[0]
        legend_loc = "upper left"
        fig_title = 'Percentage of Completion, %s %s\
                \nAgainst values reported %d days later'%(source, value_type,
                ref_lag)
        ylabel = "%Reported"
    elif data_type == "dailychange":
        scale = 2.0 / line_df["value"].quantile(0.95)
        scale = 10 ** round(log10(scale))
        line_df["value"] = line_df["value"].apply(lambda x: scale * x).apply(tanh)
        value_type = line_df["value_type"].values[0]
        legend_loc = "upper right"
        fig_title = 'Daily Change Rate, %s %s'%(source, value_type)
        ylabel = "Daily Change Rate"

    for geo in geo_values:
        plt.figure(figsize = (15, 8))
        plt.style.context("ggplot")
        subdf = line_df[(line_df["geo_value"] == geo)
                        & (line_df["lag"].isin([0, 15, 30, 45, 60, 75, 90]))]
        sns.lineplot(data=subdf, x="issue_date", y="value", hue="lag",
                     ci=95, err_style="band")
        plt.xlabel("Issue Date", fontsize=20)
        plt.ylabel(ylabel, fontsize=20)
        plt.title(fig_title, fontsize=25, loc="left")
        plt.legend(loc=legend_loc)
        startdate = subdf["issue_date"].min()
        enddate = subdf["issue_date"].max()
        n_days = (enddate - startdate).days +1
        time_index=[startdate + timedelta(days=i) for i in range(n_days)]
        selected_xtix = [x.weekday() == 6 for x in time_index]
        plt.xticks(np.array(time_index)[selected_xtix], fontsize=10, rotation=90)
        if data_type == "completeness":
            plt.axhline(90, linestyle = "--")
            plt.axhline(100, linestyle = "--") 
            vmax = (subdf["value"].max() // 10 + 1) * 10
            plt.yticks(np.linspace(0, vmax, num=int(vmax//20)+1), fontsize=10)
        else:
            plt.yticks([tanh(10*x) for x in [-1, -0.2, -0.1, -0.05, -0.02, 0,
                                             0.02, 0.05, 0.1, 0.2, 1]], 
                      ["<-100%", "-20%", "-10%", "-5%", "-2%", "0%",
                       "+2%", "+5%", "+10%", "+20%", ">+100%"])
        plt.savefig(save_dir+"/"+geo+".png", bbox_inches='tight')

def create_violinplot_by_lag(save_dir: str, backfill_df:pd.DataFrame,
                               source:str, start_date:datetime,
                               end_date:datetime, geo_values=[],
                               max_lag=90):
    """Create violinplots of the backfill estimates.

    The violinplots show the backfill estimates by lags and location across
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
    start_date : datetime
        Display data beginning on this date.
    end_date : datetime
        Display data up to this date, inclusive.
    geo_values : list of str, optional
        The list of locations for which the heatmaps will be created.
        The default is an empty list, which means all the locations included
        in the backfill_df will be considered. Otherwise, only the locations
        in the geo_values list will be considered.
    max_lag : int, optional
        The maximum lag that will be displayed in the heatmaps.
        The default is 90.

    Returns
    -------
    None.

    """
    check_create_dir(save_dir)

    if len(geo_values) == 0:
        geo_values = backfill_df["geo_value"].unique()
    max_lag =min(backfill_df["lag"].max(), max_lag)
    selected_lags = list(range(0, max_lag + 10, 10))

    line_df = backfill_df.loc[(backfill_df["lag"].isin(selected_lags))
                              & (backfill_df["time_value"]<=end_date)
                              & (backfill_df["time_value"]>=start_date)]
    data_type = line_df["data_type"].values[0]
    ref_lag = line_df["ref_lag"].values[0]
    value_type = line_df["value_type"].values[0]

    if data_type == "completeness":
        fig_title = 'Percentage of Completion, %s %s\
                \nAgainst values reported %d days later\
                \n%s, Reference Date: From %s to %s'%(source, value_type,
                ref_lag, "%s", start_date.date(), end_date.date())
        ylabel = "%Reported"
    else:      
        fig_title = 'Daily Change Rate, %s %s, \
                \n%s, Reference Date: From %s to %s'%(source, value_type, "%s",
                                          start_date.date(), end_date.date())
        ylabel = "Daily Change Rate"

    line_df = line_df[line_df["geo_value"].isin(geo_values)]

    for geo in line_df["geo_value"].unique():
        plt.figure(figsize = (10, 10))
        plt.style.context("ggplot")
        sublinedf = line_df[line_df["geo_value"] == geo]
        sns.violinplot(data=sublinedf, x="lag", y="value", cut=0)
        plt.xlabel("Lag", fontsize=20)
        plt.ylabel(ylabel, fontsize=20)
        plt.title(fig_title%geo, fontsize=25, loc="left")
        plt.savefig(save_dir+"/"+geo+".png", bbox_inches='tight')
