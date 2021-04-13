#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 18 11:19:17 2021

@author: jingjingtang
"""

import pytest

from datetime import datetime, timedelta
from os import listdir, remove, system
from os.path import join, exists
import warnings

import numpy as np
import pandas as pd
from delphi_utils.backfill_profiler import (check_create_dir,
                                            to_covidcast_df,
                                            to_backfill_df,
                                            create_heatmap_by_refdate,
                                            create_lineplot_by_loations,
                                            create_lineplot_by_issuedate,
                                            create_violinplot_by_lag)

warnings.filterwarnings("ignore")

def _clean_directory(directory):
    """Clean files out of a directory."""
    for fname in listdir(directory):
        if fname.startswith("."):
            continue
        remove(join(directory, fname))


def _non_ignored_files_set(directory):
    """List all files in a directory not preceded by a '.' and store them in a set."""
    out = set()
    for fname in listdir(directory):
        if fname.startswith("."):
            continue
        out.add(fname)
    return out

timestamps = [datetime(2020, 3, 20) + timedelta(days=i) for i in range(56)]
geo_values = ["ny", "pa", "ma"]
lags = list(range(91))
index = pd.MultiIndex.from_product([timestamps, geo_values, lags], 
                                   names = ["time_value", "geo_value", "lag"])
        
backfill_df = pd.DataFrame(index=index).reset_index()
backfill_df["value"] = np.random.random_sample(backfill_df.shape[0]) * 1e3
backfill_df["value_type"] = "Total counts"
backfill_df["data_type"] = "completeness"
backfill_df["ref_lag"] = 60
    
source = "chng"
start_date = timestamps[0]
end_date = timestamps[-1]

class TestExport:
    
    
    
    def test_check_create_dir(self):
        save_dir = "./test_output"
        check_create_dir(save_dir)
        
        assert exists(save_dir)
        system("rm -r %s"%save_dir)
    
    def test_to_covidcast_df(self):
        
        # Valid case
        test_df = pd.DataFrame({"timestamp": [datetime(2021, 1, 1)],
                                "geos": ["pa"],
                                "totalcounts": [100],
                                "gap": [2],
                                "covidcounts": [2]})
        df = to_covidcast_df(test_df, lag_col="gap", 
                             time_value_col="timestamp", 
                             geo_value_col="geos", 
                             sample_size_col="totalcounts", 
                             value_col="covidcounts")
        assert set(["time_value", "geo_value", "sample_size", "lag", "value"]).issubset(set(df.columns))
        
        # Invalid case 1
        test_df = pd.DataFrame({"timestamp": ["2021-1-1"],
                                "geos": ["pa"],
                                "totalcounts": [100],
                                "gap": [2],
                                "covidcounts": [2]})
        with pytest.raises(ValueError):
            to_covidcast_df(test_df, lag_col="gap", 
                            time_value_col="timestamp", 
                            geo_value_col="geos", 
                            sample_size_col="totalcounts", 
                            value_col="covidcounts")
        
        # Invalid case 2
        test_df = pd.DataFrame({"timestamp": [datetime(2021, 1, 1)],
                                "geos": ["pa"],
                                "totalcounts": [100],
                                "gap": ["c"],
                                "covidcounts": [2]})
        with pytest.raises(ValueError):
            to_covidcast_df(test_df, lag_col="gap", 
                            time_value_col="timestamp", 
                            geo_value_col="geos", 
                            sample_size_col="totalcounts", 
                            value_col="covidcounts")
    
    def test_to_backfill_df(self):
        
        timestamps = [datetime(2020, 3, 20) + timedelta(days=i) for i in range(56)]
        geo_values = ["ny", "pa", "ma"]
        lags = list(range(91))
        index = pd.MultiIndex.from_product([timestamps, geo_values, lags], 
                                           names = ["time_value", "geo_value", "lag"])
        
        df = pd.DataFrame(index=index).reset_index()
        df["sample_size"] = np.random.random_sample(df.shape[0]) * 1e5
        df["sample_size"] = df["sample_size"].astype(int)
        df["value"] = np.random.random_sample(df.shape[0])
        
        # Too large ref_lag
        with pytest.raises(ValueError):
            to_backfill_df(df, data_type = "completeness",
                       value_type="total_count", ref_lag=100)
         
        # Normal case for completeness
        test_backfill_df = to_backfill_df(df, data_type = "completeness",
                       value_type="total_count", ref_lag=60)
        
        assert (test_backfill_df.loc[test_backfill_df["lag"] == 60, "value"] == 100).all()
    
    def test_create_heatmap_by_refdate(self):
        save_dir = "./test_dir"
        _clean_directory(save_dir)
        create_heatmap_by_refdate(save_dir, backfill_df, source, 
                                    start_date, end_date, 
                                    geo_values=None, max_lag=90)
        
        figs = _non_ignored_files_set(save_dir)
        assert len(figs) == backfill_df["geo_value"].nunique()
        
        _clean_directory(save_dir)
        
    def create_lineplot_by_issuedate(self):
        save_dir = "./test_dir"
        _clean_directory(save_dir)
        create_lineplot_by_issuedate(save_dir, backfill_df, source, 
                                    start_date, end_date, 
                                    geo_values=None, max_lag=90)
        
        figs = _non_ignored_files_set(save_dir)
        assert len(figs) == backfill_df["geo_value"].nunique()
        
        _clean_directory(save_dir)
    
    def test_create_lineplot_by_loations(self):
        save_dir = "./test_dir"
        _clean_directory(save_dir)
        create_lineplot_by_loations(save_dir, backfill_df, 
                                    source, fig_name="test", 
                                    start_date=start_date, 
                                    end_date=end_date, 
                                    geo_values=None, max_lag=90)
        
        figs = _non_ignored_files_set(save_dir)
        assert "test.png" in figs
        
        _clean_directory(save_dir)
    
        
    def test_create_violinplot_by_lag(self):
        save_dir = "./test_dir"
        _clean_directory(save_dir)
        create_violinplot_by_lag(save_dir, backfill_df, source, start_date, 
                                 end_date, geo_values=None, max_lag=90)
        
        figs = _non_ignored_files_set(save_dir)
        assert len(figs) == backfill_df["geo_value"].nunique()
        
        _clean_directory(save_dir)
