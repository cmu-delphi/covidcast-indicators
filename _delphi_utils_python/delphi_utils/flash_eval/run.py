# -*- coding: utf-8 -*-
"""Functions to call when running the tool.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_utils.flash_eval`.
"""
from datetime import date
import pandas as pd
from .. import read_params
from .eval_day import flash_eval
from ..validator.datafetcher import read_filenames, load_csv

def run_module():
    """Run the FlaSH module.

    The parameters dictionary must include the signals and signals.
    We are only considering lag-1 data.
    """
    params = read_params()
    signals = params["flash"]["signals"]
    for signal in signals:
        export_files = read_filenames(params["common"]["export_dir"])
        days = {}
        #Concat the data from recent files at nation, state, and county resolution per day.
        for (x, _) in export_files:
            if signal in x and pd.Series([y in x for y in ['state', 'county', 'nation']]).any():
                day = pd.to_datetime(x.split('_')[0], format="%Y%m%d", errors='raise')
                days[day] = pd.concat([days.get(day, pd.DataFrame()),
                                       load_csv(f"{params['common']['export_dir']}/{x}")])
        for day, input_df in days.items():
            input_df = input_df[['geo_id', 'val']].set_index('geo_id').T
            input_df.index = [day]
            today = date.today()
            lag= (pd.to_datetime(today)-pd.to_datetime(day)).days
            #test case for inital flash implementation assume lag == 1
            if lag  in params["flash"]["lags"]:
                flash_eval(lag, day, input_df, signal, params)
