# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module` that is executed
when the module is run with `python -m MODULE_NAME`.
"""

import datetime
import logging

from delphi_utils import read_params
from delphi_epidata import Epidata

from .pull_api import GoogleHealthTrends, get_counts_states, get_counts_dma
from .map_values import derived_counts_from_dma
from .export import export_csv

SIGNALS = ["raw_search", "smoothed_search"]


def run_module():
    """Main function run when calling the module.

    Inputs parameters from the file 'params.json' and produces output data in
    the directory defined by the `export_dir` (should be "receiving" expect for
    testing purposes).
    """

    #  read parameters
    params = read_params()
    ght_key = params["ght_key"]
    start_date = params["start_date"]
    end_date = params["end_date"]
    static_dir = params["static_file_dir"]
    export_dir = params["export_dir"]
    cache_dir = params["cache_dir"]
    wip_signal = params["wip_signal"]

    # if missing end_date, set to today (GMT) minus 5 days
    if end_date == "":
        now = datetime.datetime.now(datetime.timezone.utc)
        end_date = (now - datetime.timedelta(days=4)).strftime("%Y-%m-%d")

    # Turn on basic logging messages (level INFO)
    logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.INFO)
    logging.info("Creating data from %s through %s.", start_date, end_date)

    # setup class to handle API calls
    ght = GoogleHealthTrends(ght_key=ght_key)

    #  read data frame version of the data
    df_state = get_counts_states(
        ght, start_date, end_date, static_dir=static_dir, cache_dir=cache_dir
    )
    df_dma = get_counts_dma(
        ght, start_date, end_date, static_dir=static_dir, cache_dir=cache_dir
    )
    df_hrr, df_msa = derived_counts_from_dma(df_dma, static_dir=static_dir)

    signal_names = add_prefix(SIGNALS, wip_signal, prefix="wip_")
    prefix = "wip_"
    SMOOTHED = "smoothed_search"
    RAW = "raw_search"
    for signal in signal_names:
        if signal in (SMOOTHED, prefix + SMOOTHED):
            #  export each geographic region, with both smoothed and unsmoothed data
            export_csv(df_state, "state", signal, smooth=True, receiving_dir=export_dir)
            export_csv(df_dma, "dma", signal, smooth=True, receiving_dir=export_dir)
            export_csv(df_hrr, "hrr", signal, smooth=True, receiving_dir=export_dir)
            export_csv(df_msa, "msa", signal, smooth=True, receiving_dir=export_dir)
        elif signal in (RAW, prefix + RAW):
            export_csv(df_state, "state", signal, smooth=False, receiving_dir=export_dir)
            export_csv(df_dma, "dma", signal, smooth=False, receiving_dir=export_dir)
            export_csv(df_hrr, "hrr", signal, smooth=False, receiving_dir=export_dir)
            export_csv(df_msa, "msa", signal, smooth=False, receiving_dir=export_dir)


def add_prefix(signal_names, wip_signal, prefix: str):
    """Adds prefix to signal if there is a WIP signal
    Parameters
    ----------
    signal_names: List[str]
        Names of signals to be exported
    prefix : 'wip_'
        prefix for new/non public signals
    wip_signal : List[str] or bool
        Either takes a list of wip signals: [], OR
        incorporated all signals in the registry: True OR
        no signals: False
    Returns
    -------
    List of signal names
        wip/non wip signals for further computation
    """

    if wip_signal in ("", False):
        return signal_names
    elif wip_signal and isinstance(wip_signal, bool):
        return [
            (prefix + signal) if public_signal(signal)
            else signal
            for signal in signal_names
        ]
    elif isinstance(wip_signal, list):
        for signal in wip_signal:
            if public_signal(signal):
                signal_names.append(prefix + signal)
                signal_names.remove(signal)
        return signal_names
    else:
        raise ValueError("Supply True | False or '' or [] | list()")


def public_signal(signal_):
    """Checks if the signal name is already public using Epidata
    Parameters
     ----------
    signal_ : str
        Name of the signal
    Returns
    -------
    bool
        True if the signal is not present
        False if the signal is present
    """
    epidata_df = Epidata.covidcast_meta()
    for index in range(len(epidata_df['epidata'])):
        if 'signal' in epidata_df['epidata'][index]:
            if epidata_df['epidata'][index]['signal'] == signal_:
                return False
    return True
