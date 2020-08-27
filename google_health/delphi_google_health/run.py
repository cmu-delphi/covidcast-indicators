# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module` that is executed
when the module is run with `python -m MODULE_NAME`.
"""

import datetime
import logging

from delphi_utils import (
    read_params,
    S3ArchiveDiffer
)

import covidcast
from .pull_api import GoogleHealthTrends, get_counts_states, get_counts_dma
from .map_values import derived_counts_from_dma
from .export import export_csv
from .constants import SIGNALS, RAW, SMOOTHED, MSA, HRR, STATE, DMA


def run_module():
    """Main function run when calling the module.

    Inputs parameters from the file 'params.json' and produces output data in
    the directory defined by the `export_dir` (should be "receiving" expect for
    testing purposes).
    """

    # read parameters
    params = read_params()
    ght_key = params["ght_key"]
    start_date = params["start_date"]
    end_date = params["end_date"]
    static_dir = params["static_file_dir"]
    export_dir = params["export_dir"]
    data_dir = params["data_dir"]
    wip_signal = params["wip_signal"]
    cache_dir = params["cache_dir"]

    arch_diff = S3ArchiveDiffer(
        cache_dir, export_dir,
        params["bucket_name"], "ght",
        params["aws_credentials"])
    arch_diff.update_cache()
    print(arch_diff)
    # if missing start_date, set to today (GMT) minus 5 days
    if start_date == "":
        now = datetime.datetime.now(datetime.timezone.utc)
        start_date = (now - datetime.timedelta(days=4)).strftime("%Y-%m-%d")

    # if missing start_date, set to today (GMT) minus 5 days
    if start_date == "":
        now = datetime.datetime.now(datetime.timezone.utc)
        start_date = (now - datetime.timedelta(days=4)).strftime("%Y-%m-%d")

    # if missing end_date, set to today (GMT) minus 5 days
    if end_date == "":
        now = datetime.datetime.now(datetime.timezone.utc)
        end_date = (now - datetime.timedelta(days=4)).strftime("%Y-%m-%d")

    # Turn on basic logging messages (level INFO)
    logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.INFO)
    logging.info("Creating data from %s through %s.", start_date, end_date)

    # setup class to handle API calls
    ght = GoogleHealthTrends(ght_key=ght_key)

    # read data frame version of the data
    df_state = get_counts_states(
        ght, start_date, end_date, static_dir=static_dir, data_dir=data_dir
    )
    df_dma = get_counts_dma(
        ght, start_date, end_date, static_dir=static_dir, data_dir=data_dir
    )
    df_hrr, df_msa = derived_counts_from_dma(df_dma, static_dir=static_dir)

    signal_names = add_prefix(SIGNALS, wip_signal, prefix="wip_")

    for signal in signal_names:
        if signal.endswith(SMOOTHED):
            # export each geographic region, with both smoothed and unsmoothed data
            export_csv(df_state, STATE, signal, smooth=True, receiving_dir=export_dir)
            export_csv(df_dma, DMA, signal, smooth=True, receiving_dir=export_dir)
            export_csv(df_hrr, HRR, signal, smooth=True, receiving_dir=export_dir)
            export_csv(df_msa, MSA, signal, smooth=True, receiving_dir=export_dir)
        elif signal.endswith(RAW):
            export_csv(df_state, STATE, signal, smooth=False, receiving_dir=export_dir)
            export_csv(df_dma, DMA, signal, smooth=False, receiving_dir=export_dir)
            export_csv(df_hrr, HRR, signal, smooth=False, receiving_dir=export_dir)
            export_csv(df_msa, MSA, signal, smooth=False, receiving_dir=export_dir)
    # Diff exports, and make incremental versions
    _, common_diffs, new_files = arch_diff.diff_exports()

    # Archive changed and new files only
    to_archive = [f for f, diff in common_diffs.items() if diff is not None]
    to_archive += new_files
    _, fails = arch_diff.archive_exports(to_archive)

    # Filter existing exports to exclude those that failed to archive
    succ_common_diffs = {f: diff for f, diff in common_diffs.items() if f not in fails}
    arch_diff.filter_exports(succ_common_diffs)

    # Report failures: someone should probably look at them
    for exported_file in fails:
        print(f"Failed to archive '{exported_file}'")


def add_prefix(signal_names, wip_signal, prefix):
    """Adds prefix to signal if there is a WIP signal
    Parameters
    ----------
    signal_names: List[str]
        Names of signals to be exported
    prefix : 'wip_'
        prefix for new/non public signals
    wip_signal : List[str] or bool
        a list of wip signals: [], OR
        all signals in the registry: True OR
        only signals that have never been published: False
    Returns
    -------
    List of signal names
        wip/non wip signals for further computation
    """

    if wip_signal is True:
        return [prefix + signal for signal in signal_names]
    if isinstance(wip_signal, list):
        make_wip = set(wip_signal)
        return [
            (prefix if signal in make_wip else "") + signal
            for signal in signal_names
        ]
    if wip_signal in {False, ""}:
        return [
            signal if public_signal(signal)
            else prefix + signal
            for signal in signal_names
        ]
    raise ValueError("Supply True | False or '' or [] | list()")


def public_signal(signal_):
    """Checks if the signal name is already public using COVIDcast
    Parameters
    ----------
    signal_ : str
        Name of the signal
    Returns
    -------
    bool
        True if the signal is present
        False if the signal is not present
    """
    epidata_df = covidcast.metadata()
    for index in range(len(epidata_df)):
        if epidata_df['signal'][index] == signal_:
            return True
    return False
