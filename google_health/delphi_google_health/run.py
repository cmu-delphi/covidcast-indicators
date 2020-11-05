# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module` that is executed
when the module is run with `python -m MODULE_NAME`.
"""

import datetime
import logging

from delphi_utils import (
    read_params,
    S3ArchiveDiffer,
    add_prefix
)

from .pull_api import GoogleHealthTrends, get_counts_states, get_counts_dma
from .map_values import derived_counts_from_dma
from .export import export_csv
from .constants import (SIGNALS, RAW, SMOOTHED,
                        MSA, HRR, STATE, DMA,
                        PULL_START_DATE)


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
        ght, PULL_START_DATE, end_date, static_dir=static_dir, data_dir=data_dir
    )
    df_dma = get_counts_dma(
        ght, PULL_START_DATE, end_date, static_dir=static_dir, data_dir=data_dir
    )
    df_hrr, df_msa = derived_counts_from_dma(df_dma, static_dir=static_dir)

    signal_names = add_prefix(SIGNALS, wip_signal, prefix="wip_")

    for signal in signal_names:
        if signal.endswith(SMOOTHED):
            # export each geographic region, with both smoothed and unsmoothed data
            export_csv(df_state, STATE, signal, smooth=True,
                       start_date=start_date, receiving_dir=export_dir)
            export_csv(df_dma, DMA, signal, smooth=True,
                       start_date=start_date, receiving_dir=export_dir)
            export_csv(df_hrr, HRR, signal, smooth=True,
                       start_date=start_date, receiving_dir=export_dir)
            export_csv(df_msa, MSA, signal, smooth=True,
                       start_date = start_date, receiving_dir=export_dir)
        elif signal.endswith(RAW):
            export_csv(df_state, STATE, signal, smooth=False,
                       start_date=start_date, receiving_dir=export_dir)
            export_csv(df_dma, DMA, signal, smooth=False,
                       start_date=start_date, receiving_dir=export_dir)
            export_csv(df_hrr, HRR, signal, smooth=False,
                       start_date=start_date, receiving_dir=export_dir)
            export_csv(df_msa, MSA, signal, smooth=False,
                       start_date=start_date, receiving_dir=export_dir)
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
