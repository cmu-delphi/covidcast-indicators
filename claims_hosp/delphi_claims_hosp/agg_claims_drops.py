#!/usr/bin/env python3

"""Aggregates chunks of drops.

Drops are expected to be numbered as:

../EDI_AGG_INPATIENT/EDI_AGG_INPATIENT_1_07052020_1456.csv.gz
../EDI_AGG_INPATIENT/EDI_AGG_INPATIENT_2_07052020_1456.csv.gz
... etc.
"""

# standard
from pathlib import Path

# third party
import numpy as np
import pandas as pd


def agg_and_write(data_path, logger):
    """
    Aggregate drops given a folder path.

    Will output an aggregated version in the same folder. Example below.

    Input files in folder:
      ../EDI_AGG_INPATIENT/EDI_AGG_INPATIENT_1_07052020_1456.csv.gz
      ../EDI_AGG_INPATIENT/EDI_AGG_INPATIENT_2_07052020_1456.csv.gz

    Will create:
     ../EDI_AGG_INPATIENT/EDI_AGG_INPATIENT_07052020_1456.csv.gz

    Args:
      data_path: path to the folder with duplicated drops.
      force: if aggregated file exists, whether to overwrite or not

    """
    files = np.array(list(Path(data_path).glob("*")))

    for f in files:
        filename = str(f)
        if ".csv.gz" not in filename:
            continue
        out_path = f.parents[0] / f.name
        dfs = pd.read_csv(f, dtype={"PatCountyFIPS": str,
                                    "patCountyFIPS": str})
        if "servicedate" in dfs.columns:
            dfs.rename(columns={"servicedate": "ServiceDate"}, inplace=True)
        if "patCountyFIPS" in dfs.columns:
            dfs.rename(columns={"patCountyFIPS": "PatCountyFIPS"}, inplace=True)
        if "patHRRname" in dfs.columns:
            dfs.rename(columns={"patHRRname": "Pat HRR Name"}, inplace=True)
        if "patAgeGroup" in dfs.columns:
            dfs.rename(columns={"patAgeGroup": "PatAgeGroup"}, inplace=True)
        if "patHRRid" in dfs.columns:
            dfs.rename(columns={"patHRRid": "Pat HRR ID"}, inplace=True)

        assert np.sum(
            dfs.duplicated(subset=["ServiceDate", "PatCountyFIPS",
                                   "Pat HRR Name", "PatAgeGroup"])) == 0, \
            f'Duplication across drops in {filename}!'
        assert dfs.shape[1] == 10, f'Wrong number of columns in {filename}'

        dfs.to_csv(out_path, index=False)
        logger.info(f"Wrote {out_path}")
