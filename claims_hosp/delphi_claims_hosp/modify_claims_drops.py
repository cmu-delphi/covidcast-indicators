#!/usr/bin/env python3

"""Modify the drops.

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


def modify_and_write(data_path, logger, force=True):
    """
    Modify drops given a folder path.

    Will rename necessary columns in the input files, and check the number of
    columns and duplications.

    Args:
      data_path: path to the folder with duplicated drops.
      force: if aggregated file exists, whether to overwrite or not

    """
    files = np.array(list(Path(data_path).glob("*.csv.gz")))
    dfs_list = []
    for f in files:
        filename = str(f)
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

        if force:
            dfs.to_csv(out_path, index=False)
            logger.info(f"Wrote {out_path}")
        else:
            dfs_list.append(dfs)
    return files, dfs_list
