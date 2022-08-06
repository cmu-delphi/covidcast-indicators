#!/usr/bin/env python3

"""Modify the drops.

Drops are expected to be numbered as:

../EDI_AGG_INPATIENT/EDI_AGG_INPATIENT_1_07052020_1456.csv.gz
../EDI_AGG_INPATIENT/EDI_AGG_INPATIENT_2_07052020_1456.csv.gz
... etc.
"""
# third party
import numpy as np
import pandas as pd


def modify_and_write(f, logger, test_mode=False):
    """
    Modify drops given a folder path.

    Will rename necessary columns in the input files, and check the number of
    columns and duplications.

    Args:
      data_path: path to the file to be modified.
      test_mode: Don't overwrite the drops if test_mode==True

    """
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

    if not test_mode:
        dfs.to_csv(out_path, index=False)
        logger.info(f"Wrote {out_path}")
    return dfs
