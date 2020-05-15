# -*- coding: utf-8 -*-
"""Functions to aggregate DMA regions to HRR and MSA regions.
"""
from typing import Tuple

import numpy as np
import pandas as pd


def derived_counts_from_dma(
    df_dma: pd.DataFrame, static_dir: str
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Compute derived HRR and MSA counts as a weighted sum of the DMA dataset.

    Parameters
    ----------
    df_dma: pd.DataFrame
        a data frame with columns "geo_id", "timestamp", and "val"
    static_dir: str
        path to location where static metadata files are stored

    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame]
        Two data frames, each with columns "geo_id", "timestamp", and "val".
        The first is a data frame for HRR regions and the second are MSA
        regions.
    """

    dma_mat, day_list = _dma_df_to_matrix(df_dma, static_dir)

    # compute HRR using the crosswalk file
    hrr_list = np.loadtxt(f"{static_dir}/Canonical_HRR.txt", dtype=int)
    dma_to_hrr = np.loadtxt(f"{static_dir}/MapMatrix_HRR_DMA.txt")
    hrr_mat = np.dot(dma_to_hrr, dma_mat)

    df_hrr = pd.DataFrame(
        dict(
            geo_id=np.repeat(hrr_list, len(day_list)),
            timestamp=np.tile(day_list, len(hrr_list)),
            val=hrr_mat.flatten(),
        )
    ).sort_values(["geo_id", "timestamp"])

    # compute MSA using the crosswalk file
    msa_list = np.loadtxt(f"{static_dir}/Canonical_MSA.txt", dtype=int)
    dma_to_msa = np.loadtxt(f"{static_dir}/MapMatrix_MSA_DMA.txt")
    msa_mat = np.dot(dma_to_msa, dma_mat)

    df_msa = pd.DataFrame(
        dict(
            geo_id=np.repeat(msa_list, len(day_list)),
            timestamp=np.tile(day_list, len(msa_list)),
            val=msa_mat.flatten(),
        )
    ).sort_values(["geo_id", "timestamp"])

    return df_hrr, df_msa


def _dma_df_to_matrix(
    df_dma: pd.DataFrame, static_dir: str
) -> Tuple[np.ndarray, np.ndarray]:
    """Covert data frame into a numpy array.

    Parameters
    ----------
    df_dma: pd.DataFrame
        a data frame with columns "geo_id", "timestamp", and "val"
    static_dir: str
        path to location where static metadata files are stored

    Returns
    -------
    Tuple[np.ndarray, np.ndarray]
        The first value is a matrix with one row for each DMA region and one
        column for each day, with the values filled in from the data frame.
        The second value is an array of the column names (the dates). The
        row names are not needed as they are the canonical names from the
        file "Canonical_DMA.txt".
    """

    dma_list = np.loadtxt(f"{static_dir}/Canonical_DMA.txt", dtype=int)
    day_list = df_dma["timestamp"].unique()

    mat = np.zeros((len(dma_list), len(day_list)))
    for idx, dma in enumerate(dma_list):
        for idy, day in enumerate(day_list):
            vals = df_dma[(df_dma["geo_id"] == dma) & (df_dma["timestamp"] == day)][
                "val"
            ].values
            if len(vals) == 1:
                mat[idx, idy] = vals
            elif len(vals) > 1:
                raise ValueError(f"Multiple values for day={day} and dma={dma}")
            else:
                raise ValueError(f"Missing values for day={day} and dma={dma}")

    return mat, day_list
