import pytest

from os.path import join

import pandas as pd
import numpy as np

from delphi_google_health.map_values import derived_counts_from_dma, _dma_df_to_matrix


class TestMapValues:
    def test_dummy_hrr(self):

        # Create a dummy dataset
        static_dir = join("..", "static")
        dma_list = np.loadtxt(join(static_dir, "Canonical_DMA.txt"), dtype=int)
        val = np.zeros(len(dma_list) * 2)
        val[0] = 2
        val[2] = 10
        df_dma = pd.DataFrame(
            {
                "geo_id": np.tile(dma_list, 2),
                "timestamp": np.repeat(["2020-02-03", "2020-02-04"], len(dma_list)),
                "val": val,
            }
        )

        df_hrr, _ = derived_counts_from_dma(df_dma, static_dir)

        hrr_list = np.loadtxt(join(static_dir, "Canonical_HRR.txt"), dtype=int)
        assert set(np.argwhere(df_hrr["val"].values > 0).flatten()) == set(
            [254, 256, 348, 350, 368, 370, 382, 384, 470]
        )
        assert set(df_hrr["geo_id"].unique()) == set(hrr_list)
        assert (df_hrr["timestamp"].unique() == ["2020-02-03", "2020-02-04"]).all()

    def test_dummy_msa(self):

        # Create a dummy dataset
        static_dir = join("..", "static")
        dma_list = np.loadtxt(join(static_dir, "Canonical_DMA.txt"), dtype=int)
        val = np.zeros(len(dma_list) * 2)
        val[0] = 2
        val[2] = 10
        df_dma = pd.DataFrame(
            {
                "geo_id": np.tile(dma_list, 2),
                "timestamp": np.repeat(["2020-02-03", "2020-02-04"], len(dma_list)),
                "val": val,
            }
        )

        _, df_msa = derived_counts_from_dma(df_dma, static_dir)

        msa_list = np.loadtxt(join(static_dir, "Canonical_MSA.txt"), dtype=int)
        assert set(np.argwhere(df_msa["val"].values > 0).flatten()) == set(
            [68, 400, 546, 674]
        )
        assert set(df_msa["geo_id"].unique()) == set(msa_list)
        assert (df_msa["timestamp"].unique() == ["2020-02-03", "2020-02-04"]).all()


class TestDataToMatrix:
    def test_matrix_format(self):

        # Create a dummy dataset
        static_dir = join("..", "static")
        dma_list = np.loadtxt(join(static_dir, "Canonical_DMA.txt"), dtype=int)
        val = np.zeros(len(dma_list) * 2)
        val[0] = 2
        val[2] = 10
        df_dma = pd.DataFrame(
            {
                "geo_id": np.tile(dma_list, 2),
                "timestamp": np.repeat(["2020-02-03", "2020-02-04"], len(dma_list)),
                "val": val,
            }
        )

        #  create matrix
        mat, day_list = _dma_df_to_matrix(df_dma, static_dir)

        #  check out
        assert mat.shape == (len(dma_list), 2)
        assert (day_list == ["2020-02-03", "2020-02-04"]).all()
        assert mat[0, 0] == 2
        assert mat[2, 0] == 10
        assert mat.sum() == 12
        assert mat.min() == 0

    def test_multiple_values(self):

        # Create a dummy dataset
        static_dir = join("..", "static")
        dma_list = np.loadtxt(join(static_dir, "Canonical_DMA.txt"), dtype=int)
        val = np.zeros(len(dma_list) * 2)
        val[0] = 2
        val[2] = 10
        df_dma = pd.DataFrame(
            {
                "geo_id": np.tile(dma_list, 2),
                "timestamp": np.repeat(["2020-02-03", "2020-02-03"], len(dma_list)),
                "val": val,
            }
        )

        with pytest.raises(ValueError) as e_info:
            mat, day_list = _dma_df_to_matrix(df_dma, static_dir)

    def test_missing_values(self):

        # Create a dummy dataset
        static_dir = join("..", "static")
        df_dma = pd.DataFrame(
            {"geo_id": [500], "timestamp": ["2020-02-03"], "val": [0]}
        )

        with pytest.raises(ValueError) as e_info:
            mat, day_list = _dma_df_to_matrix(df_dma, static_dir)
