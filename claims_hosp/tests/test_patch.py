import os
import shutil
from datetime import datetime
from unittest.mock import MagicMock, patch as mock_patch

import pytest

from delphi_claims_hosp.patch import (good_patch_config, log_patch_size,
                                      output_dates_per_issue, patch)

from conftest import TEST_DIR


class TestGoodPatchConfig:

    def test_good_config(self, params_w_patch):
        assert good_patch_config(params_w_patch, MagicMock())

    def test_missing_custom_run_flag(self, params_w_patch):
        params_w_patch["common"]["custom_run"] = False
        assert not good_patch_config(params_w_patch, MagicMock())

    def test_missing_patch_section(self, params):
        params["common"]["custom_run"] = True
        assert not good_patch_config(params, MagicMock())

    def test_missing_patch_keys(self, params_w_patch):
        del params_w_patch["patch"]["patch_dir"]
        assert not good_patch_config(params_w_patch, MagicMock())

    def test_drop_date_set(self, params_w_patch):
        params_w_patch["indicator"]["drop_date"] = "2020-06-11"
        assert not good_patch_config(params_w_patch, MagicMock())

    def test_bad_issue_date_format(self, params_w_patch):
        params_w_patch["patch"]["start_issue"] = "06-11-2020"
        assert not good_patch_config(params_w_patch, MagicMock())

    def test_start_issue_after_end_issue(self, params_w_patch):
        params_w_patch["patch"]["end_issue"] = "2020-06-10"
        assert not good_patch_config(params_w_patch, MagicMock())


class TestPatchSize:

    def test_window_from_n_backfill_days(self, params_w_patch):
        # start_date null, so the window is n_backfill_days deep
        assert output_dates_per_issue(params_w_patch, datetime(2026, 9, 8)) == 70

    def test_start_date_overrides_the_window(self, params_w_patch):
        # what the prod template sets; run.py applies it after n_backfill_days
        params_w_patch["indicator"]["start_date"] = "2020-02-01"
        assert output_dates_per_issue(params_w_patch, datetime(2026, 9, 8)) == 2408

    def test_reports_size_without_warning(self, params_w_patch):
        logger = MagicMock()
        log_patch_size(params_w_patch, datetime(2026, 9, 8), datetime(2026, 9, 10), logger)

        # 1 geo x 1 weekday setting x 2 numerators x 70 dates x 3 issues
        assert logger.info.call_args.kwargs == {
            "issue_count": 3, "dates_per_issue": 70, "estimated_csv_count": 420}
        logger.warning.assert_not_called()

    def test_warns_when_start_date_widens_the_window(self, params_w_patch):
        params_w_patch["indicator"]["start_date"] = "2020-02-01"
        logger = MagicMock()
        log_patch_size(params_w_patch, datetime(2026, 9, 8), datetime(2026, 9, 10), logger)

        logger.warning.assert_called_once()
        assert logger.warning.call_args.kwargs["estimated_csv_count"] > 14000


class TestPatchModule:

    def test_patch(self, params_w_patch):
        with mock_patch("delphi_claims_hosp.patch.get_structured_logger"), \
                mock_patch("delphi_claims_hosp.patch.read_params") as mock_read_params, \
                mock_patch("delphi_claims_hosp.run.download") as mock_download, \
                mock_patch("delphi_claims_hosp.run.modify_and_write") as mock_modify:
            mock_read_params.return_value = params_w_patch

            patch()

            # the drop is pulled for the issue date, not for today
            mock_download.assert_called_once()
            assert mock_download.call_args.kwargs["issue_date"] == "2020-06-11"
            # and only that drop is aggregated, not the whole staging dir
            assert mock_modify.call_count == 1
            assert mock_modify.call_args.kwargs["filepaths"][0].name == \
                "SYNEDI_AGG_INPATIENT_11062020_1451CDT.csv.gz"

            issue_dir = f"{TEST_DIR}/patch_dir/issue_20200611/hospital-admissions"
            assert os.path.isdir(issue_dir)
            assert len(os.listdir(issue_dir)) > 0

            # the drops survive the run, later issues in the range still need them
            assert "SYNEDI_AGG_INPATIENT_11062020_1451CDT.csv.gz" in \
                os.listdir(params_w_patch["indicator"]["input_dir"])

        shutil.rmtree(params_w_patch["patch"]["patch_dir"])

    def test_patch_skips_issue_without_drop(self, params_w_patch):
        # no drop for 06-12 on the server or on disk, so the issue is skipped
        # rather than failing the rest of the run
        params_w_patch["patch"]["start_issue"] = "2020-06-12"
        params_w_patch["patch"]["end_issue"] = "2020-06-12"
        with mock_patch("delphi_claims_hosp.patch.get_structured_logger"), \
                mock_patch("delphi_claims_hosp.patch.read_params") as mock_read_params, \
                mock_patch("delphi_claims_hosp.run.download"):
            mock_read_params.return_value = params_w_patch

            patch()

            # nothing to generate, so no issue directory is left behind
            assert not os.path.isdir(f"{TEST_DIR}/patch_dir/issue_20200612")

        shutil.rmtree(params_w_patch["patch"]["patch_dir"])

    def test_patch_exits_on_bad_config(self, params):
        with mock_patch("delphi_claims_hosp.patch.get_structured_logger"), \
                mock_patch("delphi_claims_hosp.patch.read_params") as mock_read_params:
            mock_read_params.return_value = params
            with pytest.raises(SystemExit):
                patch()
