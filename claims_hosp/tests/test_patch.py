import os
import shutil
from unittest.mock import MagicMock, patch as mock_patch

import pytest

from delphi_claims_hosp.patch import good_patch_config, patch

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
