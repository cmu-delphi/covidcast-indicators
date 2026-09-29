"""Tests for delphi_nssp_ed.patch."""

import gzip
import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from delphi_nssp_ed.patch import find_backup_for_issue, patch

from conftest import TEST_DATA, cleanup

TEST_DIR = Path(__file__).parent

PATCH_PARAMS = {
    "common": {
        "export_dir": str(TEST_DIR / "receiving"),
        "log_filename": str(TEST_DIR / "test.log"),
        "backup_dir": str(TEST_DIR / "test_patch_backups"),
        "custom_run": True,
    },
    "indicator": {
        "wip_signal": True,
        "export_start_date": "2026-09-05",
        "socrata_token": "",
    },
    "patch": {
        "patch_dir": str(TEST_DIR / "test_patch_output"),
        "start_issue": "2026-09-12",
        "end_issue": "2026-09-13",
    },
}


def write_backup(backup_dir: Path, yyyymmdd: str):
    backup_dir.mkdir(parents=True, exist_ok=True)
    path = backup_dir / f"{yyyymmdd}.csv.gz"
    df = pd.DataFrame.from_records(TEST_DATA)
    with gzip.open(path, "wt") as f:
        df.to_csv(f, index=False)
    return path


class TestFindBackupForIssue:
    def test_picks_latest_backup_on_or_before_issue(self, tmp_path):
        write_backup(tmp_path, "20260910")
        write_backup(tmp_path, "20260915")
        (tmp_path / "notes.txt").write_text("ignore me")
        found = find_backup_for_issue(str(tmp_path), datetime(2026, 9, 12))
        assert found is not None and found.endswith("20260910.csv.gz")
        found = find_backup_for_issue(str(tmp_path), datetime(2026, 9, 15))
        assert found.endswith("20260915.csv.gz")

    def test_none_when_no_backup_yet(self, tmp_path):
        write_backup(tmp_path, "20260915")
        assert find_backup_for_issue(str(tmp_path), datetime(2026, 9, 10)) is None


class TestPatch:
    def test_patch_issues(self, tmp_path):
        import copy

        backup_dir = tmp_path / "backups"
        write_backup(backup_dir, "20260911")

        params = copy.deepcopy(PATCH_PARAMS)
        params["common"]["backup_dir"] = str(backup_dir)
        params["common"]["export_dir"] = str(tmp_path / "receiving")
        params["patch"]["patch_dir"] = str(tmp_path / "patch_out")

        patch(params)

        # one issue dir per issue date, in batch-issue format
        for issue in ["20260912", "20260913"]:
            issue_dir = tmp_path / "patch_out" / f"issue_{issue}" / "nssp-ed"
            assert issue_dir.is_dir()
            csvs = list(issue_dir.glob("*.csv"))
            assert len(csvs) > 0
            # no record in any export may reference a date after the issue date
            for csv in csvs:
                date_part = csv.name.split("_")[0]
                assert date_part <= issue

        # the later issue contains strictly more reference dates
        def dates_in(issue):
            d = tmp_path / "patch_out" / f"issue_{issue}" / "nssp-ed"
            return {f.name.split("_")[0] for f in d.glob("*_nation_*.csv")}

        assert max(dates_in("20260912")) < max(dates_in("20260913"))

    def test_patch_skips_issue_without_backup(self, tmp_path, caplog):
        import copy

        params = copy.deepcopy(PATCH_PARAMS)
        params["common"]["backup_dir"] = str(tmp_path / "empty_backups")
        (tmp_path / "empty_backups").mkdir()
        params["common"]["export_dir"] = str(tmp_path / "receiving")
        params["patch"]["patch_dir"] = str(tmp_path / "patch_out")
        params["patch"]["start_issue"] = "2026-09-12"
        params["patch"]["end_issue"] = "2026-09-12"

        patch(params)  # should not raise
        assert not (tmp_path / "patch_out" / "issue_20260912").exists()
