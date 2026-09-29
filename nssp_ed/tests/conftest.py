import copy
import json
from pathlib import Path
from unittest.mock import patch as mock_patch

import pandas as pd
import pytest

from delphi_nssp_ed import run as _run  # noqa: F401  (ensures package imports)

TEST_DIR = Path(__file__).parent

# Static fixture: real rows from the public feed
# (https://data.cdc.gov/resource/vjzj-u7u8.json), 2026-09-05..2026-09-19,
# saved so tests never touch the network.
with open(f"{TEST_DIR}/test_data/page.json", "r") as f:
    TEST_DATA = json.load(f)


@pytest.fixture(scope="session")
def params():
    params = {
        "common": {
            "export_dir": f"{TEST_DIR}/receiving",
            "log_filename": f"{TEST_DIR}/test.log",
            "backup_dir": f"{TEST_DIR}/test_raw_data_backups",
            "custom_run": False,
        },
        "indicator": {
            "wip_signal": True,
            "export_start_date": "2026-09-05",
            "socrata_token": "",
        },
        "validation": {
            "common": {
                "span_length": 14,
                "min_expected_lag": {"all": "7"},
                "max_expected_lag": {"all": "14"},
            }
        },
    }
    return copy.deepcopy(params)


@pytest.fixture()
def socrata_mock():
    """Mock the Socrata API to serve the static fixture, then an empty page."""
    with mock_patch("delphi_nssp_ed.pull.Socrata") as mock_client_cls:
        mock_client_cls.return_value.get.side_effect = [TEST_DATA, []]
        yield mock_client_cls.return_value.get


@pytest.fixture()
def pulled_df(socrata_mock):
    """Conformed dataframe produced from the fixture via the real pull code."""
    from delphi_nssp_ed.pull import pull_nssp_ed_data

    return pull_nssp_ed_data(
        backup_dir=str(TEST_DIR / "test_raw_data_backups"),
        custom_run=True,
        source_df=pd.DataFrame.from_records(TEST_DATA),
    )


def cleanup(params):
    export_dir = Path(params["common"]["export_dir"])
    for file in export_dir.glob("*.csv"):
        file.unlink()
    backup_dir = Path(params["common"]["backup_dir"])
    if backup_dir.exists():
        for file in backup_dir.glob("*"):
            if not file.name.startswith("."):
                file.unlink()
