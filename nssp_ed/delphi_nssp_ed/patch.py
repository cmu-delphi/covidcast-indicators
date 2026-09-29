# -*- coding: utf-8 -*-
"""Patching module for the nssp_ed indicator.

To use this module, specify the range of issue dates in params.json, like so:

{
  "common": {
    ...
    "custom_run": true
  },
  "validation": {
    ...
  },
  "patch": {
    "patch_dir": ".../covidcast-indicators/nssp_ed/patch",
    "start_issue": "2026-09-20",
    "end_issue": "2026-09-27"
  }
}

It generates data for that range of issue dates and stores it in batch issue
format:
[params patch_dir]/issue_[issue-date]/nssp-ed/xxx.csv

How issues are reconstructed
----------------------------
The NSSP ED respiratory feed is not versioned by CDC: each weekly refresh
overwrites the dataset, and the last few weeks of data are routinely revised.
True as-of issue reconstruction therefore relies on the raw backups that
``create_backup_csv`` writes on every production run
(``<backup_dir>/<YYYYMMDD>.csv.gz``, named by fetch date).

For each issue date, the patcher loads the most recent raw backup fetched on
or before that issue date and keeps only records with a reference date
(``timestamp``) on or before the issue date -- i.e. data that could have been
known on the issue date. Because late-arriving revisions are not preserved by
the source, patched issues are approximations of what a same-day run would
have emitted; this limitation is inherent to the source, not the patcher.
"""

from datetime import datetime, timedelta
from glob import glob
from os import makedirs
from os.path import basename, join
from typing import List, Optional

import pandas as pd
from delphi_utils import get_structured_logger, read_params

from .run import run_module


def find_backup_for_issue(backup_dir: str, issue_date: datetime) -> Optional[str]:
    """Return the raw backup fetched most recently on/before ``issue_date``.

    Backups are named ``<YYYYMMDD>[_...].csv.gz`` by fetch date. Returns None
    when no backup exists yet for the issue date.
    """
    candidates: List[str] = []
    for path in glob(join(backup_dir, "*.csv.gz")) + glob(join(backup_dir, "*.csv")):
        stem = basename(path)
        try:
            file_date = datetime.strptime(stem[:8], "%Y%m%d")
        except ValueError:
            continue
        if file_date <= issue_date:
            candidates.append((file_date, path))
    if not candidates:
        return None
    candidates.sort()
    return candidates[-1][1]


def patch(params):
    """Run the nssp_ed indicator for a range of issue dates.

    Parameters
    ----------
    params
        Dictionary containing indicator configuration. Expected to have the
        following structure:
    - "common":
        - "export_dir": str, directory to write output (overridden per issue)
        - "backup_dir": str, directory holding raw backups from
          ``create_backup_csv``
        - "log_filename" (optional): str, name of file to write logs
        - "log_exceptions" (optional): bool, whether to log exceptions to file
        - "custom_run": bool, must be True for patching
    - "indicator": (optional) same options as in run.py
    - "patch": Only used for patching data
        - "start_issue": str, YYYY-MM-DD format, first issue date
        - "end_issue": str, YYYY-MM-DD format, last issue date
        - "patch_dir": str, directory to write all issues output
    """
    logger = get_structured_logger("delphi_nssp_ed.patch", filename=params["common"].get("log_filename"))

    issue_date = datetime.strptime(params["patch"]["start_issue"], "%Y-%m-%d")
    end_issue = datetime.strptime(params["patch"]["end_issue"], "%Y-%m-%d")

    logger.info(
        "Starting patching",
        patch_directory=params["patch"]["patch_dir"],
        start_issue=issue_date.strftime("%Y-%m-%d"),
        end_issue=end_issue.strftime("%Y-%m-%d"),
    )

    makedirs(params["patch"]["patch_dir"], exist_ok=True)
    backup_dir = params["common"]["backup_dir"]

    while issue_date <= end_issue:
        logger.info("Running issue", issue_date=issue_date.strftime("%Y-%m-%d"))

        backup_path = find_backup_for_issue(backup_dir, issue_date)
        if backup_path is None:
            logger.warning(
                "No raw backup available on or before issue date; skipping issue",
                issue_date=issue_date.strftime("%Y-%m-%d"),
            )
            issue_date += timedelta(days=1)
            continue

        # Output dir setup, in batch-issue format for acquisition.
        current_issue_yyyymmdd = issue_date.strftime("%Y%m%d")
        current_issue_dir = join(params["patch"]["patch_dir"], f"issue_{current_issue_yyyymmdd}", "nssp-ed")
        makedirs(current_issue_dir, exist_ok=True)

        # Only data with a reference date on/before the issue date could have
        # been known on the issue date. The dataframe is passed in the raw
        # feed format; run_module -> pull_nssp_ed_data conforms it.
        df_raw = pd.read_csv(backup_path)
        df_issue = df_raw[pd.to_datetime(df_raw["date"]) <= issue_date].reset_index(drop=True)
        logger.info(
            "Reconstructed issue snapshot",
            issue_date=issue_date.strftime("%Y-%m-%d"),
            backup_used=backup_path,
            num_records=len(df_issue),
        )

        params["common"]["export_dir"] = current_issue_dir
        params["common"]["custom_run"] = True
        run_module(params, logger=logger, source_df=df_issue)

        issue_date += timedelta(days=1)

    logger.info("Patching complete")


if __name__ == "__main__":  # pragma: no cover
    patch(read_params())
