# -*- coding: utf-8 -*-
"""Function for diffing and archiving."""

from os import remove, listdir
from os.path import join
from shutil import copy
from datetime import datetime

from delphi_utils import S3ArchiveDiffer

def arch_diffs(params, daily_arch_diff):
    """
    Archive differences between new updates and existing data.

    We check for updates for NCHS mortality data every weekday as how it is
    reported by NCHS and stash these daily updates on S3, but not our API.
    On a weekly level (on Mondays), we additionally upload the changes to the
    data made over the past week (due to backfill) to our API.

    Parameters:
    -----------
    params: dict
        Read from params.json
    daily_arch_diff: S3ArchiveDiffer
        Used to store and update cache
    """
    export_dir = params["export_dir"]
    daily_export_dir = params["daily_export_dir"]
    cache_dir = params["cache_dir"]

    # Weekly run of archive utility on Monday
    # - Does not upload to S3, that is handled by daily run of archive utility
    # - Exports issues into receiving for the API
    if datetime.today().weekday() == 0:
        # Copy todays raw output to receiving
        for output_file in listdir(daily_export_dir):
            copy(
                join(daily_export_dir, output_file),
                join(export_dir, output_file))

        weekly_arch_diff = S3ArchiveDiffer(
            cache_dir, export_dir,
            params["bucket_name"], "nchs_mortality",
            params["aws_credentials"])

        # Dont update cache from S3 (has daily files), only simulate a update_cache() call
        weekly_arch_diff._cache_updated = True  # pylint: disable=protected-access

        # Diff exports, and make incremental versions
        _, common_diffs, new_files = weekly_arch_diff.diff_exports()

        # Archive changed and new files only
        to_archive = [f for f, diff in common_diffs.items() if diff is not None]
        to_archive += new_files
        _, fails = weekly_arch_diff.archive_exports(to_archive, update_s3=False)

        # Filter existing exports to exclude those that failed to archive
        succ_common_diffs = {f: diff for f, diff in common_diffs.items() if f not in fails}
        weekly_arch_diff.filter_exports(succ_common_diffs)

        # Report failures: someone should probably look at them
        for exported_file in fails:
            print(f"Failed to archive (weekly) '{exported_file}'")

    # Daily run of archiving utility
    # - Uploads changed files to S3
    # - Does not export any issues into receiving

    # Diff exports, and make incremental versions
    _, common_diffs, new_files = daily_arch_diff.diff_exports()

    # Archive changed and new files only
    to_archive = [f for f, diff in common_diffs.items() if diff is not None]
    to_archive += new_files
    _, fails = daily_arch_diff.archive_exports(to_archive)

    # Daily output not needed anymore, remove them
    for exported_file in new_files:
        remove(exported_file)
    for exported_file, diff_file in common_diffs.items():
        remove(exported_file)
        if diff_file is not None:
            remove(diff_file)

    # Report failures: someone should probably look at them
    for exported_file in fails:
        print(f"Failed to archive (daily) '{exported_file}'")
