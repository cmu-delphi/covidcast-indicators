# -*- coding: utf-8 -*-
from datetime import datetime
import filecmp
from glob import glob
from os import replace, remove
from os.path import join, abspath, basename
import shutil
from typing import Tuple, List, Dict, Optional

import csvdiff
from git import Repo
import pandas as pd


def create_export_csv(
    df: pd.DataFrame,
    start_date: datetime,
    export_dir: str,
    metric: str,
    geo_res: str,
    sensor: str,
):
    """Export data in the format expected by the Delphi API.

    Parameters
    ----------
    df: pd.DataFrame
        Columns: geo_id, timestamp, val, se, sample_size
    export_dir: str
        Export directory
    metric: str
        Metric we are considering
    geo_res: str
        Geographic resolution to which the data has been aggregated
    sensor: str
        Sensor that has been calculated (cumulative_counts vs new_counts)
    """
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    dates = pd.Series(
        df[df["timestamp"] >= start_date]["timestamp"].unique()
    ).sort_values()
    for date in dates:
        export_fn = f'{date.strftime("%Y%m%d")}_{geo_res}_' f"{metric}_{sensor}.csv"
        export_file = join(export_dir, export_fn)
        df[df["timestamp"] == date][["geo_id", "val", "se", "sample_size",]].to_csv(
            export_file, index=False, na_rep="NA"
        )

def diff_exports(
    archive_dir: str,
    export_dir: str,
    csv_index_col: str = "geo_id",
    deleted_indices_ok: bool = False,
) -> Tuple[List[str], Dict[str, Optional[str]], List[str]]:

    # Glob to only pick out CSV files, ignore hidden files
    previous_files = set(basename(f) for f in glob(join(archive_dir, "*.csv")))
    exported_files = set(basename(f) for f in glob(join(export_dir, "*.csv")))

    deleted_files = sorted(join(archive_dir, f) for f in previous_files - exported_files)
    common_filenames = sorted(exported_files & previous_files)
    new_files = sorted(join(export_dir, f) for f in exported_files - previous_files)

    common_files_to_diffs = {}
    for filename in common_filenames:
        before_file = join(archive_dir, filename)
        after_file = join(export_dir, filename)

        # Check for simple file similarity before doing CSV diffs
        if filecmp.cmp(before_file, after_file, shallow=False):
            common_files_to_diffs[after_file] = None
            continue

        # TODO: Realize we only need row-level diffs, dont need CSV cell diffs
        diff = csvdiff.diff_files(
            before_file, after_file,
            index_columns=[csv_index_col])

        added_indices = [added[csv_index_col] for added in diff["added"]]
        changed_indices = [changed["key"][0] for changed in diff["changed"]]
        if len(diff["removed"]) > 0 and not deleted_indices_ok:
            raise NotImplementedError("Have not determined how to represent deletions")

        # Write the diffs to diff_file, if applicable
        if len(added_indices + changed_indices) > 0:

            # Only load up the CSV in pandas if there are indices to access
            after_df = pd.read_csv(
                after_file,
                dtype={"geo_id": str, "val": float, "se": float, "sample_size": float})
            after_df.set_index(csv_index_col, inplace=True)
            new_issues_df = after_df.loc[added_indices + changed_indices, :]

            diff_file = join(export_dir, filename + ".diff")
            new_issues_df.to_csv(diff_file, na_rep="NA")
            common_files_to_diffs[after_file] = diff_file

        else:
            common_files_to_diffs[after_file] = None

    return deleted_files, common_files_to_diffs, new_files

def archive_exports(
    exported_files: List[str],
    archive_dir: str,
    override_uncommitted: bool = False,
    auto_commit: bool = True,
    commit_partial_success: bool = False,
    commit_message: str = "Automated archive",
) -> Tuple[List[str], List[str]]:

    repo = Repo(".", search_parent_directories=True)


    # Abs paths of all modified files to check if we will override uncommitted changes
    dirty_files = [join(repo.working_tree_dir, f) for f in repo.untracked_files]
    dirty_files += [join(repo.working_tree_dir, d.a_path) for d in repo.index.diff(None)]

    archived_files = []
    archive_success = []
    archive_fail = []
    for exported_file in exported_files:
        archive_file = abspath(join(archive_dir, basename(exported_file)))

        # Archive and explicitly stage new export, depending if override
        if archive_file not in dirty_files or override_uncommitted:
            # Archive
            shutil.copyfile(exported_file, archive_file)

            archived_files.append(archive_file)
            archive_success.append(exported_file)

        # Otherwise ignore the archiving for this file
        else:
            archive_fail.append(exported_file)

    # Stage
    repo.index.add(archived_files)

    # Commit staged files
    if auto_commit and len(exported_files) > 0:

        # Support partial success and at least one archive succeeded
        partial_success = commit_partial_success and len(archive_success) > 0

        if len(archive_success) == len(exported_files) or partial_success:
            repo.index.commit(message=commit_message)

    return archive_success, archive_fail

def remove_identical_exports(
    common_files_to_diffs: Dict[str, Optional[str]]
):
    for common_file, diff_file in common_files_to_diffs.items():
        if diff_file is None:
            remove(common_file)

def replace_exports(
    exported_files: List[str],
    common_files_to_diffs: Dict[str, Optional[str]],
):
    for exported_file in exported_files:
        # If exported_file is not a key, then it was not a common file.
        # So no replacing would be needed anyway
        diff_file = common_files_to_diffs.get(exported_file, None)

        if diff_file is not None:
            replace(diff_file, exported_file)
