# -*- coding: utf-8 -*-
from datetime import datetime
import filecmp
from os import listdir
from os.path import join, abspath
import shutil
from typing import List

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

def filter_new_issues(
    export_dir: str,
    cache_dir: str,
    ) -> bool:

    repo = Repo(".", search_parent_directories=True)
    # Abs paths of all modified files to check if we will override uncommitted changes
    dirty_files = [join(repo.working_tree_dir, f) for f in repo.untracked_files]
    dirty_files += [join(repo.working_tree_dir, d.a_path) for d in repo.index.diff(None)]

    exported_files = set(listdir(export_dir))
    previous_files = set(listdir(cache_dir))

    # TODO: Deal with deleted_files (previous - exported)

    # New files
    for filename in exported_files - previous_files:
        before_file = join(cache_dir, filename)
        after_file = join(export_dir, filename)

        # Archive all, as all new files are new issues too
        assert before_file not in dirty_files
        shutil.copyfile(after_file, before_file)

        # Stage
        repo.index.add(abspath(before_file))

    # Common files
    dirty_conflict = False
    for filename in exported_files & previous_files:
        before_file = join(cache_dir, filename)
        after_file = join(export_dir, filename)
        diffs_file = join(export_dir, filename + ".diff")

        # Check for simple file similarity before doing CSV diffs
        if filecmp.cmp(before_file, after_file, shallow=False):
            continue

        after_df = pd.read_csv(
            after_file,
            dtype={"geo_id": str, "val": float, "se": float, "sample_size": float})
        after_df.set_index("geo_id", inplace=True)

        diff = csvdiff.diff_files(
            before_file, after_file,
            index_columns=["geo_id"])

        added_keys = [added["geo_id"] for added in diff["added"]]
        changed_keys = [changed["key"] for changed in diff["changed"]]
        if len(diff["removed"]) > 0:
            raise NotImplementedError("Cannot handle deletions yet")

        # Write new issues only
        new_issues_df = after_df.loc[added_keys + changed_keys, :]

        # If archiving overwrites uncommitted changes,
        # skip archiving and write new issues to diffs_file instead
        if abspath(before_file) in dirty_files:
            print(f"Warning, want to archive '{after_file}' as '{before_file}' but latter has uncommitted changes. Skipping archiving...")
            dirty_conflict = True
            new_issues_df.to_csv(diffs_file, index=False, na_rep="NA")

        # Otherwise, archive and explicitly stage new export, then replace with just new issues
        else:
            # Archive
            shutil.copyfile(after_file, before_file)

            # Stage
            repo.index.add(abspath(before_file))

            # Replace
            new_issues_df.to_csv(after_file, index=False, na_rep="NA")

    if not dirty_conflict:
        repo.index.commit(message="Automated archive")
        return True

    print(
        "Some files were not archived to prevent overwritting uncommitted changes.\n"
        f"Look for *.csv.diff files in {export_dir} and manually resolve / archive affected files.")
    return False
