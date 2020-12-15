"""
Utilities for diffing and archiving covidcast export CSVs.

Aims to simplify the creation of issues for new and backfilled value for indicators.
Also handles archiving of export CSVs to some backend (git, S3 etc.) before replacing them.

Example workflow regardless of specific ArchiveDiffer used. Should only differ in intialization.
1) Initialize and update cache folder if neccessary
>>> arch_diff = S3ArchiveDiffer(cache_dir, export_dir, ...)
>>> arch_diff.update_cache()
>>> ... # Run indicator and generate full exports in `export_dir`

2) Create new diff files from cache files vs export files
>>> deleted_files, common_diffs, new_files = arch_diff.diff_exports()

3) Archive common files with diffs and new files
>>> to_archive = [f for f, diff in common_diffs.items() if diff is not None]
>>> to_archive += new_files
>>> succs, fails = arch_diff.archive_exports(to_archive)

4) Filter exports: Replace files with their diffs, or remove if no diffs
>>> succ_common_diffs = {f: diff for f, diff in common_diffs.items() if f not in fails}
>>> arch_diff.filter_exports(succ_common_diffs)

Author: Eu Jing Chua
Created: 2020-08-06
"""

from argparse import ArgumentParser
from contextlib import contextmanager
import filecmp
from glob import glob
from os import remove, replace
from os.path import join, basename, abspath
import shutil
from typing import Tuple, List, Dict, Optional

from boto3 import Session
from boto3.exceptions import S3UploadFailedError
from git import Repo
from git.refs.head import Head
import pandas as pd

from .utils import read_params

Files = List[str]
FileDiffMap = Dict[str, Optional[str]]


def diff_export_csv(
    before_csv: str,
    after_csv: str
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Find differences in exported covidcast CSVs, using geo_id as the index.

    Treats NA == NA as True.

    Parameters
    ----------
    before_csv: str
        The CSV file to diff from
    after_csv: str
        The CSV file to diff to

    Returns
    -------
        (deleted_df, changed_df, added_df)
        deleted_df is the pd.DataFrame of deleted rows from before_csv.
        changed_df is the pd.DataFrame of common rows from after_csv with changed values.
        added_df is the pd.DataFrame of added rows from after_csv.
    """
    export_csv_dtypes = {"geo_id": str, "val": float,
                         "se": float, "sample_size": float}

    before_df = pd.read_csv(before_csv, dtype=export_csv_dtypes)
    before_df.set_index("geo_id", inplace=True)

    after_df = pd.read_csv(after_csv, dtype=export_csv_dtypes)
    after_df.set_index("geo_id", inplace=True)

    deleted_idx = before_df.index.difference(after_df.index)
    common_idx = before_df.index.intersection(after_df.index)
    added_idx = after_df.index.difference(before_df.index)

    before_df_cmn = before_df.reindex(common_idx)
    after_df_cmn = after_df.reindex(common_idx)

    # Exact comparisons, treating NA == NA as True
    same_mask = before_df_cmn == after_df_cmn
    same_mask |= pd.isna(before_df_cmn) & pd.isna(after_df_cmn)

    return (
        before_df.loc[deleted_idx, :],
        after_df_cmn.loc[~(same_mask.all(axis=1)), :],
        after_df.loc[added_idx, :])


def run_module(archive_type: str,
               cache_dir: str,
               export_dir: str,
               **kwargs):
    """Build and run an ArchiveDiffer.

    Parameters
    ----------
    archive_type: str
        Type of ArchiveDiffer to run.  Must be one of ["git", "s3"] which correspond to
        `GitArchiveDiffer` and `S3ArchiveDiffer`, respectively.
    cache_dir: str
        The directory for storing most recent archived/uploaded CSVs to start diffing from.
    export_dir: str
        The directory with most recent exported CSVs to diff to.
    **kwargs:
        Keyword arguments corresponding to constructor arguments for the respective ArchiveDiffers.
    """
    if archive_type == "git":
        arch_diff = GitArchiveDiffer(cache_dir,
                                     export_dir,
                                     kwargs["branch_name"],
                                     kwargs["override_dirty"],
                                     kwargs["commit_partial_success"],
                                     kwargs["commit_message"])
    elif archive_type == "s3":
        arch_diff = S3ArchiveDiffer(cache_dir,
                                    export_dir,
                                    kwargs["bucket_name"],
                                    kwargs["indicator_prefix"],
                                    kwargs["aws_credentials"])
    else:
        raise ValueError(f"No archive type named '{archive_type}'")
    arch_diff.run()


class ArchiveDiffer:
    """Base class for performing diffing and archiving of exported covidcast CSVs."""

    def __init__(self, cache_dir: str, export_dir: str):
        """
        Initialize an ArchiveDiffer.

        Parameters
        ----------
        cache_dir: str
            The directory for storing most recent archived/uploaded CSVs to do start diffing from.
            Usually 'cache'.
        export_dir: str
            The directory with most recent exported CSVs to diff to.
            Usually 'receiving'.
        """
        self.cache_dir = cache_dir
        self.export_dir = export_dir

        self._cache_updated = False
        self._exports_archived = False

    def update_cache(self):
        """
        Make sure cache_dir is updated correctly from a backend.

        To be implemented by specific archiving backends.
        Should set self._cache_updated = True after verifying cache is updated.
        """
        raise NotImplementedError

    def diff_exports(self) -> Tuple[Files, FileDiffMap, Files]:
        """
        Find diffs across and within CSV files, from cache_dir to export_dir.

        Should be called after update_cache() succeeds. Only works on *.csv files,
        ignores every other file.

        Returns
        -------
        (deleted_files, common_diffs, new_files): Tuple[Files, FileDiffMap, Files]
            deleted_files: List of files that are present in cache_dir but missing in export_dir.
            common_diffs: Dict mapping common files in export_dir with cache_dir to:
                          - None, if the common file is identical
                          - None, if the export_dir version only has DELETED rows
                          - a filename with .csv.diff suffix, containing ADDED and CHANGED rows ONLY
            added_files: List of files that are missing in cache_dir but present in export_dir.
        """
        assert self._cache_updated

        # Glob to only pick out CSV files, ignore hidden files
        previous_files = set(basename(f)
                             for f in glob(join(self.cache_dir, "*.csv")))
        exported_files = set(basename(f)
                             for f in glob(join(self.export_dir, "*.csv")))

        deleted_files = sorted(join(self.cache_dir, f)
                               for f in previous_files - exported_files)
        common_filenames = sorted(exported_files & previous_files)
        new_files = sorted(join(self.export_dir, f)
                           for f in exported_files - previous_files)

        common_diffs: Dict[str, Optional[str]] = {}
        for filename in common_filenames:
            before_file = join(self.cache_dir, filename)
            after_file = join(self.export_dir, filename)

            common_diffs[after_file] = None

            # Check for simple file similarity before doing CSV diffs
            if filecmp.cmp(before_file, after_file, shallow=False):
                continue

            deleted_df, changed_df, added_df = diff_export_csv(
                before_file, after_file)
            new_issues_df = pd.concat([changed_df, added_df], axis=0)

            if len(deleted_df) > 0:
                print(
                    f"Warning, diff has deleted indices in {after_file} that will be ignored")

            # Write the diffs to diff_file, if applicable
            if len(new_issues_df) > 0:
                diff_file = join(self.export_dir, filename + ".diff")

                new_issues_df.to_csv(diff_file, na_rep="NA")
                common_diffs[after_file] = diff_file

        return deleted_files, common_diffs, new_files

    def archive_exports(self, exported_files: Files) -> Tuple[Files, Files]:
        """
        Handle actual archiving of files, depending on specific backend.

        To be implemented by specific archiving backends.

        Parameters
        ----------
        exported_files: Files
            List of files to be archived. Usually new and changed files.

        Returns
        -------
        (successes, fails): Tuple[Files, Files]
            successes: List of successfully archived files
            fails: List of unsuccessfully archived files
        """
        raise NotImplementedError

    def filter_exports(self, common_diffs: FileDiffMap):
        """
        Filter export directory to only contain relevant files.

        Filters down the export_dir to only contain:
        1) New files, 2) Changed files, filtered-down to the ADDED and CHANGED rows only.
        Should be called after archive_exports() so we archive the raw exports before
        potentially modifying them.

        Parameters
        ----------
        common_diffs: FileDiffmap
            Same semantics as in diff_exports(). For each exported_file, diff key-value pair:
                1) If the diff is None, remove exported_file from export_dir
                2) If there is a diff, replace exported_file with the diff
            Since this is done for all key-value pairs, one can filter down common_diffs to only
            a subset of exported_files to operate on before calling filter_exports.
            For example, removing keys that correspond to failed-to-archive files.
        """
        # Should be called after archive_exports
        assert self._exports_archived

        for exported_file, diff_file in common_diffs.items():
            # Delete existing exports that had no data diff
            if diff_file is None:
                remove(exported_file)

            # Replace exports where diff file was generated
            else:
                replace(diff_file, exported_file)

    def run(self):
        """Run the differ and archive the changed and new files."""
        self.update_cache()

        # Diff exports, and make incremental versions
        _, common_diffs, new_files = self.diff_exports()

        # Archive changed and new files only
        to_archive = [f for f, diff in common_diffs.items()
                      if diff is not None]
        to_archive += new_files
        _, fails = self.archive_exports(to_archive)

        # Filter existing exports to exclude those that failed to archive
        succ_common_diffs = {f: diff for f,
                             diff in common_diffs.items() if f not in fails}
        self.filter_exports(succ_common_diffs)

        # Report failures: someone should probably look at them
        for exported_file in fails:
            print(f"Failed to archive '{exported_file}'")


class S3ArchiveDiffer(ArchiveDiffer):
    """
    AWS S3 backend for archiving.

    Archives CSV files into a S3 bucket, with keys "{indicator_prefix}/{csv_file_name}".
    Ideally, versioning should be enabled in this bucket to track versions of each CSV file.
    """

    def __init__(
        self, cache_dir: str, export_dir: str,
        bucket_name: str,
        indicator_prefix: str,
        aws_credentials: Dict[str, str],
    ):
        """
        Initialize a S3ArchiveDiffer.

        See this link for possible aws_credentials kwargs:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session

        Parameters
        ----------
        cache_dir: str
            The directory for storing most recent archived/uploaded CSVs to do start diffing from.
            Usually 'cache'.
        export_dir: str
            The directory with most recent exported CSVs to diff to.
            Usually 'receiving'.
        bucket_name: str
            The S3 bucket to upload files to.
        indicator_prefix: str
            The prefix for S3 keys related to this indicator.
        aws_credentials: Dict[str, str]
            kwargs to create a boto3.Session, containing AWS credentials/profile to use.
        """
        super().__init__(cache_dir, export_dir)
        self.s3 = Session(**aws_credentials).resource("s3")
        self.bucket = self.s3.Bucket(bucket_name)
        self.indicator_prefix = indicator_prefix

    def update_cache(self):
        """Make sure cache_dir is updated with all latest files from the S3 bucket."""
        # List all indicator-related objects from S3
        archive_objects = self.bucket.objects.filter(
            Prefix=self.indicator_prefix).all()
        archive_objects = [
            obj for obj in archive_objects if obj.key.endswith(".csv")]

        # Check against what we have locally and download missing ones
        cached_files = set(basename(f)
                           for f in glob(join(self.cache_dir, "*.csv")))
        for obj in archive_objects:
            archive_file = basename(obj.key)
            cached_file = join(self.cache_dir, archive_file)

            if archive_file not in cached_files:
                print(f"Updating cache with {cached_file}")
                obj.Object().download_file(cached_file)

        self._cache_updated = True

    def archive_exports(self,  # pylint: disable=arguments-differ
        exported_files: Files,
        update_cache: bool = True,
        update_s3: bool = True
    ) -> Tuple[Files, Files]:
        """
        Handle actual archiving of files to the S3 bucket.

        Parameters
        ----------
        exported_files: Files
            List of files to be archived. Usually new and changed files.

        Returns
        -------
        (successes, fails): Tuple[Files, Files]
            successes: List of successfully archived files
            fails: List of unsuccessfully archived files
        """
        archive_success = []
        archive_fail = []

        for exported_file in exported_files:
            cached_file = abspath(
                join(self.cache_dir, basename(exported_file)))
            archive_key = join(self.indicator_prefix, basename(exported_file))

            try:
                if update_cache:
                    # Update local cache
                    shutil.copyfile(exported_file, cached_file)

                if update_s3:
                    self.bucket.Object(archive_key).upload_file(exported_file)

                archive_success.append(exported_file)
            except FileNotFoundError:
                archive_fail.append(exported_file)

        self._exports_archived = True

        return archive_success, archive_fail


class GitArchiveDiffer(ArchiveDiffer):
    """
    Local git repo backend for archiving.

    Archives CSV files into a local git repo as commits.
    Assumes that a git repository is already set up.
    """

    def __init__(
        self, cache_dir: str, export_dir: str,
        branch_name: Optional[str] = None,
        override_dirty: bool = False,
        commit_partial_success: bool = False,
        commit_message: str = "Automated archive",
    ):
        """
        Initialize a GitArchiveDiffer.

        Parameters
        ----------
        cache_dir: str
            The directory for storing most recent archived/uploaded CSVs to do start diffing from.
            Either cache_dir or some parent dir should be a git repository. Usually 'cache'.
        export_dir: str
            The directory with most recent exported CSVs to diff to.
            Usually 'receiving'.
        branch_name: Optional[str]
            Branch to use for archiving. Uses current branch if None.
        override_dirty: bool
            Whether to allow overwriting of untracked & uncommitted changes in cache_dir.
        commit_partial_success: bool
            Whether to still commit even if some files were not archived and staged due
            to override_dirty=False
        commit_message: str
            The automatic commit message to use for the commit.
        """
        super().__init__(cache_dir, export_dir)

        assert override_dirty or not commit_partial_success, \
            "Only can commit_partial_success=True when override_dirty=True"

        # Assumes a repository is set up already, will raise exception if not found
        self.repo = Repo(cache_dir, search_parent_directories=True)

        self.branch = self.get_branch(branch_name)
        self.override_dirty = override_dirty
        self.commit_partial_success = commit_partial_success
        self.commit_message = commit_message

    def get_branch(self, branch_name: Optional[str] = None) -> Head:
        """
        Retrieve a Head object representing a branch of specified name.

        Creates the branch from the current active branch if does not exist yet.

        Parameters
        ----------
        branch_name: Optional[str]
            If None, just returns current branch. Otherwise, retrieves/creates branch.

        Returns
        -------
        branch: Head
        """
        if branch_name is None:
            return self.repo.active_branch

        if branch_name in self.repo.branches:
            return self.repo.branches[branch_name]

        return self.repo.create_head(branch_name)

    @contextmanager
    def archiving_branch(self):
        """
        Context manager for checking out a branch.

        Useful for checking out self.branch within a context, then switching back
        to original branch when finished.
        """
        orig_branch = self.repo.active_branch
        self.branch.checkout()

        try:
            yield self.branch
        finally:
            orig_branch.checkout()

    def update_cache(self):
        """
        Check if cache_dir is clean: has everything nicely committed if override_dirty=False.

        Since we are using a local git repo, assumes there is nothing to update from.
        """
        # Make sure cache directory is clean: has everything nicely committed
        if not self.override_dirty:
            cache_clean = not self.repo.is_dirty(
                untracked_files=True, path=abspath(self.cache_dir))
            assert cache_clean, f"There are uncommitted changes in the cache dir '{self.cache_dir}'"

        self._cache_updated = True

    def diff_exports(self) -> Tuple[Files, FileDiffMap, Files]:
        """
        Find diffs across and within CSV files, from cache_dir to export_dir.

        Same as base class diff_exports, but in context of specified branch.
        """
        with self.archiving_branch():
            return super().diff_exports()

    def archive_exports(self, exported_files: Files) -> Tuple[Files, Files]:
        """
        Handle actual archiving of files to the local git repo.

        Parameters
        ----------
        exported_files: Files
            List of files to be archived. Usually new and changed files.

        Returns
        -------
        (successes, fails): Tuple[Files, Files]
            successes: List of successfully archived files
            fails: List of unsuccessfully archived files
        """
        archived_files = []
        archive_success = []
        archive_fail = []

        with self.archiving_branch():
            # Abs paths of all modified files to check if we will override uncommitted changes
            working_tree_dir = self.repo.working_tree_dir
            dirty_files = [join(working_tree_dir, f)
                           for f in self.repo.untracked_files]
            dirty_files += [join(working_tree_dir, d.a_path)
                            for d in self.repo.index.diff(None)]

            for exported_file in exported_files:
                archive_file = abspath(
                    join(self.cache_dir, basename(exported_file)))

                # Archive and explicitly stage new export, depending if override
                if self.override_dirty or archive_file not in dirty_files:
                    try:
                        # Archive
                        shutil.copyfile(exported_file, archive_file)

                        archived_files.append(archive_file)
                        archive_success.append(exported_file)

                    except (FileNotFoundError, S3UploadFailedError) as ex:
                        print(ex)
                        archive_fail.append(exported_file)

                # Otherwise ignore the archiving for this file
                else:
                    archive_fail.append(exported_file)

            # Stage
            self.repo.index.add(archived_files)

            # Commit staged files
            if len(exported_files) > 0:

                # Support partial success and at least one archive succeeded
                partial_success = self.commit_partial_success and len(
                    archive_success) > 0

                if len(archive_success) == len(exported_files) or partial_success:
                    self.repo.index.commit(message=self.commit_message)

        self._exports_archived = True

        return archive_success, archive_fail


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--archive_type", required=True, type=str,
                        choices=["git", "s3"],
                        help="Type of archive differ to use.")
    parser.add_argument("--indicator_prefix", type=str, default="",
                        help="The prefix for S3 keys related to this indicator."
                        " Required for `archive_type = 's3'")
    parser.add_argument("--branch_name", type=str, default="",
                        help=" Branch to use for `archive_type` = 'git'.")
    parser.add_argument("--override_dirty", action="store_true",
                        help="Whether to allow overwriting of untracked &"
                        " uncommitted changes for `archive_type` = 'git'")
    parser.add_argument("--commit_partial_success",  action="store_true",
                        help="Whether to still commit for `archive_type` = "
                        "'git' even if some files were not archived and "
                        "staged due to `override_dirty` = False.")
    parser.add_argument("--commit_message", type=str, default="",
                        help="Commit message for `archive_type` = 'git'")
    args = parser.parse_args()
    params = read_params()
    run_module(args.archive_type,
               params["cache_dir"],
               params["export_dir"],
               aws_credentials=params["aws_credentials"],
               branch_name=args.branch_name,
               bucket_name=params["bucket_name"],
               commit_message=args.commit_message,
               commit_partial_success=args.commit_partial_success,
               indicator_prefix=args.indicator_prefix,
               override_dirty=args.override_dirty
               )
