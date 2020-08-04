from contextlib import contextmanager
import filecmp
from glob import glob
from os import remove, replace
from os.path import join, basename, abspath
import shutil
from typing import Tuple, List, Dict, Optional

from boto3 import Session
from git import Repo
import pandas as pd

Files = List[str]
FileDiffMap = Dict[str, Optional[str]]

def diff_export_csv(
    before_csv: str,
    after_csv: str
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    export_csv_dtypes = {"geo_id": str, "val": float, "se": float, "sample_size": float}

    before_df = pd.read_csv(before_csv, dtype=export_csv_dtypes)
    before_df.set_index("geo_id", inplace=True)

    after_df = pd.read_csv(after_csv, dtype=export_csv_dtypes)
    after_df.set_index("geo_id", inplace=True)

    deleted_idx = before_df.index.difference(after_df.index)
    common_idx = before_df.index.intersection(after_df.index)
    added_idx = after_df.index.difference(before_df.index)

    before_df_cmn = before_df.reindex(common_idx)
    after_df_cmn = after_df.reindex(common_idx)

    # Comparison treating NA == NA as True
    # TODO: Should we change exact equality to some approximate one?
    same_mask = before_df_cmn == after_df_cmn
    same_mask |= pd.isna(before_df_cmn) & pd.isna(after_df_cmn)

    return (
        before_df.loc[deleted_idx, :],
        after_df_cmn.loc[~(same_mask.all(axis=1)), :],
        after_df.loc[added_idx, :])

class ArchiveDiffer:

    def __init__(self, cache_dir: str, export_dir: str):
        self.cache_dir = cache_dir
        self.export_dir = export_dir

        self._cache_updated = False
        self._exports_archived = False

    def update_cache(self):
        # Depends on the archiving backend
        raise NotImplementedError

    def diff_exports(self) -> Tuple[Files, FileDiffMap, Files]:
        # Main logic of finding diffs across and within CSV files
        # Should be called after update_cache
        assert self._cache_updated

        # Glob to only pick out CSV files, ignore hidden files
        previous_files = set(basename(f) for f in glob(join(self.cache_dir, "*.csv")))
        exported_files = set(basename(f) for f in glob(join(self.export_dir, "*.csv")))

        deleted_files = sorted(join(self.cache_dir, f) for f in previous_files - exported_files)
        common_filenames = sorted(exported_files & previous_files)
        new_files = sorted(join(self.export_dir, f) for f in exported_files - previous_files)

        common_diffs = {}
        for filename in common_filenames:
            before_file = join(self.cache_dir, filename)
            after_file = join(self.export_dir, filename)

            common_diffs[after_file] = None

            # Check for simple file similarity before doing CSV diffs
            if filecmp.cmp(before_file, after_file, shallow=False):
                continue

            deleted_df, changed_df, added_df = diff_export_csv(before_file, after_file)
            new_issues_df = pd.concat([changed_df, added_df], axis=0)

            if len(deleted_df) > 0:
                print(f"Warning, diff has deleted indices in {after_file} that will be ignored")

            # Write the diffs to diff_file, if applicable
            if len(new_issues_df) > 0:
                diff_file = join(self.export_dir, filename + ".diff")

                new_issues_df.to_csv(diff_file, na_rep="NA")
                common_diffs[after_file] = diff_file

        return deleted_files, common_diffs, new_files

    def archive_exports(self, exported_files: Files) -> Tuple[Files, Files]:
        raise NotImplementedError

    def filter_exports(self, common_diffs: FileDiffMap):
        # Should be called after archive_exports
        assert self._exports_archived

        for exported_file, diff_file in common_diffs.items():
            # Delete existing exports that had no data diff
            if diff_file is None:
                remove(exported_file)

            # Replace exports where diff file was generated
            else:
                replace(diff_file, exported_file)

class S3ArchiveDiffer(ArchiveDiffer):

    def __init__(
        self, cache_dir: str, export_dir: str,
        bucket_name: str,
        indicator_prefix: str,
        s3_credentials: Dict[str, str],
    ):
        super().__init__(cache_dir, export_dir)
        self.s3 = Session(**s3_credentials).resource("s3")
        self.bucket = self.s3.Bucket(bucket_name)
        self.bucket_versioning = self.s3.BucketVersioning(bucket_name)
        self.indicator_prefix = indicator_prefix

    def update_cache(self):
        # List all indicator-related objects from S3
        archive_objects = self.bucket.objects.filter(Prefix=self.indicator_prefix).all()
        archive_objects = [obj for obj in archive_objects if obj.key.endswith(".csv")]

        # Check against what we have locally and download missing ones
        cached_files = set(basename(f) for f in glob(join(self.cache_dir, "*.csv")))
        for obj in archive_objects:
            archive_file = basename(obj.key)
            cached_file = join(self.cache_dir, archive_file)

            if archive_file not in cached_files:
                print(f"Updating cache with {cached_file}")
                obj.Object().download_file(cached_file)

        self._cache_updated = True

    def archive_exports(self, exported_files: Files) -> Tuple[Files, Files]:
        archive_success = []
        archive_fail = []

        # Enable versioning if turned off
        # if self.bucket_versioning.status != "Enabled":
        #     self.bucket_versioning.enable()

        for exported_file in exported_files:
            cached_file = abspath(join(self.cache_dir, basename(exported_file)))
            archive_key = join(self.indicator_prefix, basename(exported_file))

            try:
                # Update local cache
                shutil.copyfile(exported_file, cached_file)

                self.bucket.Object(archive_key).upload_file(exported_file)
                # TODO: Wait until exists to confirm successful upload?
                archive_success.append(exported_file)
            except:
                archive_fail.append(exported_file)

        self._exports_archived = True

        return archive_success, archive_fail

class GitArchiveDiffer(ArchiveDiffer):

    def __init__(
        self, cache_dir: str, export_dir: str,
        branch_name: Optional[str] = None,
        override_dirty: bool = False,
        commit_partial_success: bool = False,
        commit_message: str = "Automated archive",
    ):
        super().__init__(cache_dir, export_dir)

        assert override_dirty or not commit_partial_success, \
                "Only can commit_partial_success=True when override_dirty=True"

        # TODO: Handle if no repository is found
        self.repo = Repo(cache_dir, search_parent_directories=True)

        self.orig_branch = self.repo.active_branch
        self.branch = self.get_branch(branch_name)
        self.override_dirty = override_dirty
        self.commit_partial_success = commit_partial_success
        self.commit_message = commit_message

    def get_branch(self, branch_name: Optional[str] = None):
        if branch_name is None:
            return self.repo.active_branch

        if branch_name in self.repo.branches:
            return self.repo.branches[branch_name]

        return self.repo.create_head(branch_name)

    @contextmanager
    def archiving_branch(self):
        self.branch.checkout()

        try:
            yield self.branch
        finally:
            self.orig_branch.checkout()

    def update_cache(self):
        # Make sure cache directory is clean: has everything nicely committed
        if not self.override_dirty:
            cache_clean = not self.repo.is_dirty(untracked_files=True, path=abspath(self.cache_dir))
            assert cache_clean, f"There are uncommitted changes in the cache dir '{self.cache_dir}'"

        self._cache_updated = True

    def diff_exports(self) -> Tuple[Files, FileDiffMap, Files]:
        with self.archiving_branch():
            return super().diff_exports()

    def archive_exports(self, exported_files: Files) -> Tuple[Files, Files]:
        archived_files = []
        archive_success = []
        archive_fail = []

        with self.archiving_branch():
            # Abs paths of all modified files to check if we will override uncommitted changes
            working_tree_dir = self.repo.working_tree_dir
            dirty_files = [join(working_tree_dir, f) for f in self.repo.untracked_files]
            dirty_files += [join(working_tree_dir, d.a_path) for d in self.repo.index.diff(None)]

            for exported_file in exported_files:
                archive_file = abspath(join(self.cache_dir, basename(exported_file)))

                # Archive and explicitly stage new export, depending if override
                if self.override_dirty or archive_file not in dirty_files:
                    # Archive
                    shutil.copyfile(exported_file, archive_file)

                    archived_files.append(archive_file)
                    archive_success.append(exported_file)

                # Otherwise ignore the archiving for this file
                else:
                    archive_fail.append(exported_file)

            # Stage
            self.repo.index.add(archived_files)

            # Commit staged files
            if len(exported_files) > 0:

                # Support partial success and at least one archive succeeded
                partial_success = self.commit_partial_success and len(archive_success) > 0

                if len(archive_success) == len(exported_files) or partial_success:
                    self.repo.index.commit(message=self.commit_message)

        self._exports_archived = True

        return archive_success, archive_fail
