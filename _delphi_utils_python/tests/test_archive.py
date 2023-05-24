from dataclasses import dataclass, field
from io import StringIO, BytesIO
from os import listdir, mkdir
from os.path import join
from typing import Any, Dict, List

from boto3 import Session
from git import Repo, exc
import mock
from moto import mock_s3
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

from delphi_utils.archive import ArchiveDiffer, GitArchiveDiffer, S3ArchiveDiffer,\
    archiver_from_params
from delphi_utils.nancodes import Nans

CSV_DTYPES = {
    "geo_id": str, "val": float, "se": float, "sample_size": float,
    "missing_val": "Int64", "missing_se": "Int64", "missing_sample_size": "Int64"
}

class Example:
    def __init__(self, before, after, diff):
        def fix_df(df):
            if isinstance(df, pd.DataFrame):
                return Example._set_df_datatypes(df, CSV_DTYPES)
            return df
        self.before = fix_df(before)
        self.after = fix_df(after)
        self.diff = fix_df(diff)
    @staticmethod
    def _set_df_datatypes(df: pd.DataFrame, dtypes: Dict[str, Any]) -> pd.DataFrame:
        df = df.copy()
        for k, v in dtypes.items():
            if k in df.columns:
                df[k] = df[k].astype(v)
        return df

@dataclass
class Expecteds:
    deleted: List[str]
    common_diffs: Dict[str, str]
    new: List[str]
    raw_exports: List[str] = field(init=False)
    diffed_exports: List[str] = field(init=False)
    filtered_exports: List[str] = field(init=False)

    def __post_init__(self):
        self.raw_exports = list(self.common_diffs.keys()) + self.new
        self.diffed_exports = self.raw_exports + [diff_name for diff_name in self.common_diffs.values() if diff_name is not None]
        self.filtered_exports = [f.replace(".diff", "") for f in self.diffed_exports if f.endswith(".diff")] + self.new
EMPTY = "empty"
CSVS = {
    "unchanged": Example( # was: csv0
        before=pd.DataFrame({
            "geo_id": ["1", "2", "3"],
            "val": [1.000000001, 2.00000002, 3.00000003],
            "se": [0.1, 0.2, 0.3],
            "sample_size": [10.0, 20.0, 30.0],
            "missing_val": [Nans.NOT_MISSING] * 3,
            "missing_se": [Nans.NOT_MISSING] * 3,
            "missing_sample_size": [Nans.NOT_MISSING] * 3,
        }),
        after=pd.DataFrame({
            "geo_id": ["1", "2", "3"],
            "val": [1.0, 2.0, 3.0], # ignore changes beyond 7 decimal points
            "se": [0.10000001, 0.20000002, 0.30000003],
            "sample_size": [10.0, 20.0, 30.0],
            "missing_val": [Nans.NOT_MISSING] * 3,
            "missing_se": [Nans.NOT_MISSING] * 3,
            "missing_sample_size": [Nans.NOT_MISSING] * 3,
        }),
        diff=EMPTY # unchanged names are listed in common files but have no diff file
    ),
    "mod_2_del_3_add_4": Example( # was: csv1
        before=pd.DataFrame({
            "geo_id": ["1", "2", "3"],
            "val": [1.0, 2.0, 3.0],
            "se": [np.nan, 0.20000002, 0.30000003],
            "sample_size": [10.0, 20.0, 30.0],
            "missing_val": [Nans.NOT_MISSING] * 3,
            "missing_se": [Nans.CENSORED] + [Nans.NOT_MISSING] * 2,
            "missing_sample_size": [Nans.NOT_MISSING] * 3,
        }),
        after=pd.DataFrame({
            "geo_id": ["1", "2", "4"],
            "val": [1.0, 2.1, 4.0],
            "se": [np.nan, 0.21, np.nan],
            "sample_size": [10.0, 21.0, 40.0],
            "missing_val": [Nans.NOT_MISSING] * 3,
            "missing_se": [Nans.CENSORED] + [Nans.NOT_MISSING] * 2,
            "missing_sample_size": [Nans.NOT_MISSING] * 3,
        }),
        diff=pd.DataFrame({
            "geo_id": ["2", "3", "4"],
            "val": [2.1, np.nan, 4.0],
            "se": [0.21, np.nan, np.nan],
            "sample_size": [21.0, np.nan, 40.0],
            "missing_val": [Nans.NOT_MISSING, Nans.DELETED, Nans.NOT_MISSING],
            "missing_se": [Nans.NOT_MISSING, Nans.DELETED, Nans.NOT_MISSING],
            "missing_sample_size": [Nans.NOT_MISSING, Nans.DELETED, Nans.NOT_MISSING],
        })
    ),
    "delete_file": Example( # was: csv2
        before=pd.DataFrame({
            "geo_id": ["1"],
            "val": [1.0],
            "se": [0.1],
            "sample_size": [10.0],
            "missing_val": [Nans.NOT_MISSING],
            "missing_se": [Nans.NOT_MISSING],
            "missing_sample_size": [Nans.NOT_MISSING],
        }),
        after=None,
        diff=None
    ),
    "add_file": Example( # was: csv3
        before=None,
        after=pd.DataFrame({
            "geo_id": ["2"],
            "val": [2.0000002],
            "se": [0.2],
            "sample_size": [20.0],
            "missing_val": [Nans.NOT_MISSING],
            "missing_se": [Nans.NOT_MISSING],
            "missing_sample_size": [Nans.NOT_MISSING],
        }),
        diff=None
    ),
    "unchanged_old_new": Example( # was: csv4
        before=pd.DataFrame({
            "geo_id": ["1"],
            "val": [1.0],
            "se": [0.1],
            "sample_size": [10.0]
        }),
        after=pd.DataFrame({
            "geo_id": ["1"],
            "val": [1.0],
            "se": [0.1],
            "sample_size": [10.0],
            "missing_val": [Nans.NOT_MISSING],
            "missing_se": [Nans.NOT_MISSING],
            "missing_sample_size": [Nans.NOT_MISSING],
        }),
        diff=pd.DataFrame({
            "geo_id": ["1"],
            "val": [1.0],
            "se": [0.1],
            "sample_size": [10.0],
            "missing_val": [Nans.NOT_MISSING],
            "missing_se": [Nans.NOT_MISSING],
            "missing_sample_size": [Nans.NOT_MISSING],
        })
    ),
    "mod_2_del_3_add_4_old_old": Example( # was: csv5
        before=pd.DataFrame({
            "geo_id": ["1", "2", "3"],
            "val": [1.0, 2.0, 3.0],
            "se": [np.nan, 0.20000002, 0.30000003],
            "sample_size": [10.0, 20.0, 30.0]
        }),
        after=pd.DataFrame({
            "geo_id": ["1", "2", "4"],
            "val": [1.0, 2.1, 4.0],
            "se": [np.nan, 0.21, np.nan],
            "sample_size": [10.0, 21.0, 40.0]
        }),
        diff=pd.DataFrame({
            "geo_id": ["2", "3", "4"],
            "val": [2.1, np.nan, 4.0],
            "se": [0.21, np.nan, np.nan],
            "sample_size": [21.0, np.nan, 40.0],
            "missing_val": [np.nan, Nans.DELETED, np.nan],
            "missing_se": [np.nan, Nans.DELETED, np.nan],
            "missing_sample_size": [np.nan, Nans.DELETED, np.nan],
        })
    )
}
EXPECTEDS = Expecteds(
    deleted=["delete_file.csv"],
    common_diffs=dict(
        (f"{csv_name}.csv", None if dfs.diff is EMPTY else f"{csv_name}.csv.diff")
        for csv_name, dfs in CSVS.items() if dfs.diff is not None
    ),
    new=["add_file.csv"],
)
# check for incomplete modifications to tests
assert set(EXPECTEDS.new) == set(f"{csv_name}.csv" for csv_name, dfs in CSVS.items() if dfs.before is None), \
    "Bad programmer: added more new files to CSVS.after without updating EXPECTEDS.new"

def _assert_frames_equal_ignore_row_order(df1, df2, index_cols: List[str] = None):
    return assert_frame_equal(df1.set_index(index_cols).sort_index(), df2.set_index(index_cols).sort_index())

class ArchiveDifferTestlike:
    def set_up(self, tmp_path):
        cache_dir = join(str(tmp_path), "cache")
        export_dir = join(str(tmp_path), "export")
        mkdir(cache_dir)
        mkdir(export_dir)
        return cache_dir, export_dir
    def check_filtered_exports(self, export_dir):
        assert set(listdir(export_dir)) == set(EXPECTEDS.filtered_exports)
        for f in EXPECTEDS.filtered_exports:
            example = CSVS[f.replace(".csv", "")]
            _assert_frames_equal_ignore_row_order(
                pd.read_csv(join(export_dir, f), dtype=CSV_DTYPES),
                example.after if example.diff is None else example.diff,
                index_cols=["geo_id"]
            )

class TestArchiveDiffer(ArchiveDifferTestlike):

    def test_stubs(self):
        arch_diff = ArchiveDiffer("cache", "export")

        with pytest.raises(NotImplementedError):
            arch_diff.update_cache()

        with pytest.raises(NotImplementedError):
            arch_diff.archive_exports(None)

    def test_diff_and_filter_exports(self, tmp_path):
        cache_dir, export_dir = self.set_up(tmp_path)

        arch_diff = ArchiveDiffer(cache_dir, export_dir)

        # Test diff_exports
        # =================

        # Should fail as cache was not updated yet
        with pytest.raises(AssertionError):
            arch_diff.diff_exports()

        # Simulate cache updated, and signal ran finish
        for csv_name, dfs in CSVS.items():
            if dfs.before is not None:
                dfs.before.to_csv(join(cache_dir, f"{csv_name}.csv"), index=False)
            if dfs.after is not None:
                dfs.after.to_csv(join(export_dir, f"{csv_name}.csv"), index=False)
        arch_diff._cache_updated = True

        deleted_files, common_diffs, new_files = arch_diff.diff_exports()

        # Check deleted, common, diffed, and new file names match expected
        assert set(deleted_files) == {join(cache_dir, f) for f in EXPECTEDS.deleted}
        assert set(common_diffs.keys()) == {
            join(export_dir, csv_name) for csv_name in EXPECTEDS.common_diffs}
        assert set(new_files) == {join(export_dir, f) for f in EXPECTEDS.new}
        # Check that diffed file names are identical
        assert all(
            (common_diffs[join(export_dir, csv_name)] ==
             None if diff_name is None else join(export_dir, diff_name))
            for csv_name, diff_name in EXPECTEDS.common_diffs.items()
        )

        # Check filesystem for actual files
        assert set(listdir(export_dir)) == set(EXPECTEDS.diffed_exports)

        # Check that the diff files look as expected
        for key, diff_name in EXPECTEDS.common_diffs.items():
            if diff_name is None: continue
            _assert_frames_equal_ignore_row_order(
                pd.read_csv(join(export_dir, diff_name), dtype=CSV_DTYPES),
                CSVS[key.replace(".csv", "")].diff,
                index_cols=["geo_id"]
            )


        # Test filter_exports
        # ===================

        # Should fail as archive_exports not called yet
        with pytest.raises(AssertionError):
            arch_diff.filter_exports(common_diffs)

        # Simulate archive
        arch_diff._exports_archived = True

        arch_diff.filter_exports(common_diffs)

        # Check exports directory just has incremental changes
        self.check_filtered_exports(export_dir)

AWS_CREDENTIALS = {
    "aws_access_key_id": "FAKE_TEST_ACCESS_KEY_ID",
    "aws_secret_access_key": "FAKE_TEST_SECRET_ACCESS_KEY",
}

class TestS3ArchiveDiffer(ArchiveDifferTestlike):
    bucket_name = "test-bucket"
    indicator_prefix = "test"

    @mock_s3
    def test_update_cache(self, tmp_path):
        s3_client = Session(**AWS_CREDENTIALS).client("s3")
        cache_dir, export_dir = self.set_up(tmp_path)

        csv1 = CSVS["mod_2_del_3_add_4"].before
        csv2 = CSVS["mod_2_del_3_add_4"].after
        csv1_buf = StringIO()
        csv2_buf = StringIO()
        csv1.to_csv(csv1_buf, index=False)
        csv2.to_csv(csv2_buf, index=False)

        # Set up bucket with both objects
        s3_client.create_bucket(Bucket=self.bucket_name)
        s3_client.put_object(
            Bucket=self.bucket_name,
            Key=f"{self.indicator_prefix}/csv1.csv",
            Body=BytesIO(csv1_buf.getvalue().encode()))
        s3_client.put_object(
            Bucket=self.bucket_name,
            Key=f"{self.indicator_prefix}/csv2.csv",
            Body=BytesIO(csv2_buf.getvalue().encode()))

        # Save only csv1 into cache folder
        csv1.to_csv(join(cache_dir, "csv1.csv"), index=False)
        assert set(listdir(cache_dir)) == {"csv1.csv"}

        arch_diff = S3ArchiveDiffer(
            cache_dir, export_dir,
            self.bucket_name, self.indicator_prefix,
            AWS_CREDENTIALS)

        # Should download csv2 into cache folder
        arch_diff.update_cache()
        assert set(listdir(cache_dir)) == {"csv1.csv", "csv2.csv"}

    @mock_s3
    def test_archive_exports(self, tmp_path):
        s3_client = Session(**AWS_CREDENTIALS).client("s3")
        cache_dir, export_dir = self.set_up(tmp_path)

        csv1 = CSVS["mod_2_del_3_add_4"].before
        csv1.to_csv(join(export_dir, "csv1.csv"), index=False)

        s3_client.create_bucket(Bucket=self.bucket_name)

        arch_diff = S3ArchiveDiffer(
            cache_dir, export_dir,
            self.bucket_name, self.indicator_prefix,
            AWS_CREDENTIALS)

        successes, fails = arch_diff.archive_exports([
            join(export_dir, "csv1.csv"),
            join(export_dir, "not_a_csv.csv"),
        ])

        assert set(successes) == {join(export_dir, "csv1.csv")}
        assert set(fails) == {join(export_dir, "not_a_csv.csv")}

        body = s3_client.get_object(
            Bucket=self.bucket_name,
            Key=f"{self.indicator_prefix}/csv1.csv")["Body"]

        assert_frame_equal(pd.read_csv(body, dtype=CSV_DTYPES), csv1)

    @mock_s3
    def test_run(self, tmp_path):
        s3_client = Session(**AWS_CREDENTIALS).client("s3")
        cache_dir, export_dir = self.set_up(tmp_path)

        s3_client.create_bucket(Bucket=self.bucket_name)
        for csv_name, dfs in CSVS.items():
            # Set up current buckets to be the 'before' files.
            if dfs.before is not None:
                csv_buf = StringIO()
                dfs.before.to_csv(csv_buf, index=False)
                s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=f"{self.indicator_prefix}/{csv_name}.csv",
                    Body=BytesIO(csv_buf.getvalue().encode()))
            # Set up the exported files to be the 'after' files.
            if dfs.after is not None:
                dfs.after.to_csv(join(export_dir, f"{csv_name}.csv"), index=False)

        # Create and run differ.
        arch_diff = S3ArchiveDiffer(
            cache_dir, export_dir,
            self.bucket_name, self.indicator_prefix,
            AWS_CREDENTIALS)
        arch_diff.run()

        # Check that the buckets now contain the exported files.
        for csv_name, dfs in CSVS.items():
            if dfs.after is None:
                continue
            body = s3_client.get_object(Bucket=self.bucket_name, Key=f"{self.indicator_prefix}/{csv_name}.csv")["Body"]
            assert_frame_equal(pd.read_csv(body, dtype=CSV_DTYPES), dfs.after)

        # Check exports directory just has incremental changes
        self.check_filtered_exports(export_dir)

class TestGitArchiveDiffer(ArchiveDifferTestlike):

    def test_init_args(self, tmp_path):
        cache_dir, export_dir = self.set_up(tmp_path)

        with pytest.raises(AssertionError):
            GitArchiveDiffer(cache_dir, export_dir,
                             override_dirty=False, commit_partial_success=True)

        with pytest.raises(exc.InvalidGitRepositoryError):
            GitArchiveDiffer(cache_dir, export_dir)

        repo = Repo.init(cache_dir)
        assert not repo.is_dirty(untracked_files=True)

        arch_diff = GitArchiveDiffer(cache_dir, export_dir)
        assert arch_diff.branch == arch_diff.repo.active_branch

    def test_update_cache(self, tmp_path):
        cache_dir, export_dir = self.set_up(tmp_path)

        Repo.init(cache_dir)

        # Make repo dirty
        with open(join(cache_dir, "test.txt"), "w") as f:
            f.write("123")

        arch_diff1 = GitArchiveDiffer(
            cache_dir, export_dir, override_dirty=False)
        with pytest.raises(AssertionError):
            arch_diff1.update_cache()

        arch_diff2 = GitArchiveDiffer(
            cache_dir, export_dir, override_dirty=True)
        arch_diff2.update_cache()
        assert arch_diff2._cache_updated

    def test_diff_exports(self, tmp_path):
        cache_dir, export_dir = self.set_up(tmp_path)

        branch_name = "test-branch"

        repo = Repo.init(cache_dir)
        repo.index.commit(message="Initial commit")
        orig_branch = repo.active_branch
        assert branch_name not in repo.heads

        orig_files = {".git"}

        arch_diff = GitArchiveDiffer(
            cache_dir, export_dir,
            branch_name=branch_name)
        arch_diff.update_cache()

        # Should have no differences, but branch should be created
        deleted_files, common_diffs, new_files = arch_diff.diff_exports()

        assert branch_name in repo.heads
        assert set(listdir(cache_dir)) == orig_files
        assert set(deleted_files) == set()
        assert set(common_diffs.keys()) == set()
        assert set(new_files) == set()

        csv1 = pd.DataFrame({
            "geo_id": ["1", "2", "3"],
            "val": [1.0, 2.0, 3.0],
            "se": [0.1, 0.2, 0.3],
            "sample_size": [10.0, 20.0, 30.0],
            "missing_val": [Nans.NOT_MISSING] * 3,
            "missing_se": [Nans.NOT_MISSING] * 3,
            "missing_sample_size": [Nans.NOT_MISSING] * 3,
        })

        # Write exact same CSV into cache and export, so no diffs expected
        csv1.to_csv(join(cache_dir, "csv1.csv"), index=False)
        csv1.to_csv(join(export_dir, "csv1.csv"), index=False)

        # Store the csv in custom branch
        arch_diff.get_branch(branch_name).checkout()
        repo.index.add([join(cache_dir, "csv1.csv")])
        repo.index.commit(message="Test commit")
        orig_branch.checkout()

        assert repo.active_branch == orig_branch

        deleted_files, common_diffs, new_files = arch_diff.diff_exports()

        # We will be back in original branch, so cache should not have csv1.csv
        assert set(listdir(cache_dir)) == orig_files
        assert set(deleted_files) == set()
        assert set(common_diffs.keys()) == {join(export_dir, "csv1.csv")}
        assert common_diffs[join(export_dir, "csv1.csv")] is None
        assert set(new_files) == set()

    def test_archive_exports(self, tmp_path):
        cache_dir, export_dir = self.set_up(tmp_path)

        repo = Repo.init(cache_dir)
        repo.index.commit(message="Initial commit")
        orig_commit = repo.active_branch.commit

        csv1 = pd.DataFrame({
            "geo_id": ["1", "2", "3"],
            "val": [1.0, 2.0, 3.0],
            "se": [0.1, 0.2, 0.3],
            "sample_size": [10.0, 20.0, 30.0],
            "missing_val": [Nans.NOT_MISSING] * 3,
            "missing_se": [Nans.NOT_MISSING] * 3,
            "missing_sample_size": [Nans.NOT_MISSING] * 3,
        })

        # csv1.csv is now a dirty edit in the repo, and to be exported too
        csv1.to_csv(join(cache_dir, "csv1.csv"), index=False)
        csv1.to_csv(join(export_dir, "csv1.csv"), index=False)

        # Try to archive csv1.csv and non-existant csv2.csv
        exported_files = [join(export_dir, "csv1.csv"),
                          join(export_dir, "csv2.csv")]

        # All should fail, cannot override dirty and file not found
        arch_diff1 = GitArchiveDiffer(
            cache_dir, export_dir,
            override_dirty=False,
            commit_partial_success=False)

        succs, fails = arch_diff1.archive_exports(exported_files)
        assert set(succs) == set()
        assert set(fails) == set(exported_files)

        # Only csv1.csv should succeed, but no commit should be made
        arch_diff2 = GitArchiveDiffer(
            cache_dir, export_dir,
            override_dirty=True,
            commit_partial_success=False)

        succs, fails = arch_diff2.archive_exports(exported_files)
        assert set(succs) == {join(export_dir, "csv1.csv")}
        assert set(fails) == {join(export_dir, "csv2.csv")}
        assert repo.active_branch.commit == orig_commit

        # Only csv1.csv should succeed, and a commit should be made
        arch_diff3 = GitArchiveDiffer(
            cache_dir, export_dir,
            override_dirty=True,
            commit_partial_success=True)

        succs, fails = arch_diff3.archive_exports(exported_files)
        assert set(succs) == {join(export_dir, "csv1.csv")}
        assert set(fails) == {join(export_dir, "csv2.csv")}
        assert repo.active_branch.set_commit("HEAD~1").commit == orig_commit

    def test_run(self, tmp_path):
        cache_dir, export_dir = self.set_up(tmp_path)

        branch_name = "test-branch"

        repo = Repo.init(cache_dir)
        repo.index.commit(message="Initial commit")
        original_branch = repo.active_branch

        for csv_name, dfs in CSVS.items():
            # Set up the current cache to contain 'before' files
            if dfs.before is not None:
                dfs.before.to_csv(join(cache_dir, f"{csv_name}.csv"), index=False)
            # Set up the current export to contain 'after' files
            if dfs.after is not None:
                dfs.after.to_csv(join(export_dir, f"{csv_name}.csv"), index=False)

        # Create and run differ.
        arch_diff = GitArchiveDiffer(
            cache_dir, export_dir,
            branch_name=branch_name, override_dirty=True)
        arch_diff.run()

        # Check that the archive branch contains 'after' files.
        arch_diff.get_branch(branch_name).checkout()
        for csv_name, dfs in CSVS.items():
            if dfs.after is None: continue
            assert_frame_equal(pd.read_csv(join(cache_dir, f"{csv_name}.csv"), dtype=CSV_DTYPES), dfs.after)
        original_branch.checkout()

        # Check exports directory just has incremental changes
        self.check_filtered_exports(export_dir)

class TestFromParams:
    """Tests for creating archive differs from params."""

    def test_null_creation(self):
        """Test that a None object is created with no "archive" params."""
        assert archiver_from_params({"common": {}}) is None

    @mock.patch("delphi_utils.archive.GitArchiveDiffer")
    def test_get_git_archiver(self, mock_archiver):
        """Test that GitArchiveDiffer is created successfully."""
        params = {
            "common": {
                "export_dir": "dir"
            },
            "archive": {
                "cache_dir": "cache",
                "branch_name": "branch",
                "override_dirty": True,
                "commit_partial_success": True,
                "commit_message": "msg"
            }
        }

        archiver_from_params(params)
        mock_archiver.assert_called_once_with(
            export_dir="dir",
            cache_dir="cache",
            branch_name="branch",
            override_dirty=True,
            commit_partial_success=True,
            commit_message="msg"
        )

    @mock.patch("delphi_utils.archive.GitArchiveDiffer")
    def test_get_git_archiver_with_defaults(self, mock_archiver):
        """Test that GitArchiveDiffer is created successfully without optional arguments."""
        params = {
            "common": {
                "export_dir": "dir"
            },
            "archive": {
                "cache_dir": "cache",
                "branch_name": "branch",
                "commit_message": "msg"
            }
        }

        archiver_from_params(params)
        mock_archiver.assert_called_once_with(
            export_dir="dir",
            cache_dir="cache",
            branch_name="branch",
            commit_message="msg"
        )
    @mock.patch("delphi_utils.archive.S3ArchiveDiffer")
    def test_get_s3_archiver(self, mock_archiver):
        """Test that S3ArchiveDiffer is created successfully."""
        params = {
            "common": {
                "export_dir": "dir"
            },
            "archive": {
                "cache_dir": "cache",
                "bucket_name": "bucket",
                "indicator_prefix": "ind",
                "aws_credentials": {"pass": "word"}
            }
        }

        archiver_from_params(params)
        mock_archiver.assert_called_once_with(
            export_dir="dir",
            cache_dir="cache",
            bucket_name="bucket",
            indicator_prefix="ind",
            aws_credentials={"pass": "word"}
        )

    def test_get_s3_archiver_without_required(self):
        """Test that S3ArchiveDiffer is not created without required arguments."""
        params = {
            "common": {
                "export_dir": "dir"
            },
            "archive": {
                "cache_dir": "cache",
                "bucket_name": "bucket"
            }
        }

        with pytest.raises(AssertionError,
                           match="Missing indicator_prefix in params"):
            archiver_from_params(params)

        params["archive"]["indicator_prefix"] = "prefix"
        with pytest.raises(AssertionError,
                           match="Missing aws_credentials in params"):
            archiver_from_params(params)

    @mock.patch("delphi_utils.archive.FilesystemArchiveDiffer")
    def test_get_filesystem_archiver(self, mock_archiver):
        """Test that FilesystemArchiveDiffer is created successfully."""
        params = {
            "common": {
                "export_dir": "dir"
            },
            "archive": {
                "cache_dir": "cache"
            }
        }

        archiver_from_params(params)
        mock_archiver.assert_called_once_with(
            export_dir="dir",
            cache_dir="cache"
        )

    def test_get_filesystem_archiver_with_extra_params(self):
        """Test that FilesystemArchiveDiffer is not created with extra parameters."""
        params = {
            "common": {
                "export_dir": "dir"
            },
            "archive": {
                "cache_dir": "cache",
                "indicator_prefix": "prefix"
            }
        }

        with pytest.raises(AssertionError,
                           match="If you intended to run"):
            archiver_from_params(params)

        del params["archive"]["cache_dir"]
        del params["archive"]["indicator_prefix"]
        with pytest.raises(AssertionError,
                           match="If you intended to run"):
            archiver_from_params(params)
