
from io import StringIO, BytesIO
from os import listdir, mkdir
from os.path import join

from boto3 import Session
from git import Repo, exc
from moto import mock_s3
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

from delphi_utils import ArchiveDiffer, GitArchiveDiffer, S3ArchiveDiffer

CSV_DTYPES = {"geo_id": str, "val": float, "se": float, "sample_size": float}

class TestArchiveDiffer:

    def test_stubs(self):
        arch_diff = ArchiveDiffer("cache", "export")

        with pytest.raises(NotImplementedError):
            arch_diff.update_cache()

        with pytest.raises(NotImplementedError):
            arch_diff.archive_exports(None)


    def test_diff_and_filter_exports(self, tmp_path):
        cache_dir = join(str(tmp_path), "cache")
        export_dir = join(str(tmp_path), "export")
        mkdir(cache_dir)
        mkdir(export_dir)

        csvs_before = {
            # Common
            "csv0": pd.DataFrame({
                "geo_id": ["1", "2", "3"],
                "val": [1.0, 2.0, 3.0],
                "se": [0.1, 0.2, 0.3],
                "sample_size": [10.0, 20.0, 30.0]}),

            "csv1": pd.DataFrame({
                "geo_id": ["1", "2", "3"],
                "val": [1.0, 2.0, 3.0],
                "se": [np.nan, 0.2, 0.3],
                "sample_size": [10.0, 20.0, 30.0]}),

            # Deleted
            "csv2": pd.DataFrame({
                "geo_id": ["1"],
                "val": [1.0],
                "se": [0.1],
                "sample_size": [10.0]}),
        }

        csvs_after = {
            # Common
            "csv0": pd.DataFrame({
                "geo_id": ["1", "2", "3"],
                "val": [1.0, 2.0, 3.0],
                "se": [0.1, 0.2, 0.3],
                "sample_size": [10.0, 20.0, 30.0]}),

            "csv1": pd.DataFrame({
                "geo_id": ["1", "2", "4"],
                "val": [1.0, 2.1, 4.0],
                "se": [np.nan, 0.21, np.nan],
                "sample_size": [10.0, 21.0, 40.0]}),

            # Added
            "csv3": pd.DataFrame({
                "geo_id": ["2"],
                "val": [2.0],
                "se": [0.2],
                "sample_size": [20.0]}),
        }
        csv1_diff = pd.DataFrame({
            "geo_id": ["2", "4"],
            "val": [2.1, 4.0],
            "se": [0.21, np.nan],
            "sample_size": [21.0, 40.0]})

        arch_diff = ArchiveDiffer(cache_dir, export_dir)

        # Test diff_exports
        # =================

        # Should fail as cache was not updated yet
        with pytest.raises(AssertionError):
            arch_diff.diff_exports()

        # Simulate cache updated, and signal ran finish
        for csv_name, df in csvs_before.items():
            df.to_csv(join(cache_dir, f"{csv_name}.csv"), index=False)
        for csv_name, df in csvs_after.items():
            df.to_csv(join(export_dir, f"{csv_name}.csv"), index=False)
        arch_diff._cache_updated = True

        deleted_files, common_diffs, new_files = arch_diff.diff_exports()

        # Check return values
        assert set(deleted_files) == {join(cache_dir, "csv2.csv")}
        assert set(common_diffs.keys()) == {join(export_dir, f) for f in ["csv0.csv", "csv1.csv"]}
        assert set(new_files) == {join(export_dir, "csv3.csv")}
        assert common_diffs[join(export_dir, "csv0.csv")] is None
        assert common_diffs[join(export_dir, "csv1.csv")] == join(export_dir, "csv1.csv.diff")

        # Check filesystem for actual files
        assert set(listdir(export_dir)) == {"csv0.csv", "csv1.csv", "csv1.csv.diff", "csv3.csv"}
        assert_frame_equal(
            pd.read_csv(join(export_dir, "csv1.csv.diff"), dtype=CSV_DTYPES),
            csv1_diff)

        # Test filter_exports
        # ===================

        # Should fail as archive_exports not called yet
        with pytest.raises(AssertionError):
            arch_diff.filter_exports(common_diffs)

        # Simulate archive
        arch_diff._exports_archived = True

        arch_diff.filter_exports(common_diffs)

        # Check exports directory just has incremental changes
        assert set(listdir(export_dir)) == {"csv1.csv", "csv3.csv"}
        assert_frame_equal(
            pd.read_csv(join(export_dir, "csv1.csv"), dtype=CSV_DTYPES),
            csv1_diff)

AWS_CREDENTIALS = {
    "aws_access_key_id": "FAKE_TEST_ACCESS_KEY_ID",
    "aws_secret_access_key": "FAKE_TEST_SECRET_ACCESS_KEY",
}

@pytest.fixture(scope="function")
def s3_client():
    with mock_s3():
        yield Session(**AWS_CREDENTIALS).client("s3")

class TestS3ArchiveDiffer:
    bucket_name = "test-bucket"
    indicator_prefix = "test"

    @mock_s3
    def test_update_cache(self, tmp_path, s3_client):
        cache_dir = join(str(tmp_path), "cache")
        export_dir = join(str(tmp_path), "export")
        mkdir(cache_dir)
        mkdir(export_dir)

        csv1 = pd.DataFrame({
            "geo_id": ["1", "2", "3"],
            "val": [1.0, 2.0, 3.0],
            "se": [0.1, 0.2, 0.3],
            "sample_size": [10.0, 20.0, 30.0]})
        csv1_buf = StringIO()
        csv1.to_csv(csv1_buf, index=False)

        csv2 = pd.DataFrame({
            "geo_id": ["1", "2", "4"],
            "val": [1.0, 2.1, 4.0],
            "se": [0.1, 0.21, 0.4],
            "sample_size": [10.0, 21.0, 40.0]})
        csv2_buf = StringIO()
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
    def test_archive_exports(self, tmp_path, s3_client):
        cache_dir = join(str(tmp_path), "cache")
        export_dir = join(str(tmp_path), "export")
        mkdir(cache_dir)
        mkdir(export_dir)

        csv1 = pd.DataFrame({
            "geo_id": ["1", "2", "3"],
            "val": [1.0, 2.0, 3.0],
            "se": [0.1, 0.2, 0.3],
            "sample_size": [10.0, 20.0, 30.0]})
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

class TestGitArchiveDiffer:

    def test_init_args(self, tmp_path):
        cache_dir = str(tmp_path / "cache")
        export_dir = str(tmp_path / "export")
        mkdir(cache_dir)
        mkdir(export_dir)

        with pytest.raises(AssertionError):
            GitArchiveDiffer(cache_dir, export_dir, override_dirty=False, commit_partial_success=True)

        with pytest.raises(exc.InvalidGitRepositoryError):
            GitArchiveDiffer(cache_dir, export_dir)

        repo = Repo.init(cache_dir)
        assert not repo.is_dirty(untracked_files=True)

        arch_diff = GitArchiveDiffer(cache_dir, export_dir)
        assert arch_diff.branch == arch_diff.repo.active_branch

    def test_update_cache(self, tmp_path):
        cache_dir = str(tmp_path / "cache")
        export_dir = str(tmp_path / "export")
        mkdir(cache_dir)
        mkdir(export_dir)

        Repo.init(cache_dir)

        # Make repo dirty
        with open(join(cache_dir, "test.txt"), "w") as f:
            f.write("123")

        arch_diff1 = GitArchiveDiffer(cache_dir, export_dir, override_dirty=False)
        with pytest.raises(AssertionError):
            arch_diff1.update_cache()

        arch_diff2 = GitArchiveDiffer(cache_dir, export_dir, override_dirty=True)
        arch_diff2.update_cache()
        assert arch_diff2._cache_updated

    def test_diff_exports(self, tmp_path):
        cache_dir = str(tmp_path / "cache")
        export_dir = str(tmp_path / "export")
        mkdir(cache_dir)
        mkdir(export_dir)

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
            "sample_size": [10.0, 20.0, 30.0]})

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
        cache_dir = str(tmp_path / "cache")
        export_dir = str(tmp_path / "export")
        mkdir(cache_dir)
        mkdir(export_dir)

        repo = Repo.init(cache_dir)
        repo.index.commit(message="Initial commit")
        orig_commit = repo.active_branch.commit

        csv1 = pd.DataFrame({
            "geo_id": ["1", "2", "3"],
            "val": [1.0, 2.0, 3.0],
            "se": [0.1, 0.2, 0.3],
            "sample_size": [10.0, 20.0, 30.0]})

        # csv1.csv is now a dirty edit in the repo, and to be exported too
        csv1.to_csv(join(cache_dir, "csv1.csv"), index=False)
        csv1.to_csv(join(export_dir, "csv1.csv"), index=False)

        # Try to archive csv1.csv and non-existant csv2.csv
        exported_files = [join(export_dir, "csv1.csv"), join(export_dir, "csv2.csv")]

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
