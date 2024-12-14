# standard
import pytest
import mock
from datetime import datetime as dt
from datetime import timedelta

# first party
from delphi_changehc.download_ftp_files import *
from delphi_changehc.constants import EXPECTED_FILES_PER_DROP

class TestDownloadFTPFiles:

    class MockSFTP:

        # Mocks an SFTP connection
        def __init__(self, attrs):
            self.attrs = attrs
            self.num_gets = 0

        # Attrs are modified time and filename
        def listdir_attr(self):
            return self.attrs

        # Don't download anything, just note that method was called
        def get(self, infile, outfile, callback=None):
            self.num_gets += 1
            return


    class FileAttr:

        def __init__(self, time, name):
            self.st_mtime = time
            self.filename = name


    @mock.patch("os.path")
    def test_get_files(self, mock_path):

        # When one new file is present, one file is downloaded
        one_new = self.MockSFTP([
            self.FileAttr(dt.timestamp(dt.now()-timedelta(minutes=1)), "00001122_foo")
        ])
        get_files_from_dir(one_new, "00001122", "")
        assert one_new.num_gets == 1

        # When one new file and one old file are present, one file is downloaded
        one_new_one_old = self.MockSFTP([
            self.FileAttr(dt.timestamp(dt.now()-timedelta(minutes=1)), "00005566_foo"),
            self.FileAttr(dt.timestamp(dt.now()-timedelta(days=10)), "00001122_foo")
        ])
        get_files_from_dir(one_new_one_old, "00005566", "")
        assert one_new_one_old.num_gets == 1

        # When too many new files are present, AssertionError
        file_batch = [
            self.FileAttr(dt.timestamp(dt.now()), f"00001122_foo{i}")
            for i in range(EXPECTED_FILES_PER_DROP + 1)
        ]
        too_many_new = self.MockSFTP(file_batch)
        with pytest.raises(AssertionError):
            get_files_from_dir(too_many_new, "00001122", "")

        # When the file already exists, no files are downloaded
        mock_path.exists.return_value = True
        one_exists = self.MockSFTP([file_batch[0]])
        get_files_from_dir(one_new, "00001122", "")
        assert one_exists.num_gets == 0
        
