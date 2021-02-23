# standard
import pytest
import mock
from datetime import datetime as dt
from datetime import timedelta

# first party
from delphi_changehc.download_ftp_files import *

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

        # When seven new files are present, AssertionError
        new_file1 = self.FileAttr(dt.timestamp(dt.now()), "00001122_foo1")
        new_file2 = self.FileAttr(dt.timestamp(dt.now()), "00001122_foo2")
        new_file3 = self.FileAttr(dt.timestamp(dt.now()), "00001122_foo3")
        new_file4 = self.FileAttr(dt.timestamp(dt.now()), "00001122_foo4")
        new_file5 = self.FileAttr(dt.timestamp(dt.now()), "00001122_foo5")
        new_file6 = self.FileAttr(dt.timestamp(dt.now()), "00001122_foo6")
        new_file7 = self.FileAttr(dt.timestamp(dt.now()), "00001122_foo7")
        seven_new = self.MockSFTP([new_file1, new_file2, new_file3, new_file4,
                                    new_file5, new_file6, new_file7])
        with pytest.raises(AssertionError):
            get_files_from_dir(seven_new, "00001122", "")

        # When the file already exists, no files are downloaded
        mock_path.exists.return_value = True
        one_exists = self.MockSFTP([new_file1])
        get_files_from_dir(one_new, "00001122", "")
        assert one_exists.num_gets == 0
        
