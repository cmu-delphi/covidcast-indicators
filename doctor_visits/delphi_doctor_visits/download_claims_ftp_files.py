#!/usr/bin/env python3
"""Downloads files modified in the last 24 hours from the delphi ftp server."""

# standard
import re
import datetime
import functools
from os import path

# third party
import paramiko


class AllowAnythingPolicy(paramiko.MissingHostKeyPolicy):
    """Class for missing host key policy."""

    def missing_host_key(self, client, hostname, key):
        """Check missing host key."""
        return


def print_callback(filename, logger, bytes_so_far, bytes_total, progress_chunks):
    """Print the callback information."""
    rough_percent_transferred = int(100 * (bytes_so_far / bytes_total))
    if rough_percent_transferred in progress_chunks:
        logger.info("Transfer in progress", filename=filename, percent=rough_percent_transferred)
        progress_chunks.remove(rough_percent_transferred)

OLD_FILENAME_TIMESTAMP = re.compile(
    r".*EDI_AGG_OUTPATIENT_[0-9]_(?P<ymd>[0-9]*)_(?P<hm>[0-9]*)[^0-9]*")
NEW_FILENAME_TIMESTAMP = re.compile(r".*EDI_AGG_OUTPATIENT_(?P<ymd>[0-9]*)_(?P<hm>[0-9]*)[^0-9]*")
def get_timestamp(name):
    """Get the reference date in datetime format."""
    if len(name.split("_")) > 5:
        m = OLD_FILENAME_TIMESTAMP.match(name)
    else:
        m = NEW_FILENAME_TIMESTAMP.match(name)
    if not m:
        return datetime.datetime(1900, 1, 1)
    try:
        return datetime.datetime.strptime(''.join(m.groups()), "%Y%m%d%H%M")
    except ValueError:
        return datetime.datetime.strptime(''.join(m.groups()), "%m%d%Y%H%M")

def change_date_format(name):
    """Flip date from YYYYMMDD to MMDDYYYY."""
    split_name = name.split("_")
    date = split_name[3]
    flip_date = date[6:] + date[4:6] + date[:4]
    split_name[3] = flip_date
    name = '_'.join(split_name)
    return name

def download(ftp_credentials, out_path, logger, issue_date=None):
    """Pull the latest raw files."""
    if not issue_date:
        current_time = datetime.datetime.now()
    else:
        current_time = datetime.datetime.strptime(issue_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)

    logger.info("starting download", time=current_time)
    seconds_in_day = 24 * 60 * 60

    # open client
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(AllowAnythingPolicy())

    client.connect(ftp_credentials["host"],
                   username=ftp_credentials["user"],
                   password=ftp_credentials["pass"],
                   port=ftp_credentials["port"])
    sftp = client.open_sftp()
    sftp.chdir('/optum/receiving')

    # go through files in recieving dir
    files_to_download = []
    for fileattr in sftp.listdir_attr():
        # file_time = datetime.datetime.fromtimestamp(fileattr.st_mtime)
        file_time = get_timestamp(fileattr.filename)
        time_diff_to_current_time = current_time - file_time
        if 0 < time_diff_to_current_time.total_seconds() <= seconds_in_day:
            files_to_download.append(fileattr.filename)
            logger.info("File to download", filename=fileattr.filename)

    # make sure we don't download more that the 3 chunked drops (2x a day) for OP
    # and the 1 chunk (2x a day) for IP - 01/07/21, multiplied by 2 since missing
    # days are often added
    assert len(files_to_download) <= 2 * (3 * 2), \
        "more files dropped than expected"

    filepaths_to_download = {}
    for file in files_to_download:
        flipped_file = change_date_format(file)
        if "OUTPATIENT" in file:
            full_path = path.join(out_path, flipped_file)
            if path.exists(full_path):
                logger.info("Skip the existing file", filename=flipped_file)
            else:
                filepaths_to_download[file] = full_path

    # download!
    for infile, outfile in filepaths_to_download.items():
        callback_for_filename = functools.partial(print_callback, infile, logger, progress_chunks=[0, 25, 50, 75])
        sftp.get(infile, outfile, callback=callback_for_filename)
        logger.info("Transfer in progress", filename=infile, percent=100)

    client.close()
