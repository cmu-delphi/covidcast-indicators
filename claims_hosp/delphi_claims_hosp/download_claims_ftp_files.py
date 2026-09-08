#!/usr/bin/env python3
"""Downloads files modified in the last 24 hours from the delphi ftp server."""

# standard
import datetime
import functools
from os import path
import re

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
        # Remove progress chunk, so it is not logged again
        progress_chunks.remove(rough_percent_transferred)


# the chunk number (old style drops) and the underscore between the date and
# the time (e.g. EDI_AGG_INPATIENT_060620260000CDT.csv.gz) are both optional
FILENAME_TIMESTAMP = re.compile(r".*EDI_AGG_INPATIENT_(?:[0-9]_)?(?P<ymd>[0-9]{8})_?(?P<hm>[0-9]{4})[^0-9]*")


def get_timestamp(name):
    """Get the reference date in datetime format."""
    m = FILENAME_TIMESTAMP.match(name)
    if not m:
        return datetime.datetime(1900, 1, 1)
    try:
        return datetime.datetime.strptime(''.join(m.groups()), "%Y%m%d%H%M")
    except ValueError:
        return datetime.datetime.strptime(''.join(m.groups()), "%m%d%Y%H%M")

def change_date_format(name):
    """Flip the date in a raw filename to DDMMYYYY, separating the date and the time.

    Drops carry the date as either YYYYMMDD or MMDDYYYY, and may omit the
    underscore between the date and the time, e.g.
    EDI_AGG_INPATIENT_060620260000CDT.csv.gz. Downloaded files are always named
    EDI_AGG_INPATIENT_DDMMYYYY_HHMM{timezone}.csv.gz.
    """
    split_name = name.split("_")
    date = split_name[3]
    # chunked drops carry the chunk number in this field instead of the date;
    # as before, those names are left alone
    if len(date) < 8 or not date[:8].isdigit():
        return name
    # MMDD read as a year is always before 1241
    if int(date[:4]) > 1240:
        flip_date = date[6:8] + date[4:6] + date[:4]
    else:
        flip_date = date[2:4] + date[:2] + date[4:8]
    split_name[3] = flip_date
    time_and_suffix = date[8:]
    if time_and_suffix:
        # the date and the time were not separated
        split_name.insert(4, time_and_suffix)
    return "_".join(split_name)


def download(ftp_credentials, out_path, logger, issue_date=None):
    """Pull the latest raw files."""
    if issue_date:
        current_time = datetime.datetime.strptime(issue_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
    else:
        current_time = datetime.datetime.now()
    seconds_in_day = 24 * 60 * 60
    logger.info("Starting download")

    # open client
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(AllowAnythingPolicy())

    client.connect(ftp_credentials["host"],
                   username=ftp_credentials["user"],
                   password=ftp_credentials["pass"],
                   port=ftp_credentials["port"])
    sftp = client.open_sftp()
    sftp.chdir('./receiving')


    # go through files in recieving dir
    files_to_download = []
    for fileattr in sftp.listdir_attr():
        file_time = get_timestamp(fileattr.filename)
        time_diff_to_current_time = current_time - file_time
        if 0 < time_diff_to_current_time.total_seconds() <= seconds_in_day:
            files_to_download.append(fileattr.filename)
            logger.info("File to download", filename=fileattr.filename)

    # make sure we don't download more than the 1 chunk (2x a day) drops for IP - 01/07/21,
    # *2 for multiple day drops
    assert len(files_to_download) <= 2 * (2), \
        f"more files dropped ({len(files_to_download)}) than expected (4)"

    filepaths_to_download = {}
    for file in files_to_download:
        flipped_file = change_date_format(file)
        if "INPATIENT" in file:
            full_path = path.join(out_path, flipped_file)
            if path.exists(full_path):
                logger.info("Skip the existing file", filename=flipped_file)
            else:
                filepaths_to_download[file] = full_path

    # download!
    for infile, outfile in filepaths_to_download.items():
        callback_for_filename = functools.partial(print_callback, infile, logger, progress_chunks=[0, 25, 50, 75])
        sftp.get(infile, outfile, callback=callback_for_filename)
        logger.info("Transfer finished", filename=infile, percent=100)

    client.close()
