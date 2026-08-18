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

EPOCH = datetime.datetime(1900, 1, 1)

# a drop filename is <source>_AGG_INPATIENT_[<chunk>_]<date>[_]<time><tz><ext>, where:
# - the chunk number is only present on chunked drops
# - the date is 8 digits, either YYYYMMDD or MMDDYYYY depending on the drop
# - the separator between date and time is optional (dropped by the source in 2026)
FILENAME_TIMESTAMP = re.compile(
    r"^(?P<prefix>.*EDI_AGG_INPATIENT)_(?P<chunk>[0-9]_)?"
    r"(?P<ymd>[0-9]{8})_?(?P<hm>[0-9]{4})(?P<suffix>[^0-9].*)?$")

def get_timestamp(name):
    """Get the reference date in datetime format."""
    m = FILENAME_TIMESTAMP.match(name)
    if not m:
        return EPOCH
    stamp = m.group("ymd") + m.group("hm")
    # MMDD as a year is always before 1231, so YYYYMMDD is the only reading that can parse
    for date_format in ("%Y%m%d%H%M", "%m%d%Y%H%M"):
        try:
            return datetime.datetime.strptime(stamp, date_format)
        except ValueError:
            continue
    return EPOCH

def change_date_format(name):
    """Rewrite the date field to DDMMYYYY, the layout get_latest_filename expects."""
    m = FILENAME_TIMESTAMP.match(name)
    timestamp = get_timestamp(name)
    if m is None or timestamp == EPOCH:
        return name
    prefix = m.group("prefix")
    chunk = m.group("chunk") or ""
    suffix = m.group("suffix") or ""
    return f"{prefix}_{chunk}{timestamp:%d%m%Y}_{timestamp:%H%M}{suffix}"


def download(ftp_credentials, out_path, logger):
    """Pull the latest raw files."""
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
