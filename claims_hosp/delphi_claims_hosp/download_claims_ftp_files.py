#!/usr/bin/env python3
"""Downloads files modified in the last 24 hours from the delphi ftp server."""

# standard
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


def print_callback(filename, logger, bytes_so_far, bytes_total):
    """Print the callback information."""
    rough_percent_transferred = int(100 * (bytes_so_far / bytes_total))
    if (rough_percent_transferred % 25) == 0:
        logger.info(f'{filename} transfer: {rough_percent_transferred}%')


def get_timestamp(name):
    """Get the reference date in datetime format."""
    try:
        split_name = name.split("_")
        yyyymmdd = split_name[3]
        hhmm = ''.join(filter(str.isdigit, split_name[4]))
        timestamp = datetime.datetime.strptime(''.join([yyyymmdd, hhmm]),
                                               "%Y%m%d%H%M")
    except Exception:
        timestamp = datetime.datetime(1900, 1, 1)

    return timestamp

def change_date_format(name):
    """Flip date from YYYYMMDD to MMDDYYYY."""
    split_name = name.split("_")
    date = split_name[3]
    flip_date = date[6:] + date[4:6] + date[:4]
    split_name[3] = flip_date
    name = '_'.join(split_name)
    return name


def download(ftp_credentials, out_path, logger):
    """Pull the latest raw files."""
    current_time = datetime.datetime.now()
    seconds_in_day = 24 * 60 * 60
    logger.info(f"current time is {current_time}")

    # open client
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(AllowAnythingPolicy())

    client.connect(ftp_credentials["host"],
                   username=ftp_credentials["user"],
                   password=ftp_credentials["pass"],
                   port=ftp_credentials["port"])
    sftp = client.open_sftp()
    sftp.chdir('/hosp/receiving')


    # go through files in recieving dir
    files_to_download = []
    for fileattr in sftp.listdir_attr():
        file_time = get_timestamp(fileattr.filename)
        time_diff_to_current_time = current_time - file_time
        if 0 < time_diff_to_current_time.total_seconds() <= seconds_in_day:
            files_to_download.append(fileattr.filename)

    # make sure we don't download more that the 3 chunked drops (2x a day) for OP
    # and the 1 chunk (2x a day) for IP - 01/07/21, *2 for multiple day drops
    assert len(files_to_download) <= 2 * ((3 * 2) + 2), "more files dropped than expected"

    filepaths_to_download = {}
    for file in files_to_download:
        flipped_file = change_date_format(file)
        if "INPATIENT" in file:
            full_path = path.join(out_path, flipped_file)
            if path.exists(full_path):
                logger.info(f"{flipped_file} exists, skipping")
            else:
                filepaths_to_download[file] = full_path

    # download!
    for infile, outfile in filepaths_to_download.items():
        callback_for_filename = functools.partial(print_callback, infile, logger)
        sftp.get(infile, outfile, callback=callback_for_filename)

    client.close()
