#!/usr/bin/env python3
"""Downloads files modified in the last 24 hours from the delphi ftp server."""

# standard
import datetime
import functools
import sys
from os import path

# third party
import click
import paramiko

# first party
from secrets import claims


class AllowAnythingPolicy(paramiko.MissingHostKeyPolicy):
    def missing_host_key(self, client, hostname, key):
        return


def print_callback(filename, bytes_so_far, bytes_total):
    rough_percent_transferred = int(100 * (bytes_so_far / bytes_total))
    if (rough_percent_transferred % 25) == 0:
        print(f'{filename} transfer: {rough_percent_transferred}%')


def get_timestamp(name):
    try:
        split_name = name.split("_")
        yyyymmdd = split_name[3]
        hhmm = ''.join(filter(str.isdigit, split_name[4]))
        timestamp = datetime.datetime.strptime(''.join([yyyymmdd, hhmm]),
                                               "%Y%m%d%H%M")
    except Exception:
        timestamp = datetime.datetime(1900, 1, 1)

    return timestamp


def flip_MMDDYYYY_to_DDMMYYYY(name):
    # flip date from MMDDYYYY to DDMMYYYY
    split_name = name.split("_")
    date = split_name[4]
    flip_date = date[2:4] + date[:2] + date[4:]
    split_name[4] = flip_date
    name = '_'.join(split_name)
    return name


def flip_YYYYMMDD_to_DDMMYYYY(name):
    split_name = name.split("_")
    date = split_name[3]
    flip_date = date[6:] + date[4:6] + date[:4]
    split_name[3] = flip_date
    name = '_'.join(split_name)
    return name


@click.command()
@click.argument("out_path")
def download(out_path):
    current_time = datetime.datetime.now()
    seconds_in_day = 24 * 60 * 60
    print(f"current time is {current_time}")

    # open client
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(AllowAnythingPolicy())

    client.connect(claims.HOST,
                   username=claims.USER, password=claims.PASS, port=claims.PORT)
    sftp = client.open_sftp()
    sftp.chdir('/hosp/receiving')


    # go through files in recieving dir
    files_to_download = []
    for fileattr in sftp.listdir_attr():
        # file_time = datetime.datetime.fromtimestamp(fileattr.st_mtime)
        file_time = get_timestamp(fileattr.filename)
        time_diff_to_current_time = current_time - file_time
        if 0 < time_diff_to_current_time.total_seconds() <= seconds_in_day:
            files_to_download.append(fileattr.filename)

    # make sure we don't download more that the 3 chunked drops (2x a day) for OP
    # and the 1 chunk (2x a day) for IP - 01/07/21, *2 for multiple day drops
    assert len(files_to_download) <= 2 * ((3 * 2) + 2), "more files dropped than expected"

    filepaths_to_download = {}
    for file in files_to_download:
        flipped_file = flip_YYYYMMDD_to_DDMMYYYY(file)
        if "INPATIENT" in file:
            full_path = path.join(out_path, flipped_file)
            if path.exists(full_path):
                print(f"{flipped_file} exists, skipping")
            else:
                filepaths_to_download[file] = full_path

    # download!
    for infile, outfile in filepaths_to_download.items():
        callback_for_filename = functools.partial(print_callback, infile)
        sftp.get(infile, outfile, callback=callback_for_filename)

    client.close()


if __name__ == "__main__":
    download()
