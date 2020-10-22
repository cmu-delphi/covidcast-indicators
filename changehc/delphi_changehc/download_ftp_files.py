"""
Downloads files modified in the last 24 hours from the specified ftp server."""

# standard
import datetime
import functools
import sys
from os import path

# third party
import paramiko

class AllowAnythingPolicy(paramiko.MissingHostKeyPolicy):
    def missing_host_key(self, client, hostname, key):
        return


def print_callback(filename, bytes_so_far, bytes_total):
    rough_percent_transferred = int(100 * (bytes_so_far / bytes_total))
    if (rough_percent_transferred % 25) == 0:
        print(f'{filename} transfer: {rough_percent_transferred}%')


def get_files_from_dir(sftp, out_path):
    current_time = datetime.datetime.now()
    seconds_in_day = 24 * 60 * 60

    # go through files in recieving dir
    files_to_download = []
    for fileattr in sftp.listdir_attr():
        file_time = datetime.datetime.fromtimestamp(fileattr.st_mtime)
        time_diff_to_current_time = current_time - file_time
        if time_diff_to_current_time.total_seconds() <= seconds_in_day:
            files_to_download.append(fileattr.filename)

    filepaths_to_download = {}
    for file in files_to_download:
        full_path = path.join(out_path, file)
        if path.exists(full_path):
            print(f"{file} exists, skipping")
        else:
            filepaths_to_download[file] = full_path

    # make sure we don't download more than 2 files per day
    assert len(files_to_download) <= 2, "more files dropped than expected"

    # download!
    for infile, outfile in filepaths_to_download.items():
        callback_for_filename = functools.partial(print_callback, infile)
        sftp.get(infile, outfile, callback=callback_for_filename)


def download(out_path, ftp_conn):

    # open client
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(AllowAnythingPolicy())

    client.connect(ftp_conn["host"], username=ftp_conn["user"],
                   password=ftp_conn["pass"][1:] + ftp_conn["pass"][0],
                   port=ftp_conn["port"],
                   allow_agent=False, look_for_keys=False)
    sftp = client.open_sftp()

    sftp.chdir('/dailycounts/All_Outpatients_By_County')
    get_files_from_dir(sftp, out_path)

    sftp.chdir('/dailycounts/Covid_Outpatients_By_County')
    get_files_from_dir(sftp, out_path)

    client.close()
