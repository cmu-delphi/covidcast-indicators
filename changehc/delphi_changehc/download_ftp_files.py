"""Download files from the specified ftp server."""

# standard
import datetime
import functools
from os import path

# third party
import paramiko


def print_callback(filename, bytes_so_far, bytes_total):
    """Log file transfer progress."""
    rough_percent_transferred = int(100 * (bytes_so_far / bytes_total))
    if (rough_percent_transferred % 25) == 0:
        print(f'{filename} transfer: {rough_percent_transferred}%')


def get_files_from_dir(sftp, filedate, out_path):
    """Download files from sftp server tagged with the specified day.

    Args:
        sftp: SFTP Session from Paramiko client
        filedate: YYYYmmdd string for which the files are named
        out_path: Path to local directory into which to download the files
    """
    # go through files in recieving dir
    filepaths_to_download = {}
    for fileattr in sftp.listdir_attr():
        filename = fileattr.filename
        if fileattr.filename.startswith(filedate) and \
                not path.exists(path.join(out_path, filename)):
            filepaths_to_download[filename] = path.join(out_path, filename)

    # make sure we don't download more than 6 files per day
    assert len(filepaths_to_download) <= 6, "more files dropped than expected"

    # download!
    for infile, outfile in filepaths_to_download.items():
        callback_for_filename = functools.partial(print_callback, infile)
        sftp.get(infile, outfile, callback=callback_for_filename)


def download_covid(filedate, out_path, ftp_conn):
    """Download files necessary to create chng-covid signal from ftp server.

    Args:
        filedate: YYYYmmdd string for which the files are named
        out_path: Path to local directory into which to download the files
        ftp_conn: Dict containing login credentials to ftp server
    """
    # open client
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        client.connect(ftp_conn["host"], username=ftp_conn["user"],
                       password=ftp_conn["pass"],
                       port=ftp_conn["port"],
                       allow_agent=False, look_for_keys=False)
        sftp = client.open_sftp()

        sftp.chdir('/countproducts')
        get_files_from_dir(sftp, filedate, out_path)

    finally:
        if client:
            client.close()


def download_cli(filedate, out_path, ftp_conn):
    """Download files necessary to create chng-cli signal from ftp server.

    Args:
        filedate: YYYYmmdd string for which the files are named
        out_path: Path to local directory into which to download the files
        ftp_conn: Dict containing login credentials to ftp server
    """
    # open client
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        client.connect(ftp_conn["host"], username=ftp_conn["user"],
                       password=ftp_conn["pass"],
                       port=ftp_conn["port"],
                       allow_agent=False, look_for_keys=False)
        sftp = client.open_sftp()

        sftp.chdir('/countproducts')
        get_files_from_dir(sftp, filedate, out_path)

    finally:
        if client:
            client.close()
