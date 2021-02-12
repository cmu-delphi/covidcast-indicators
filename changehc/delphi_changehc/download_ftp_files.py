"""Download files modified in the last 24 hours from the specified ftp server."""

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


def get_files_from_dir(sftp, out_path):
    """Download files from sftp server that have been uploaded in last day.

    Args:
        sftp: SFTP Session from Paramiko client
        out_path: Path to local directory into which to download the files
    """
    current_time = datetime.datetime.now()

    # go through files in recieving dir
    filepaths_to_download = {}
    for fileattr in sftp.listdir_attr():
        file_time = datetime.datetime.fromtimestamp(fileattr.st_mtime)
        filename = fileattr.filename
        if current_time - file_time < datetime.timedelta(days=1) and \
                not path.exists(path.join(out_path, filename)):
            filepaths_to_download[filename] = path.join(out_path, filename)

    # make sure we don't download more than 2 files per day
    assert len(filepaths_to_download) <= 2, "more files dropped than expected"

    # download!
    for infile, outfile in filepaths_to_download.items():
        callback_for_filename = functools.partial(print_callback, infile)
        sftp.get(infile, outfile, callback=callback_for_filename)


def download_covid(out_path, ftp_conn):
    """Download files necessary to create chng-covid signal from ftp server.

    Args:
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

        sftp.chdir('/dailycounts/All_Outpatients_By_County')
        get_files_from_dir(sftp, out_path)

        sftp.chdir('/dailycounts/Covid_Outpatients_By_County')
        get_files_from_dir(sftp, out_path)

    finally:
        if client:
            client.close()


def download_cli(out_path, ftp_conn):
    """Download files necessary to create chng-cli signal from ftp server.

    Args:
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

        sftp.chdir('/dailycounts/All_Outpatients_By_County')
        get_files_from_dir(sftp, out_path)

        sftp.chdir('/dailycounts/Flu_Patient_Count_By_County')
        get_files_from_dir(sftp, out_path)

        sftp.chdir('/dailycounts/Mixed_Patient_Count_By_County')
        get_files_from_dir(sftp, out_path)

        sftp.chdir('/dailycounts/Flu_Like_Patient_Count_By_County')
        get_files_from_dir(sftp, out_path)

        sftp.chdir('/dailycounts/Covid_Like_Patient_Count_By_County')
        get_files_from_dir(sftp, out_path)

    finally:
        if client:
            client.close()
