"""FTP created files over to Delphi Covidcast ingestion."""
# standard
import datetime
import os
from pathlib import Path

# third party
import click
import paramiko

# first party
from secrets import covidcast

NUM_FILES = 71*6*2  # expect (71 dates x 6 geos x 2 signals)
NUM_SE_FILES = 71*6*1  # expect (71 dates x 6 geos x 1 signals)


class AllowAnythingPolicy(paramiko.MissingHostKeyPolicy):
    def missing_host_key(self, client, hostname, key):
        return


@click.command()
@click.argument("local_receiving_dir")
def upload(local_receiving_dir):
    """Upload files to the delphi covidcast ingestion folders

    Args:
        local_receiving_dir: local dir containing the non-se signal files

    """
    today = datetime.datetime.now().date()

    # open client
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(AllowAnythingPolicy())
    client.connect(covidcast.HOST, username=covidcast.USER, password=covidcast.PASS)
    sftp = client.open_sftp()

    files_to_upload = []
    for file in Path(local_receiving_dir).glob("*.csv"):
        files_to_upload.append(file)

    assert len(files_to_upload) == NUM_FILES, "more files to upload than expected!"

    # upload signal without se
    sftp.chdir("/common/covidcast/receiving/hospital-admissions")
    for i, file in enumerate(files_to_upload):
        assert (
                datetime.datetime.fromtimestamp(os.path.getmtime(file)).date() == today
        ), f"uploading old file {file}"

        sftp.put(file, file.name)
        if (i % 61) == 0:
            print(f"Finished {i} out of {len(files_to_upload)}")

    print(f"Successfully uploaded the hospital-admissions claims signal")
    client.close()


if __name__ == "__main__":
    upload()
