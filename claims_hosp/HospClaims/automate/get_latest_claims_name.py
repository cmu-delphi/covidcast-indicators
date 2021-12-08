#!/usr/bin/env python3
"""Return the latest drop."""

# standard
import datetime
from pathlib import Path

# third party
import click


@click.command()
@click.argument("dir_path")
def get_latest_filename(dir_path):
    current_date = datetime.datetime.now()
    files = list(Path(dir_path).glob("*"))

    latest_timestamp = datetime.datetime(1900, 1, 1)
    latest_filename = None
    for file in files:
        split_name = file.name.split("_")
        if len(split_name) == 5:
            ddmmyyyy = split_name[3]
            hhmm = ''.join(filter(str.isdigit, split_name[4]))
            timestamp = datetime.datetime.strptime(''.join([ddmmyyyy, hhmm]),
                                                   "%d%m%Y%H%M")
            if timestamp > latest_timestamp:
                if timestamp <= current_date:
                    latest_timestamp = timestamp
                    latest_filename = file

    assert current_date.date() == latest_timestamp.date(), "no drop for today"

    # write to stdout for shell script to use
    print(latest_filename)

    # return for other uses
    return latest_filename


if __name__ == "__main__":
    get_latest_filename()
