#!/usr/bin/env python3
"""Return the latest drop."""

# standard
import datetime
from pathlib import Path

def get_latest_filename(dir_path, logger, issue_date=None):
    """Get the latest filename from the list of downloaded raw files."""
    current_date = issue_date if issue_date else datetime.datetime.now()
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
    if issue_date is None:
        assert current_date.date() == latest_timestamp.date(), "no drop for today"

    logger.info("Latest claims file", filename=latest_filename)

    # return for other uses
    return latest_filename
