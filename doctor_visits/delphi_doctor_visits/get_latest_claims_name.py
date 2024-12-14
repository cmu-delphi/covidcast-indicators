#!/usr/bin/env python3
"""Return the latest drop."""

# standard
import datetime
from pathlib import Path

def get_latest_filename(dir_path, logger, issue_date=None):
    """Get the latest filename from the list of downloaded raw files."""
    if issue_date:
        current_date = datetime.datetime.strptime(issue_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
    else:
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

    assert current_date.date() == latest_timestamp.date(), f"no drop for {current_date}"

    logger.info("Latest claims file", filename=latest_filename)

    # return for other uses
    return latest_filename
