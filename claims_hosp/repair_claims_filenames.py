#!/usr/bin/env python3
"""Repair drops misnamed by the MMDDYYYY date flip bug.

Drops that arrived from the FTP server with an MMDDYYYY date were renamed by
change_date_format as though the date were YYYYMMDD, e.g.

    EDI_AGG_INPATIENT_08162026_0209CDT.csv.gz  (MMDDYYYY, 16 Aug 2026)
    -> EDI_AGG_INPATIENT_26200816_0209CDT.csv.gz

which get_latest_filename cannot parse, so the run dies. This renames those
files to the DDMMYYYY form the pipeline expects:

    -> EDI_AGG_INPATIENT_16082026_0209CDT.csv.gz

Only files whose date field fails to parse as DDMMYYYY and whose repaired date
does parse are touched, so it is safe to rerun and safe on a mixed directory.
Prints what it would do unless --apply is given.

Usage:
    python repair_claims_filenames.py <dir> [--apply]
"""

# standard
import argparse
import datetime
from pathlib import Path


def is_ddmmyyyy(date):
    """Check whether a date field is already in the expected DDMMYYYY format."""
    try:
        datetime.datetime.strptime(date, "%d%m%Y")
        return True
    except ValueError:
        return False


def repair_date(date):
    """Undo the bad flip of an MMDDYYYY date and redo it as DDMMYYYY.

    The bug wrote MMDDYYYY as YY + YY + MMDD, so the original month and day are
    the last four digits and the year is split across the first four.
    """
    return date[6:8] + date[4:6] + date[2:4] + date[:2]


def find_repairs(dir_path):
    """List (path, repaired name) pairs for every misnamed drop in a folder."""
    repairs = []
    for file in sorted(Path(dir_path).glob("EDI_AGG_*.csv.gz")):
        split_name = file.name.split("_")
        if len(split_name) != 5:
            continue
        date = split_name[3]
        if len(date) != 8 or not date.isdigit() or is_ddmmyyyy(date):
            continue
        repaired = repair_date(date)
        if not is_ddmmyyyy(repaired):
            continue
        split_name[3] = repaired
        repairs.append((file, "_".join(split_name)))
    return repairs


def main():
    """Rename every misnamed drop in a folder."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dir_path", help="folder holding the downloaded drops")
    parser.add_argument("--apply", action="store_true",
                        help="actually rename; otherwise just print the plan")
    args = parser.parse_args()

    repairs = find_repairs(args.dir_path)
    if not repairs:
        print("nothing to repair in %s" % args.dir_path)
        return

    for file, new_name in repairs:
        target = file.parent / new_name
        if target.exists():
            print("SKIP  %s -> %s (target exists)" % (file.name, new_name))
            continue
        print("%s  %s -> %s" % ("RENAME" if args.apply else "WOULD", file.name, new_name))
        if args.apply:
            file.rename(target)

    if not args.apply:
        print("\n%d file(s) to repair; rerun with --apply" % len(repairs))


if __name__ == "__main__":
    main()
