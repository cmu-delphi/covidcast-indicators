# -*- coding: utf-8 -*-
"""Functions to call when downloading data."""
from dataclasses import dataclass
import datetime
import os
import re
from urllib.parse import quote_plus as quote_as_url

import pandas as pd
import requests

from delphi_utils.geomap import GeoMapper

from .constants import TRANSFORMS, SIGNALS
from .constants import DOWNLOAD_ATTACHMENT, DOWNLOAD_LISTING

# YYYYMMDD
# example: "Community Profile Report 20211104.xlsx"
RE_DATE_FROM_FILENAME = re.compile(r'.*([0-9]{4})([0-9]{2})([0-9]{2}).*xlsx')

# example: "TESTING: LAST WEEK (October 24-30, Test Volume October 20-26)"
# example: "TESTING: PREVIOUS WEEK (October 17-23, Test Volume October 13-19)"
DATE_EXP = r'(?:(.*) )?([0-9]{1,2})'
DATE_RANGE_EXP = f"{DATE_EXP}-{DATE_EXP}"
RE_DATE_FROM_HEADER = re.compile(
    rf'TESTING: (.*) WEEK \({DATE_RANGE_EXP}, Test Volume ({DATE_RANGE_EXP})\)'
)

# example: "NAAT positivity rate - last 7 days (may be an underestimate due to delayed reporting)"
# example: "Total NAATs - last 7 days (may be an underestimate due to delayed reporting)"
RE_COLUMN_FROM_HEADER = re.compile('- (.*) 7 days')

@dataclass
class DatasetTimes:
    """Collect reference dates for a column."""

    column: str
    positivity_reference_date: datetime.date
    total_reference_date: datetime.date
    def __getitem__(self, key):
        """Use DatasetTimes like a dictionary."""
        if key.lower()=="positivity":
            return self.positivity_reference_date
        if key.lower()=="total":
            return self.total_reference_date
        raise ValueError(
            f"Bad reference date type request '{key}'; need 'total' or 'positivity'"
        )
    def __eq__(self, other):
        """Check equality by value."""
        return isinstance(other, DatasetTimes) and \
            other.column == self.column and \
            other.positivity_reference_date == self.positivity_reference_date and \
            other.total_reference_date == self.total_reference_date

def as_reference_date(header, year=2021):
    """Convert reference dates in overheader to DatasetTimes."""
    findall_result = RE_DATE_FROM_HEADER.findall(header)[0]
    def as_date(sub_result):
        month = sub_result[2] if sub_result[2] else sub_result[0]
        day = sub_result[3]
        return datetime.datetime.strptime(f"{year}-{month}-{day}", "%Y-%B-%d").date()
    column = findall_result[0].lower()
    return DatasetTimes(column, as_date(findall_result[1:5]), as_date(findall_result[6:10]))

class Dataset:
    """All data extracted from a single report file."""

    def __init__(self, config, sheets=TRANSFORMS.keys(), logger=None):
        """Create a new Dataset instance.

        Download and cache the requested report file.

        Parse the file into data frames at multiple geo levels.
        """
        self.publish_date = datetime.date(
            *[int(x) for x in RE_DATE_FROM_FILENAME.findall(config['filename'])[0]]
        )

        self.url = DOWNLOAD_ATTACHMENT.format(
            asset_id=config['assetId'],
            filename=quote_as_url(config['filename'])
        )
        if logger:
            logger.info("Downloading file", filename=config['cached_filename'])
        resp = requests.get(self.url)
        with open(config['cached_filename'], 'wb') as f:
            f.write(resp.content)

        self.workbook = pd.ExcelFile(config['cached_filename'])

        self.dfs = {}
        self.times = {}
        for si in sheets:
            assert si in TRANSFORMS, f"Bad sheet requested: {si}"
            if logger:
                logger.info("Building dfs",
                            sheet=f"{si}",
                            filename=config['cached_filename'])
            sheet = TRANSFORMS[sheet]
            self._parse_times_for_sheet(sheet)
            self._parse_sheet(sheet)

    @staticmethod
    def skip_overheader(header):
        """Ignore irrelevant overheaders."""
        # include "TESTING: LAST WEEK (October 24-30, Test Volume October 20-26)"
        # include "TESTING: PREVIOUS WEEK (October 17-23, Test Volume October 13-19)"
        return not (isinstance(header, str) and header.startswith("TESTING:") \
                    # exclude "TESTING: % CHANGE FROM PREVIOUS WEEK" \
                    # exclude "TESTING: DEMOGRAPHIC DATA" \
                    and header.find("WEEK (") > 0)
    def _parse_times_for_sheet(self, sheet):
        """Record reference dates for this sheet."""
        # grab reference dates from overheaders
        for h in pd.read_excel(
                self.workbook, sheet_name=sheet.name,
                header=None,
                nrows=1
        ).values.flatten().tolist():
            if self.skip_overheader(h):
                continue

            dt = as_reference_date(h)
            if dt.column in self.times:
                assert self.times[dt.column] == dt, \
                    f"Conflicting reference date from {sheet.name} {dt}" + \
                    f"vs previous {self.times[dt.column]}"
            else:
                self.times[dt.column] = dt

    @staticmethod
    def retain_header(header):
        """Ignore irrelevant headers."""
        return all([
            # include "Total NAATs - last 7 days ..."
            # include "Total NAATs - previous 7 days ..."
            # include "NAAT positivity rate - last 7 days ..."
            # include "NAAT positivity rate - previous 7 days ..."
            (header.startswith("Total NAATs") or header.startswith("NAAT positivity rate")),
            # exclude "NAAT positivity rate - absolute change ..."
            header.find("7 days") > 0,
            # exclude "NAAT positivity rate - last 7 days - ages <5"
            header.find(" ages") < 0,
        ])
    def _parse_sheet(self, sheet):
        """Extract data frame for this sheet."""
        df = pd.read_excel(
            self.workbook,
            sheet_name=sheet.name,
            header=1,
            index_col=0,
        )
        if sheet.row_filter:
            df = df.loc[sheet.row_filter(df)]
        select = [
            (RE_COLUMN_FROM_HEADER.findall(h)[0], h, h.lower())
            for h in list(df.columns)
            if self.retain_header(h)
        ]
        for sig in SIGNALS:
            sig_select = [s for s in select if s[-1].find(sig) >= 0]
            self.dfs[(sheet.level, sig)] = pd.concat([
                pd.DataFrame({
                    "geo_id": sheet.geo_id_select(df).apply(sheet.geo_id_apply),
                    "timestamp": pd.to_datetime(self.times[si[0]][sig]),
                    "val": df[si[-2]],
                    "se": None,
                    "sample_size": None
                })
                for si in sig_select
            ])
        self.dfs[(sheet.level, "total")]["val"] /= 7 # 7-day total -> 7-day average


def as_cached_filename(params, config):
    """Formulate a filename to uniquely identify this report in the input cache."""
    return os.path.join(
        params['indicator']['input_cache'],
        f"{config['assetId']}--{config['filename']}"
    )

def fetch_listing(params):
    """Generate the list of report files to process."""
    listing = requests.get(DOWNLOAD_LISTING).json()['metadata']['attachments']

    # drop the pdf files
    listing = [
        dict(el, cached_filename=as_cached_filename(params, el))
        for el in listing if el['filename'].endswith("xlsx")
    ]

    # drop files we already have in the input cache
    listing = [el for el in listing if os.path.exists(el['cached_filename'])]
    return listing

def download_and_parse(listing, logger):
    """Convert a list of report files into Dataset instances."""
    datasets = {}
    for item in listing:
        d = Dataset(item, logger=logger)
        for sig, df in d.dfs.items():
            if sig not in datasets:
                datasets[sig] = []
            datasets[sig].append(df)
    return datasets

def fetch_new_reports(params, logger=None):
    """Retrieve, compute, and collate all data we haven't seen yet."""
    listing = fetch_listing(params)

    # download and parse individual reports
    datasets = download_and_parse(listing, logger)

    # collect like signals together
    ret = {}
    for sig, lst in datasets.items():
        ret[sig] = pd.concat(lst)

    # add nation from state
    geomapper = GeoMapper()
    for sig in SIGNALS:
        df = geomapper.replace_geocode(
            ret[("state", sig)].rename(columns={"geo_id":"state_code"}),
            'state_code',
            'nation',
            new_col="geo_id"
        )
        ret[("nation", sig)] = df

    return ret
