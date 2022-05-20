# -*- coding: utf-8 -*-
"""Functions to call when downloading data."""
from dataclasses import dataclass
import datetime
import os
import re
from typing import Dict, Tuple
from urllib.parse import quote_plus as quote_as_url

import pandas as pd
import numpy as np
import requests

from delphi_utils.geomap import GeoMapper

from .constants import (
    TRANSFORMS, SIGNALS, COUNTS_7D_SIGNALS, NEWLINE,
    IS_PROP, NOT_PROP,
    DOWNLOAD_ATTACHMENT, DOWNLOAD_LISTING,
    INTERP_LENGTH
)

DataDict = Dict[Tuple[str, str, bool], pd.DataFrame]

# YYYYMMDD
# example: "Community Profile Report 20211104.xlsx"
RE_DATE_FROM_FILENAME = re.compile(r'.*([0-9]{4})([0-9]{2})([0-9]{2}).*xlsx')

# example: "TESTING: LAST WEEK (October 24-30, Test Volume October 20-26)"
# example: "TESTING: PREVIOUS WEEK (October 17-23, Test Volume October 13-19)"
DATE_EXP = r'(?:([A-Za-z]*) )?([0-9]{1,2})'
DATE_RANGE_EXP = f"{DATE_EXP}-{DATE_EXP}"
RE_DATE_FROM_TEST_HEADER = re.compile(
    rf'.*TESTING: (.*) WEEK \({DATE_RANGE_EXP}(?:, Test Volume ({DATE_RANGE_EXP}))? *\)'
)

# example: "HOSPITAL UTILIZATION: LAST WEEK (January 2-8)"
RE_DATE_FROM_HOSP_HEADER = re.compile(
    rf'HOSPITAL UTILIZATION: (.*) WEEK \({DATE_RANGE_EXP}\)'
)

# example: "COVID-19 VACCINATION DATA: LAST WEEK (January 5-11)"
RE_DATE_FROM_VAC_HEADER_WEEK= re.compile(
    rf'COVID-19 VACCINATION DATA: (.*) WEEK \({DATE_RANGE_EXP}\)'
)

# example: 'COVID-19 VACCINATION DATA: CUMULATIVE (January 11)'
RE_DATE_FROM_VAC_HEADER_CUMULATIVE= re.compile(
    rf'COVID-19 VACCINATION DATA: CUMULATIVE (.*)\({DATE_EXP}\)'
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
    hosp_reference_date: datetime.date
    vac_reference_date: datetime.date
    cumulative_vac_reference_date: datetime.date

    @staticmethod
    def from_header(header, publish_date):
        """Convert reference dates in overheader to DatasetTimes."""
        positivity_reference_date = None
        total_reference_date = None
        hosp_reference_date = None
        vac_reference_date = None
        cumulative_vac_reference_date= None
        def as_date(sub_result, is_single_date):
            if is_single_date:
                month = sub_result[0]
                day = sub_result[1]
                month_numeric = datetime.datetime.strptime(month, "%B").month
            else:
                month = sub_result[2] if sub_result[2] else sub_result[0]
                assert month, f"Bad month in header: {header}\nsub_result: {sub_result}"
                month_numeric = datetime.datetime.strptime(month, "%B").month
                day = sub_result[3]
            year = publish_date.year
            # year boundary
            if month_numeric > publish_date.month:
                year -= 1
            return datetime.datetime.strptime(f"{year}-{month}-{day}", "%Y-%B-%d").date()

        if RE_DATE_FROM_TEST_HEADER.match(header):
            findall_result = RE_DATE_FROM_TEST_HEADER.findall(header)[0]
            column = findall_result[0].lower()
            positivity_reference_date = as_date(findall_result[1:5], False)
            if findall_result[6]:
                # Reports published starting 2021-03-17 specify different reference
                # dates for positivity and total test volume
                total_reference_date = as_date(findall_result[6:10], False)
            else:
                total_reference_date = positivity_reference_date
        elif RE_DATE_FROM_HOSP_HEADER.match(header):
            findall_result = RE_DATE_FROM_HOSP_HEADER.findall(header)[0]
            column = findall_result[0].lower()
            hosp_reference_date = as_date(findall_result[1:5], False)
        elif RE_DATE_FROM_VAC_HEADER_WEEK.match(header):
            findall_result = RE_DATE_FROM_VAC_HEADER_WEEK.findall(header)[0]
            column = findall_result[0].lower()
            vac_reference_date = as_date(findall_result[1:5], False)
        elif RE_DATE_FROM_VAC_HEADER_CUMULATIVE.match(header):
            findall_result = RE_DATE_FROM_VAC_HEADER_CUMULATIVE.findall(header)[0]
            column = findall_result[0].lower()
            cumulative_vac_reference_date = as_date(findall_result[1:], True)
        else:
            raise ValueError(f"Couldn't find reference date in header '{header}'")
        return DatasetTimes(column, positivity_reference_date,
            total_reference_date, hosp_reference_date,
            cumulative_vac_reference_date, vac_reference_date)
    def __getitem__(self, key):
        """Use DatasetTimes like a dictionary."""
        ref_list = list(SIGNALS.keys())
        if key.lower()=="positivity":
            return self.positivity_reference_date
        if key.lower()=="total":
            return self.total_reference_date
        if key.lower()=="confirmed covid-19 admissions":
            return self.hosp_reference_date
        if key.lower() in ["doses administered","booster doses administered"]:
            return self.cumulative_vac_reference_date
        if key.lower() in ["fully vaccinated","booster dose since"]:
            return self.vac_reference_date
        raise ValueError(
            f"Bad reference date type request '{key}'; " + \
            "need one of: " + " ,".join(ref_list)
        )
    def __setitem__(self, key, newvalue):
        """Use DatasetTimes like a dictionary."""
        ref_list = list(SIGNALS.keys())
        if key.lower()=="positivity":
            self.positivity_reference_date = newvalue
        if key.lower()=="total":
            self.total_reference_date = newvalue
        if key.lower()=="confirmed covid-19 admissions":
            self.hosp_reference_date = newvalue
        if key.lower() in ["doses administered","booster doses administered"]:
            self.cumulative_vac_reference_date = newvalue
        if key.lower() in ["fully vaccinated","booster dose since"]:
            self.vac_reference_date = newvalue
        if key.lower() not in ref_list:
            raise ValueError(
                f"Bad reference date type request '{key}'; " + \
                "need one of: " + " ,".join(ref_list)
            )
    def __eq__(self, other):
        """Check equality by value."""
        return isinstance(other, DatasetTimes) and \
            other.column == self.column and \
            other.positivity_reference_date == self.positivity_reference_date and \
            other.total_reference_date == self.total_reference_date

class Dataset:
    """All data extracted from a single report file."""

    def __init__(self, config, sheets=TRANSFORMS.keys(), logger=None):
        """Create a new Dataset instance.

        Download and cache the requested report file.

        Parse the file into data frames at multiple geo levels.
        """
        self.publish_date = self.parse_publish_date(config['filename'])
        self.url = DOWNLOAD_ATTACHMENT.format(
            assetId=config['assetId'],
            filename=quote_as_url(config['filename'])
        )
        if not os.path.exists(config['cached_filename']):
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
            sheet = TRANSFORMS[si]
            self._parse_times_for_sheet(sheet)
            self._parse_sheet(sheet)

    @staticmethod
    def parse_publish_date(report_filename):
        """Extract publish date from filename."""
        return datetime.date(
            *[int(x) for x in RE_DATE_FROM_FILENAME.findall(report_filename)[0]]
        )
    @staticmethod
    def skip_overheader(header):
        """Ignore irrelevant overheaders."""
        # include "TESTING: [LAST|PREVIOUS] WEEK (October 24-30, Test Volume October 20-26)"
        # include "VIRAL (RT-PCR) LAB TESTING: [LAST|PREVIOUS] WEEK (August 24-30, ..."
        # include "HOSPITAL UTILIZATION: LAST WEEK (January 2-8)"
        return not (isinstance(header, str) and \
                    (((header.startswith("TESTING:") or \
                     header.startswith("VIRAL (RT-PCR) LAB TESTING:") or \
                     header.startswith("HOSPITAL UTILIZATION: ")) and \
                    # exclude "TESTING: % CHANGE FROM PREVIOUS WEEK" \
                    # exclude "TESTING: DEMOGRAPHIC DATA" \
                    # exclude "HOSPITAL UTILIZATION: CHANGE FROM PREVIOUS WEEK" \
                    # exclude "HOSPITAL UTILIZATION: DEMOGRAPHIC DATA" \
                    header.find("WEEK (") > 0) or \
                    # include "COVID-19 VACCINATION DATA: CUMULATIVE (January 25)"
                    # include "COVID-19 VACCINATION DATA: LAST WEEK (January 25-31)"
                    (header.startswith("COVID-19 VACCINATION DATA: CUMULATIVE") or
                    header.startswith("COVID-19 VACCINATION DATA: LAST WEEK") \
                        )))


    def _parse_times_for_sheet(self, sheet):
        """Record reference dates for this sheet."""
        # grab reference dates from overheaders
        overheaders = pd.read_excel(
                self.workbook, sheet_name=sheet.name,
                header=None,
                nrows=1
        ).values.flatten().tolist()
        for h in overheaders:
            if self.skip_overheader(h):
                continue

            dt = DatasetTimes.from_header(h, self.publish_date)
            if dt.column in self.times:
                # Items that are not None should be the same between sheets.
                # Fill None items with the newly calculated version of the
                # field from dt.
                for sig in SIGNALS:
                    if self.times[dt.column][sig] is not None and dt[sig] is not None:
                        assert self.times[dt.column][sig] == dt[sig], \
                            f"Conflicting reference date from {sheet.name} {dt[sig]}" + \
                            f"vs previous {self.times[dt.column][sig]}"
                    elif self.times[dt.column][sig] is None:
                        self.times[dt.column][sig] = dt[sig]
            else:
                self.times[dt.column] = dt

        if self.publish_date <= datetime.date(2021, 1, 11):
            # No vaccination data available, so we only have hospitalization and testing overheaders
            assert len(self.times) == 2, \
                f"No times extracted from overheaders:\n{NEWLINE.join(str(s) for s in overheaders)}"
        else:
            assert len(self.times) == 3, \
                f"No times extracted from overheaders:\n{NEWLINE.join(str(s) for s in overheaders)}"

    @staticmethod
    def retain_header(header):
        """Ignore irrelevant headers."""
        return all([
            # include "Total NAATs - [last|previous] 7 days ..."
            # include "Total RT-PCR diagnostic tests - [last|previous] 7 days ..."
            # include "NAAT positivity rate - [last|previous] 7 days ..."
            # include "Viral (RT-PCR) lab test positivity rate - [last|previous] 7 days ..."
            # include "Booster doses administered - [last|previous] 7 days ..."
            # include "Doses administered - [last|previous] 7 days ..."
            (header.startswith("Total NAATs") or
             header.startswith("NAAT positivity rate") or
             header.startswith("Total RT-PCR") or
             header.startswith("Viral (RT-PCR)") or
             header.startswith("Booster") or
             header.startswith("Doses administered -")
             ),
            # exclude "NAAT positivity rate - absolute change ..."
            header.find("7 days") > 0,
            # exclude "NAAT positivity rate - last 7 days - ages <5"
            header.find(" ages") < 0,
        ]) or (
            # include "Confirmed COVID-19 admissions - last 7 days"
            # exclude "Confirmed COVID-19 admissions - percent change"
            # exclude "Confirmed COVID-19 admissions - last 7 days - ages <18"
            # exclude "Confirmed COVID-19 admissions - last 7 days - age unknown"
            # exclude "Confirmed COVID-19 admissions per 100 inpatient beds - last 7 days"
            # exclude "Confirmed COVID-19 admissions per 100k - last 7 days"
            header == "Confirmed COVID-19 admissions - last 7 days"
        ) or all([
            # include "People who are fully vaccinated"
            # include "People who have received a booster dose since August 13, 2021"
            header.startswith("People who"),
            # exclude "People who are fully vaccinated as % of total population"
            # exclude "People who have received a booster dose as % of fully vaccinated population"
            header.find("%") < 0,
            # exclude "People who are fully vaccinated - ages 5-11" ...
            # exclude "People who have received a booster dose - ages 65+" ...
            header.find(" age") < 0,
            # exclude "People who are fully vaccinated - 12-17" ...
            header.find("-") < 0,
        ]) or all([
            # include "People with full course administered"
            header.startswith("People with full course"),
            # exclude "People with full course administered as % of adult population"
            header.find("%") < 0,
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


        def select_fn(h):
            """Allow for default to the 7-day in the name of the dataframe column."""
            try:
                return (RE_COLUMN_FROM_HEADER.findall(h)[0], h, h.lower())
            except IndexError:
                return ("", h, h.lower())

        select = [
            select_fn(h)
            for h in list(df.columns)
            if self.retain_header(h)
        ]

        for sig in SIGNALS:
            ## Check if field is known to be missing
            # Hospital admissions not available at the county or CBSA level prior to Jan 8, 2021.
            is_hosp_adm_before_jan8 = (sheet.level == "msa" or sheet.level == "county") \
                and self.publish_date < datetime.date(2021, 1, 8) \
                and sig == "confirmed covid-19 admissions"
            # Booster data not available before November 1 2021.
            is_booster_before_nov1 = self.publish_date < datetime.date(2021, 11, 1) \
                and (sig in ["booster dose since", "booster doses administered"])
            # Booster and weekly doses administered not available below the state level.
            is_booster_below_state = ((sheet.level != "hhs" and sheet.level != "state") \
                and (sig in ["doses administered", \
                 "booster doses administered", "booster dose since"]))
            # Weekly doses administered not available on or before Apr 29, 2021.
            is_dose_admin_apr29 = self.publish_date <= datetime.date(2021, 4, 29) \
                and sig == "doses administered"
            # People fully vaccinated not available on or before Apr 11, 2021 at the CBSA level.
            is_fully_vax_msa_before_apr11 = (sheet.level == "msa" or sheet.level == "county") \
                and self.publish_date <= datetime.date(2021, 4, 11) \
                and sig == "fully vaccinated"
            # People fully vaccinated not available before Jan 15, 2021 at any geo level.
            is_fully_vax_before_jan14 = self.publish_date <= datetime.date(2021, 1, 14) \
                and sig == "fully vaccinated"

            if any([is_hosp_adm_before_jan8,
                is_booster_before_nov1,
                is_booster_below_state,
                is_dose_admin_apr29,
                is_fully_vax_msa_before_apr11,
                is_fully_vax_before_jan14
            ]):
                self.dfs[(sheet.level, sig, NOT_PROP)] = pd.DataFrame(
                        columns = ["geo_id", "timestamp", "val", \
                            "se", "sample_size", "publish_date"]
                    )
                continue

            sig_select = [s for s in select if s[-1].find(sig) >= 0]
            # The name of the cumulative vaccination was changed after 03/09/2021
            # when J&J vaccines were added.
            if (sig == "fully vaccinated") and (len(sig_select) == 0):
                sig_select = [s for s in select if s[-1].find("people with full course") >= 0]
            # Since "doses administered" is a substring of another desired header,
            # "booster doses administered", we need to more strictly check if "doses administered"
            # occurs at the beginning of a header to find the correct match.
            if sig == "doses administered":
                sig_select = [s for s in select if s[-1].startswith(sig)]
            assert len(sig_select) > 0, \
                f"No {sig} in any of {select}\n\nAll headers:\n{NEWLINE.join(list(df.columns))}"

            self.dfs[(sheet.level, sig, NOT_PROP)] = pd.concat([
                pd.DataFrame({
                    "geo_id": sheet.geo_id_select(df).apply(sheet.geo_id_apply),
                    "timestamp": pd.to_datetime(self.times[si[0]][sig]),
                    "val": df[si[-2]],
                    "se": None,
                    "sample_size": None,
                    "publish_date": self.publish_date
                })
                for si in sig_select
            ])

        for sig in COUNTS_7D_SIGNALS:
            assert (sheet.level, sig, NOT_PROP) in self.dfs.keys()
            self.dfs[(sheet.level, sig, NOT_PROP)]["val"] /= 7 # 7-day total -> 7-day average

def as_cached_filename(params, config):
    """Formulate a filename to uniquely identify this report in the input cache."""
    # eg "Community Profile Report 20220128.xlsx"
    # but delimiters vary; don't get tripped up if they do something wacky like
    # Community.Profile.Report.20220128.xlsx
    name, _, ext = config['filename'].rpartition(".")
    return os.path.join(
        params['indicator']['input_cache'],
        f"{name}--{config['assetId']}.{ext}"
    )

def fetch_listing(params):
    """Generate the list of report files to process."""
    export_start_date = params['indicator'].get(
        'export_start_date', datetime.datetime.utcfromtimestamp(0).date()
    )

    listing = requests.get(DOWNLOAD_LISTING).json()['metadata']['attachments']
    # drop the pdf files
    listing = [
        dict(
            el,
            cached_filename=as_cached_filename(params, el),
            publish_date=Dataset.parse_publish_date(el['filename'])
        )
        for el in listing if el['filename'].endswith("xlsx")
    ]

    def check_valid_publish_date(x):
        return x['publish_date'] >= export_start_date

    if params['indicator']['reports'] == 'new':
        # drop files we already have in the input cache
        keep = [
            el for el in listing
            if not os.path.exists(el['cached_filename']) and check_valid_publish_date(el)
        ]
    elif params['indicator']['reports'].find("--") > 0:
        # drop files outside the specified publish-date range
        start_str, _, end_str = params['indicator']['reports'].partition("--")
        start_date = datetime.datetime.strptime(start_str, "%Y-%m-%d").date()
        end_date = datetime.datetime.strptime(end_str, "%Y-%m-%d").date()
        keep = [
            el for el in listing
            if (start_date <= el['publish_date'] <= end_date) and check_valid_publish_date(el)
        ]
    elif params['indicator']['reports'] == 'all':
        keep = [
            el for el in listing if check_valid_publish_date(el)
        ]
    else:
        raise ValueError("params['indicator']['reports'] is set to" \
            + f" {params['indicator']['reports']}, which isn't 'new', 'all', or a date range.")

    return extend_listing_for_interp(keep, listing)

def extend_listing_for_interp(keep, listing):
    """Grab additional files from the full listing for interpolation if needed.

    Selects files based purely on publish_date, so may include duplicates where
    multiple reports for a single publish_date are available.

    Parameters:
     - keep: list of reports desired in the final output
     - listing: complete list of reports available from healthdata.gov

    Returns: list of reports including keep and additional files needed for
    interpolation.
    """
    publish_date_keeplist = set()
    for el in keep:
        # starts at 0 so includes keep publish_dates
        for i in range(INTERP_LENGTH):
            publish_date_keeplist.add(el['publish_date'] - datetime.timedelta(days=i))
    keep = [el for el in listing if el['publish_date'] in publish_date_keeplist]
    return keep

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

def nation_from_state(df, sig, geomapper):
    """Compute nation level from state df."""
    if df.empty:
        return df
    if SIGNALS[sig]["is_rate"]: # true if sig is a rate
        df = geomapper.add_population_column(df, "state_id") \
                      .rename(columns={"population":"weight"})

        norm_denom = df.groupby("timestamp").agg(norm_denom=("weight", "sum"))
        df = df.join(
            norm_denom, on="timestamp", how="left"
        ).assign(
            weight=lambda x: x.weight / x.norm_denom
        ).drop(
            "norm_denom", axis=1
        )
    # The filter in `fetch_new_reports` to keep most recent publish date
    # gurantees that we'll only see one unique publish date per timestamp
    # here, so just keep the first obs of each group.
    publish_date_by_ts = df.groupby(
        ["timestamp"]
    )["publish_date"].first(
    ).reset_index(
    )
    df = geomapper.replace_geocode(
        df.drop("publish_date", axis=1),
        'state_id',
        'nation',
        new_col="geo_id"
    )
    df["se"] = None
    df["sample_size"] = None
    # Recreate publish_date column
    df =  pd.merge(df, publish_date_by_ts, on="timestamp", how="left")

    return df

def keep_latest_report(df, sig):
    """Keep data associated with most recent report for each timestamp."""
    df = df.groupby(
            "timestamp"
        ).apply(
            lambda x: x[x["publish_date"] == x["publish_date"].max()]
        ).drop_duplicates(
        )

    if not df.empty:
        df = df.reset_index(drop=True)
        assert all(df.groupby(
                ["timestamp", "geo_id"]
            ).size(
            ).reset_index(
                drop=True
            ) == 1), f"Duplicate rows in {sig} indicate that one or" \
            + " more reports were published multiple times and the copies differ"

    return df

def fetch_new_reports(params, logger=None):
    """Retrieve, compute, and collate all data we haven't seen yet."""
    listing = fetch_listing(params)

    # download and parse individual reports
    datasets = download_and_parse(listing, logger)
    # collect like signals together, keeping most recent publish date
    ret = {}

    for key, lst in datasets.items():
        (_, sig, _) = key
        latest_key_df = pd.concat(lst)
        if sig in ("total", "positivity"):
            latest_key_df = pd.concat(apply_thres_change_date(
                keep_latest_report,
                latest_key_df,
                [sig] * 2
            ))
        else:
            latest_key_df = keep_latest_report(latest_key_df, sig)

        if not latest_key_df.empty:
            ret[key] = latest_key_df

    # add nation from state
    geomapper = GeoMapper()
    for sig in SIGNALS:
        state_key = ("state", sig, NOT_PROP)
        if state_key not in ret:
            continue

        if sig in ("total", "positivity"):
            nation_df = pd.concat(apply_thres_change_date(
                nation_from_state,
                ret[state_key].rename(columns={"geo_id": "state_id"}),
                [sig] * 2,
                [geomapper] * 2
            ))
        else:
            nation_df = nation_from_state(
                ret[state_key].rename(columns={"geo_id": "state_id"}),
                sig,
                geomapper
            )
        ret[("nation", sig, NOT_PROP)] = nation_df

    for key, df in ret.copy().items():
        (geo, sig, prop) = key

        if sig == "positivity":
            # Combine with test volume using publish date.
            total_key = (geo, "total", prop)
            ret[key] = unify_testing_sigs(
                df, ret[total_key]
            ).drop(
                "publish_date", axis=1
            )

            # No longer need "total" signal.
            del ret[total_key]
        elif sig != "total":
            # If signal is not test volume or test positivity, we don't need
            # publish date.
            df = df.drop("publish_date", axis=1)
            ret[key] = df

        if SIGNALS[sig]["make_prop"]:
            ret[(geo, sig, IS_PROP)] = generate_prop_signal(df, geo, geomapper)

    ret = interpolate_missing_values(ret)

    return ret

def interpolate_missing_values(dfs: DataDict) -> DataDict:
    """Interpolates each signal in the dictionary of dfs."""
    interpolate_df = dict()
    for key, df in dfs.items():
        # Here we exclude the 'positivity' signal from interpolation. This is a temporary fix.
        # https://github.com/cmu-delphi/covidcast-indicators/issues/1576
        _, sig, _ = key
        if sig == "positivity":
            reindexed_group_df = df.set_index(["geo_id", "timestamp"]).sort_index().reset_index()
            interpolate_df[key] = reindexed_group_df[~reindexed_group_df.val.isna()]
            continue

        geo_dfs = []
        for geo, group_df in df.groupby("geo_id"):
            reindexed_group_df = group_df.set_index("timestamp").reindex(
                pd.date_range(group_df.timestamp.min(), group_df.timestamp.max())
            )
            reindexed_group_df["geo_id"] = geo
            if "val" in reindexed_group_df.columns and not reindexed_group_df["val"].isna().all():
                reindexed_group_df["val"] = (
                    reindexed_group_df["val"]
                    .astype(float)
                    .interpolate(method="linear", limit_area="inside")
                )
            if "se" in reindexed_group_df.columns:
                reindexed_group_df["se"] = (
                    reindexed_group_df["se"]
                    .astype(float)
                    .interpolate(method="linear", limit_area="inside")
                )
            if (
                "sample_size" in reindexed_group_df.columns
                and not reindexed_group_df["sample_size"].isna().all()
            ):
                reindexed_group_df["sample_size"] = (
                    reindexed_group_df["sample_size"]
                    .astype(float)
                    .interpolate(method="linear", limit_area="inside")
                )
            if "publish_date" in reindexed_group_df.columns:
                reindexed_group_df["publish_date"] = reindexed_group_df["publish_date"].fillna(
                    method="bfill"
                )
            reindexed_group_df = reindexed_group_df[~reindexed_group_df.val.isna()]
            geo_dfs.append(reindexed_group_df)
        interpolate_df[key] = (
            pd.concat(geo_dfs)
            .reset_index()
            .rename(columns={"index": "timestamp"})
            .set_index(["geo_id", "timestamp"])
            .sort_index()
            .reset_index()
        )
    return interpolate_df

def generate_prop_signal(df, geo, geo_mapper):
    """Transform base df into a proportion (per 100k population)."""
    if geo == "state":
        geo = "state_id"
    if geo == "county":
        geo = "fips"

    # Add population data
    if geo == "msa":
        map_df = geo_mapper.get_crosswalk("fips", geo)
        map_df = geo_mapper.add_population_column(
            map_df, "fips"
        ).drop(
            "fips", axis=1
        ).groupby(
            geo
        ).sum(
        ).reset_index(
        )
        df = pd.merge(df, map_df, left_on="geo_id", right_on=geo, how="inner")
    else:
        df = geo_mapper.add_population_column(df, geo, geocode_col="geo_id")

    df["val"] = df["val"] / df["population"] * 100000
    df.drop(["population", geo], axis=1, inplace=True)

    return df

def unify_testing_sigs(positivity_df, volume_df):
    """
    Drop any observations with a sample size of 5 or less. Generate standard errors.

    This combines test positivity and testing volume into a single signal,
    where testing volume *from the same spreadsheet/publish date* (NOT the
    same reference date) is used as the sample size for test positivity.

    Total testing volume is typically provided for a 7-day period about 4 days
    before the test positivity period. Since the CPR is only published on
    weekdays, test positivity and test volume are only available for the same
    reported dates 3 times a week. We have chosen to censor 7dav test
    positivity based on the 7dav test volume provided in the same originating
    spreadsheet, corresponding to a period ~4 days earlier.

    This approach makes the signals maximally available (5 days per week) with
    low latency. It avoids complications of having to process multiple
    spreadsheets each day, and the fact that test positivity and test volume
    are not available for all the same reference dates.

    Discussion of decision and alternatives (Delphi-internal share drive):
    https://docs.google.com/document/d/1MoIimdM_8OwG4SygoeQ9QEVZzIuDl339_a0xoYa6vuA/edit#

    """
    # Check that we have positivity *and* volume for each publishdate+geo, and
    # that they have the same number of timestamps.
    pos_count_ts = positivity_df.groupby(
        ["publish_date", "geo_id"]
    ).agg(
        num_obs=("timestamp", "count"),
        num_unique_obs=("timestamp", "nunique")
    )
    vol_count_ts = volume_df.groupby(
        ["publish_date", "geo_id"]
    ).agg(
        num_obs=("timestamp", "count"),
        num_unique_obs=("timestamp", "nunique")
    )
    merged = pos_count_ts.merge(
        vol_count_ts,
        on=["geo_id", "publish_date"],
        how="outer",
        indicator=True
    )
    assert all(
        merged["_merge"] == "both"
    ) and all(
        merged.num_obs_x == merged.num_obs_y
    ) and all(
        merged.num_unique_obs_x == merged.num_unique_obs_y
    ), \
        "Each publish date-geo value combination should be available for both " + \
        "test positivity and test volume, and have the same number of timestamps available."
    assert len(positivity_df.index) == len(volume_df.index), \
        "Test positivity and volume data have different numbers of observations."
    expected_rows = len(positivity_df.index)

    volume_df = add_max_ts_col(volume_df)[
        ["geo_id", "publish_date", "val", "is_max_group_ts"]
    ].rename(
        columns={"val":"sample_size"}
    )
    col_order = list(positivity_df.columns)
    positivity_df = add_max_ts_col(positivity_df).drop(["sample_size"], axis=1)

    # Combine test positivity and test volume, maintaining "this week" and
    # "previous week" status. Perform outer join here so that we can later
    # check if any observations did not have a match.
    df = pd.merge(
        positivity_df, volume_df,
        on=["publish_date", "geo_id", "is_max_group_ts"],
        how="outer",
        indicator=True
    ).drop(
        ["is_max_group_ts"], axis=1
    )

    # Check that every volume observation was matched with a positivity observation.
    assert (len(df.index) == expected_rows) and all(df["_merge"] == "both"), \
        "Some observations in the test positivity data were not matched with test volume data."

    # Drop everything with 5 or fewer total tests.
    df = df.loc[df.sample_size > 5]

    # Generate stderr.
    df = df.assign(
        se=std_err(df)
    ).drop(
        ["_merge"],
        axis=1
    )

    return df[col_order]

def add_max_ts_col(df):
    """
    Add column to differentiate timestamps for a given publish date-geo combo.

    Each publish date is associated with up to two timestamps for test volume
    and test positivity. The older timestamp corresponds to data from the
    "previous week"; the newer timestamp corresponds to the "last week".

    Since test volume and test positivity timestamps don't match exactly, we
    can't use them to merge the two signals together, but we still need a way
    to uniquely identify observations to avoid duplicating observations during
    the join. This new column, which is analagous to the "last/previous week"
    classification, is used to merge on.
    """
    assert_df = df.groupby(
        ["publish_date", "geo_id"]
    ).agg(
        num_obs=("timestamp", "count"),
        num_unique_obs=("timestamp", "nunique")
    )
    assert all(
        assert_df.num_obs <= 2
    ) and all(
        assert_df.num_obs == assert_df.num_unique_obs
    ), "Testing signals should have up to two timestamps per publish date-geo level " + \
        "combination. Each timestamp should be unique."

    max_ts_by_group = df.groupby(
        ["publish_date", "geo_id"], as_index=False
    )["timestamp"].max(
    ).rename(
        columns={"timestamp":"max_timestamp"}
    )
    df = pd.merge(
        df, max_ts_by_group,
        on=["publish_date", "geo_id"],
        how="outer"
    ).assign(
        is_max_group_ts=lambda df: df["timestamp"] == df["max_timestamp"]
    ).drop(
        ["max_timestamp"], axis=1
    )

    return df

def std_err(df):
    """
    Find Standard Error of a binomial proportion.

    Assumes input sample_size are all > 0.

    Parameters
    ----------
    df: pd.DataFrame
        Columns: val, sample_size, ...

    Returns
    -------
    pd.Series
        Standard error of the positivity rate of PCR-specimen tests.
    """
    assert all(df.sample_size > 0), "Sample sizes must be greater than 0"
    p = df.val
    n = df.sample_size
    return np.sqrt(p * (1 - p) / n)

def apply_thres_change_date(apply_fn, df, *apply_fn_args):
    """
    Apply a function separately to data before and after the test volume change date.

    The test volume change date is when test volume and test positivity
    started being reported for different reference dates within the same
    report. This first occurred on 2021-03-17.

    Parameters
    ----------
    apply_fn: function
        function to apply to data before and after the test volume change date
    df: pd.DataFrame
        Columns: val, sample_size, ...
    apply_fn_args: tuple of lists
        variable number of additional arguments to pass to the `apply_fn`.
        Each additional argument should be a list of length 2. The first
        element of each list will be passed to the `apply_fn` when processing
        pre-change date data; the second element will be used for the
        post-change date data.

    Returns
    -------
    map object
        Iterator with two entries, one for the "before" data and one for the "after" data.
    """
    change_date = datetime.date(2021, 3, 17)
    list_of_dfs = [df[df.publish_date < change_date], df[df.publish_date >= change_date]]

    for arg_field in apply_fn_args:
        assert len(arg_field) == 2, "Extra arguments must be iterables with " + \
            "length 2, the same as the number of dfs to process"

    return map(apply_fn, list_of_dfs, *apply_fn_args)
