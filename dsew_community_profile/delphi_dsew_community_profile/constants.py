"""Registry for variations."""
from collections.abc import Callable as function
from dataclasses import dataclass

URL_PREFIX = "https://healthdata.gov/api/views/gqxm-d9w9"
DOWNLOAD_ATTACHMENT = URL_PREFIX + "/files/{assetId}?download=true&filename={filename}"
DOWNLOAD_LISTING = URL_PREFIX + ".json"

@dataclass
class Transform:
    """Transformation filters for interpreting a particular sheet in the workbook."""

    name: str = None
    level: str = None
    row_filter: function = None
    geo_id_select: function = None
    geo_id_apply: function = None

T_FIRST = lambda df: df[df.columns[0]]
TRANSFORMS = {
    t.name: t for t in [
        Transform(
            name="Regions",
            level="hhs",
            geo_id_select=lambda df: df.index.to_series(),
            geo_id_apply=lambda x: x.replace("Region ", "")
        ),
        Transform(
            name="States",
            level="state",
            geo_id_select=T_FIRST,
            geo_id_apply=lambda x: x.lower()
        ),
        Transform(
            name="CBSAs",
            level="msa",
            row_filter=lambda df: df['CBSA type'] == "Metropolitan",
            geo_id_select=T_FIRST,
            geo_id_apply=lambda x: f"{x}"
        ),
        Transform(
            name="Counties",
            level="county",
            geo_id_select=T_FIRST,
            geo_id_apply=lambda x: f"{x:05}"
        )
    ]}

# key: signal id, string pattern used to find column to report as signal
#     is_rate: originating signal is a percentage (e.g. test positivity)
#     is_cumulative: originating signal is cumulative (e.g. vaccine doses ever administered)
#     api_name: name to use in API
#     make_prop: report originating signal as-is and per 100k population
#     api_prop_name: name to use in API for proportion signal
SIGNALS = {
    "total": {
        "is_rate" : False,
        "api_name": "covid_naat_num_7dav",
        "make_prop": False,
        "is_cumulative" : False
    },
    "positivity": {
        "is_rate" : True,
        "api_name": "covid_naat_pct_positive_7dav",
        "make_prop": False,
        "is_cumulative" : False
    },
    "confirmed covid-19 admissions": {
        "is_rate" : False,
        "api_name": "confirmed_admissions_covid_1d_7dav",
        "make_prop": True,
        "api_prop_name": "confirmed_admissions_covid_1d_prop_7dav",
        "is_cumulative" : False
    },
    "fully vaccinated": {
        "is_rate" : False,
        "api_name": "people_full_vaccinated",
        "make_prop": False,
        "is_cumulative" : True
    },
    "booster dose since": {
        "is_rate" : False,
        "api_name": "people_booster_doses",
        "make_prop": False,
        "is_cumulative" : True
    },
    "booster doses administered": {
        "is_rate" : False,
        "api_name": "booster_doses_admin_7dav",
        "make_prop": False,
        "is_cumulative" : False
    },
    "doses administered": {
        "is_rate" : False,
        "api_name": "doses_admin_7dav",
        "make_prop": False,
        "is_cumulative" : False
    }
}

COUNTS_7D_SIGNALS = {key for key, value in SIGNALS.items() \
                        if not((value["is_rate"]) or (value["is_cumulative"]))}

def make_signal_name(key, is_prop=False):
    """Convert a signal key to the corresponding signal name for the API.

    Note, this function gets called twice with the same `key` for signals that support
    population-proportion ("prop") variants.
    """
    if is_prop:
        return SIGNALS[key]["api_prop_name"]
    return SIGNALS[key]["api_name"]

NEWLINE = "\n"
IS_PROP = True
NOT_PROP = False
