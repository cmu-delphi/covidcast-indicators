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

# signal id : is_rate, name to report in API
SIGNALS = {
    "total": {
        "is_rate" : False,
        "api_name": "naats_total_7dav"
    },
    "positivity": {
        "is_rate" : True,
        "api_name": "naats_positivity_7dav"
    },
    "confirmed covid-19 admissions": {
        "is_rate" : False,
        "api_name": "confirmed_admissions_covid_1d_7dav"
    },
    "fully vaccinated": {
        "is_rate" : False,
        "api_name": "full_vaccinated_7dav"
    },
    "booster dose since": {
        "is_rate" : False,
        "api_name": "booster_doses_7dav"
    },
    "booster doses administered": {
        "is_rate" : False,
        "api_name": "total_booster_7dav"
    },
    "doses administered": {
        "is_rate" : False,
        "api_name": "total_doses_7dav"
    }
}

COUNTS_7D_SIGNALS = {key for key, value in SIGNALS.items() if not value["is_rate"]}

def make_signal_name(key):
    """Convert a signal key to the corresponding signal name for the API."""
    return SIGNALS[key]["api_name"]

NEWLINE="\n"
