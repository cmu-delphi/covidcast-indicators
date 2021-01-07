"""Registry for constants."""
from functools import partial
from datetime import timedelta

from .smooth import (
    identity,
    kday_moving_average,
)

# global constants
METRICS = ["Anosmia", "Ageusia"]
COMBINED_METRIC = "sum_anosmia_ageusia"
SMOOTHERS = ["raw", "smoothed"]
GEO_RESOLUTIONS = [
        "state",
        "county",
        "msa",
        "hrr",
        "hhs",
        "nation"
]

seven_day_moving_average = partial(kday_moving_average, k=7)
SMOOTHERS_MAP = {
    "raw":           (identity, lambda d: d - timedelta(days=7)),
    "smoothed":      (seven_day_moving_average, lambda d: d),
}

STATE_TO_ABBREV = {'Alabama':'al',
                   'Alaska': 'ak',
#                   'American Samoa': 'as',
                   'Arizona': 'az',
                   'Arkansas': 'ar',
                   'California': 'ca',
                   'Colorado': 'co',
                   'Connecticut': 'ct',
                   'Delaware': 'de',
#                   'District of Columbia': 'dc',
                   'Florida': 'fl',
                   'Georgia': 'ga',
#                   'Guam': 'gu',
                   'Hawaii': 'hi',
                   'Idaho': 'id',
                   'Illinois': 'il',
                   'Indiana': 'in',
                   'Iowa': 'ia',
                   'Kansas': 'ks',
                   'Kentucky': 'ky',
                   'Louisiana': 'la',
                   'Maine': 'me',
                   'Maryland': 'md',
                   'Massachusetts': 'ma',
                   'Michigan': 'mi',
                   'Minnesota': 'mn',
                   'Mississippi': 'ms',
                   'Missouri': 'mo',
                   'Montana': 'mt',
                   'Nebraska': 'ne',
                   'Nevada': 'nv',
                   'New_Hampshire': 'nh',
                   'New_Jersey': 'nj',
                   'New_Mexico':'nm',
                   'New_York': 'ny',
                   'North_Carolina': 'nc',
                   'North_Dakota': 'nd',
#                   'Northern Mariana Islands': 'mp',
                   'Ohio': 'oh',
                   'Oklahoma': 'ok',
                   'Oregon': 'or',
                   'Pennsylvania': 'pa',
#                   'Puerto Rico': 'pr',
                   'Rhode_Island': 'ri',
                   'South_Carolina': 'sc',
                   'South_Dakota': 'sd',
                   'Tennessee': 'tn',
                   'Texas': 'tx',
                   'Utah': 'ut',
                   'Vermont': 'vt',
#                   'Virgin Islands': 'vi',
                   'Virginia': 'va',
                   'Washington': 'wa',
                   'West_Virginia': 'wv',
                   'Wisconsin': 'wi',
                   'Wyoming': 'wy'}

DC_FIPS = "11001"
