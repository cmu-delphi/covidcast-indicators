"""Registry for signal names."""
from delphi_utils import Smoother

CONFIRMED = "confirmed_admissions_covid_1d"
SUM_CONF_SUSP = "sum_confirmed_suspected_admissions_covid_1d"


SIGNALS = [
    CONFIRMED,
    SUM_CONF_SUSP
]

GEOS = [
    "nation",
    "hhs",
    "state"
]

SMOOTHERS = [
    (Smoother("identity", impute_method=None), ""),
    (Smoother("moving_average", window_length=7), "7dav_"),
]
