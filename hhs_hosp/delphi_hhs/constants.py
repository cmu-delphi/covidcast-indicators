"""Registry for signal names."""
from delphi_utils import Smoother

CONFIRMED = "confirmed_admissions_covid_1d"
SUM_CONF_SUSP = "sum_confirmed_suspected_admissions_covid_1d"
CONFIRMED_PROP = "confirmed_admissions_covid_1d_prop"
SUM_CONF_SUSP_PROP = "sum_confirmed_suspected_admissions_covid_1d_prop"

SIGNALS = [
    CONFIRMED,
    SUM_CONF_SUSP,
    CONFIRMED_PROP,
    SUM_CONF_SUSP_PROP
]

GEOS = [
    "nation",
    "hhs",
    "state"
]

SMOOTHERS = [
    (Smoother("identity", impute_method=None), ""),
    (Smoother("moving_average", window_length=7), "_7dav"),
]
