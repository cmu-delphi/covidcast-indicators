"""Registry for signal names."""
from delphi_utils import Smoother

CONFIRMED = "confirmed_admissions_covid_1d"
SUM_CONF_SUSP = "sum_confirmed_suspected_admissions_covid_1d"
CONFIRMED_PROP = "confirmed_admissions_covid_1d_prop"
SUM_CONF_SUSP_PROP = "sum_confirmed_suspected_admissions_covid_1d_prop"
CONFIRMED_FLU = "confirmed_admissions_influenza_1d"
CONFIRMED_FLU_PROP = CONFIRMED_FLU+"_prop"

SIGNALS = [
    CONFIRMED,
    SUM_CONF_SUSP,
    CONFIRMED_PROP,
    SUM_CONF_SUSP_PROP,
    CONFIRMED_FLU,
    CONFIRMED_FLU_PROP
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
