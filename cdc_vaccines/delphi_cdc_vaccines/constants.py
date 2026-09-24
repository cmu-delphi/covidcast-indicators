"""Registry for variations."""

from itertools import product
from delphi_utils import Smoother


CUMULATIVE = 'cumulative'
INCIDENCE ='incidence'
FREQUENCY = [CUMULATIVE, INCIDENCE]
STATUS = ["tot", "part"]
AGE = ["", "_12P", "_18P", "_65P"]

SIGNALS = [f"{frequency}_counts_{status}_vaccine{AGE}" for
	frequency, status, age in product(FREQUENCY, STATUS, AGE)]
DIFFERENCE_MAPPING = {
    f"{INCIDENCE}_counts_{status}_vaccine{age}": f"{CUMULATIVE}_counts_{status}_vaccine{age}"
    for status, age in product(STATUS, AGE)
}
SIGNALS = list(DIFFERENCE_MAPPING.keys()) + list(DIFFERENCE_MAPPING.values())


GEOS = [
    "nation",
    "state",
    "hrr",
    "hhs",
    "msa"
]

SMOOTHERS = [
    (Smoother("identity", impute_method=None), ""),
    (Smoother("moving_average", window_length=7), "_7dav"),
]
