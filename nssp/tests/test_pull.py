from datetime import datetime, date
import json
from unittest.mock import patch
import tempfile
import os
import time
from datetime import datetime

import pandas as pd
import pandas.api.types as ptypes

from delphi_nssp.pull import (
    construct_typedicts,
    warn_string,
)
import numpy as np



def test_column_type_dicts():
    type_dict = construct_typedicts()
    assert type_dict == {'timestamp': 'datetime64[ns]',
                         'percent_visits_covid': float,
                         'percent_visits_influenza': float,
                         'percent_visits_rsv': float,
                         'percent_visits_combined': float,
                         'percent_visits_smoothed_covid': float,
                         'percent_visits_smoothed_influenza': float,
                         'percent_visits_smoothed_rsv': float,
                         'percent_visits_smoothed_combined': float,
                         'geography': str,
                         'county': str,
                         'fips': int}
