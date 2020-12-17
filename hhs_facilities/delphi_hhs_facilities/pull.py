"""Functions for mapping geographic regions."""

from datetime import date, datetime, timedelta

import pandas as pd
import numpy as np
from delphi_epidata import Epidata
from delphi_utils.geomap import GeoMapper

from .constants import NAN_VALUES


def pull_data() -> pd.DataFrame:
    """
    Pull HHS data from Epidata API for all dates and state available.

    Returns
    -------
    DataFrame of HHS data.
    """
    mapper = GeoMapper()
    # get a list of state codes
    all_states = pd.DataFrame(
        {"state_code": ["{:02d}".format(x) for x in range(1, 72)]}
    )
    all_states = mapper.add_geocode(all_states, "state_code", "state_id")
    request_all_states = ",".join(all_states.state_id)
    today = date.today()
    past_reference_day = date(year=2020, month=1, day=1)  # first available date in DB
    date_range = [int(x.strftime("%Y%m%d")) for x in [past_reference_day, today]]
    response = Epidata.covid_hosp_facility(request_all_states, date_range)
    if response["result"] != 1:
        raise Exception(f"Bad result from Epidata: {response['message']}")
    all_columns = pd.DataFrame(response["epidata"])
    all_columns.replace(NAN_VALUES, np.nan, inplace=True)
    all_columns["timestamp"] = pd.to_datetime(all_columns["collection_week"])
    return all_columns
