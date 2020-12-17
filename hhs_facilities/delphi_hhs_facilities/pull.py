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
    today = int(date.today().strftime("%Y%m%d"))
    past_reference_day = int(date(2020, 1, 1).strftime("%Y%m%d"))  # first available date in DB
    mapper = GeoMapper()
    all_states = mapper.get_geo_values("state_id")
    responses = []
    for state in all_states:
        state_hospital_ids = Epidata.covid_hosp_facility_lookup(state)
        for i in range(0, len(state_hospital_ids), 10):
            response = Epidata.covid_hosp_facility(state_hospital_ids[i:i+10],
                                                   past_reference_day,
                                                   today)
            if response["result"] != 1:
                raise Exception(f"Bad result from Epidata: {response['message']}")
            responses += response["epidata"]
    all_columns = pd.DataFrame(responses)
    all_columns.replace(NAN_VALUES, np.nan, inplace=True)
    all_columns["timestamp"] = pd.to_datetime(all_columns["collection_week"])
    return all_columns
