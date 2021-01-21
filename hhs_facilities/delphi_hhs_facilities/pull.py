"""Functions for mapping geographic regions."""

from datetime import date

import pandas as pd
import numpy as np
from delphi_utils.geomap import GeoMapper
from delphi_epidata import Epidata

from .constants import NAN_VALUES


def pull_data_iteratively(states: set, dates: dict) -> list:
    """
    Pull Epidata API for a set of states and dates.

    To avoid Epidata API row limits, does not grab all values at once. Instead, it loops through
    each state and pulls all data for 10 hospitals at a time.

    Parameters
    ----------
    states: set
      Set of state codes (2 letter lowercase abbreviation) to get data for.
    dates: dict
      Dict of 'from' and 'to' dates output by Epidata.range().

    Returns
    -------
    List of dictionaries. Concatenation of all the response['epidata'] lists.
    """
    responses = []
    for state in states:
        lookup_response = Epidata.covid_hosp_facility_lookup(state)
        state_hospital_ids = [i["hospital_pk"] for i in lookup_response.get("epidata", [])]
        for i in range(0, len(state_hospital_ids), 50):
            response = Epidata.covid_hosp_facility(state_hospital_ids[i:i+50], dates)
            if response["result"] == 2:
                raise Exception(f"Bad result from Epidata: {response['message']}")
            responses += response.get("epidata", [])
    if len(responses) == 0:
        raise Exception("No results found.")
    return responses


def pull_data() -> pd.DataFrame:
    """
    Pull HHS data from Epidata API for all states and dates and convert to a DataFrame.

    Returns
    -------
    DataFrame of HHS data.
    """
    today = int(date.today().strftime("%Y%m%d"))
    past_reference_day = int(date(2020, 1, 1).strftime("%Y%m%d"))  # first available date in DB
    all_states = GeoMapper().get_geo_values("state_id")
    responses = pull_data_iteratively(all_states, Epidata.range(past_reference_day, today))
    all_columns = pd.DataFrame(responses).replace(NAN_VALUES, np.nan)
    all_columns["timestamp"] = pd.to_datetime(all_columns["collection_week"], format="%Y%m%d")
    return all_columns
