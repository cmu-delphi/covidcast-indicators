# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_hhs`.
"""
from datetime import date, datetime, timedelta

from delphi_epidata import Epidata
from delphi_utils import read_params
from delphi_utils.export import create_export_csv
from delphi_utils.geomap import GeoMapper
import pandas as pd

from .constants import HOSPITALIZATIONS

def int_date_to_previous_day_datetime(x):
    """Convert integer dates to Python datetimes for the previous day.

    Epidata uses an integer date format. This needs to be converted to
    a datetime so that the exporter can interpret it.

    The HHS columns we are interested in measure admissions for the
    previous day. To accurately indicate the date of incidence, the
    date must be shifted back by one day.
    """
    return x.apply(lambda x: datetime.strptime(str(x), "%Y%m%d") - timedelta(days=1))

def run_module():
    """Generate ground truth HHS hospitalization data."""
    params = read_params()
    mapper = GeoMapper()
    request_all_states = ",".join(mapper.get_geo_values("state_id"))

    today = date.today()
    past_reference_day = date(year=2020, month=1, day=1) # first available date in DB
    date_range = [ int(x.strftime("%Y%m%d")) for x in [past_reference_day, today] ]
    response = Epidata.covid_hosp(request_all_states, date_range)
    if response['result'] != 1:
        raise Exception(f"Bad result from Epidata: {response['message']}")
    all_columns = pd.DataFrame(response['epidata'])
    signal = pd.DataFrame({
        "geo_id": all_columns.state.apply(str.lower),
        "timestamp":int_date_to_previous_day_datetime(all_columns.date),
        "val": \
        all_columns.previous_day_admission_adult_covid_confirmed + \
        all_columns.previous_day_admission_pediatric_covid_confirmed,
        "se": None,
        "sample_size": None
    })
    create_export_csv(
        signal,
        params["export_dir"],
        "state",
        HOSPITALIZATIONS
        )
