"""
This file contains configuration variables used to generate the claims-based Hosp. signal.

Author: Maria Jahja
Created: 2020-06-01
Modified: 2021-12-11

"""

from datetime import datetime, timedelta


class Config:
    """Static configuration variables."""

    signal_name = {
        "Covid_like": "smoothed_covid19_from_claims",
        "Flu1": "smoothed_flu_from_claims",
    }
    signal_weekday_name = {
        "Covid_like": "smoothed_adj_covid19_from_claims",
        "Flu1": "smoothed_adj_flu_from_claims",
    }

    # max number of CPUs available for pool
    MAX_CPU_POOL = 10

    # first date we consider in training data
    FIRST_DATA_DATE = datetime(2020, 1, 1)

    # number of days training needs to produce estimate
    # (one day needed for smoother to produce values)
    BURN_IN_PERIOD = timedelta(days=1)

    # if desired, shift dates forward for labeling purposes
    DAY_SHIFT = timedelta(days=0)

    # data columns
    CLAIMS_COUNT_COLS = ["Denominator", "Covid_like", "Flu1"]
    CLAIMS_DATE_COL = "ServiceDate"
    FIPS_COL = "fips"
    DATE_COL = "timestamp"
    AGE_COL = "age_group"
    HRR_COL = "hrr"

    CLAIMS_RENAME_COLS = {"Pat HRR ID": HRR_COL, "ServiceDate":  DATE_COL,
                          "PatCountyFIPS": FIPS_COL, "PatAgeGroup": AGE_COL}
    CLAIMS_DTYPES = {
        "ServiceDate": str,
        "PatCountyFIPS": str,
        "Denominator": float,
        "Covid_like": float,
        "Flu1": float,
        "PatAgeGroup": str,
        "Pat HRR ID": str,
    }



    SMOOTHER_BANDWIDTH = 100  # bandwidth for the linear left Gaussian filter
    MIN_DEN = 100  # number of total visits needed to produce a sensor
    MAX_BACKWARDS_PAD_LENGTH = (
        7  # maximum number of days used to average a backwards padding
    )
    MIN_CUM_VISITS = 500  # need to observe at least 500 counts before averaging


class GeoConstants:
    """Constant geographical variables."""

    # number of counties in usa, including megacounties
    NUM_COUNTIES = 3141 + 52
    NUM_HRRS = 308
    NUM_MSAS = 392 + 52  # MSA + States
    NUM_STATES = 54  # including DC, PR, VI, GU
    NUM_HHSS = 10
    NUM_NATIONS = 1

    MAX_GEO = {"county": NUM_COUNTIES,
               "hrr": NUM_HRRS,
               "msa": NUM_MSAS,
               "state": NUM_STATES,
               "hhs": NUM_HHSS,
               "nation": NUM_NATIONS}
