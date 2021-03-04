"""
This file contains configuration variables used to generate the CHC signal.

Author: Aaron Rumack
Created: 2020-10-14
"""

from datetime import datetime, timedelta


class Config:
    """Static configuration variables."""

    ## dates
    FIRST_DATA_DATE = datetime(2020, 1, 1)

    # number of days training needs to produce estimate
    # (one day needed for smoother to produce values)
    BURN_IN_PERIOD = timedelta(days=1)

    # shift dates forward for labeling purposes
    DAY_SHIFT = timedelta(days=1)

    ## data columns
    COVID_COL = "COVID"
    DENOM_COL = "Denominator"
    FLU_COL = "Flu"
    MIXED_COL = "Mixed"
    FLU_LIKE_COL = "Flu-like"
    COVID_LIKE_COL = "Covid-like"
    COUNT_COLS = [COVID_COL,DENOM_COL,FLU_COL,MIXED_COL,FLU_LIKE_COL,COVID_LIKE_COL]
    DATE_COL = "date"
    GEO_COL = "fips"
    ID_COLS = [DATE_COL] + [GEO_COL]
    FILT_COLS = ID_COLS + COUNT_COLS

    DENOM_COLS = [DATE_COL, GEO_COL, DENOM_COL]
    COVID_COLS = [DATE_COL, GEO_COL, COVID_COL]
    FLU_COLS = [DATE_COL, GEO_COL, FLU_COL]
    MIXED_COLS = [DATE_COL, GEO_COL, MIXED_COL]
    FLU_LIKE_COLS = [DATE_COL, GEO_COL, FLU_LIKE_COL]
    COVID_LIKE_COLS = [DATE_COL, GEO_COL, COVID_LIKE_COL]

    DENOM_DTYPES = {DATE_COL: str, DENOM_COL: str, GEO_COL: str}
    COVID_DTYPES = {DATE_COL: str, COVID_COL: str, GEO_COL: str}
    FLU_DTYPES = {DATE_COL: str, FLU_COL: str, GEO_COL: str}
    MIXED_DTYPES = {DATE_COL: str, MIXED_COL: str, GEO_COL: str}
    FLU_LIKE_DTYPES = {DATE_COL: str, FLU_LIKE_COL: str, GEO_COL: str}
    COVID_LIKE_DTYPES = {DATE_COL: str, COVID_LIKE_COL: str, GEO_COL: str}

    SMOOTHER_BANDWIDTH = 100  # bandwidth for the linear left Gaussian filter
    MIN_DEN = 100  # number of total visits needed to produce a sensor
    MAX_BACKFILL_WINDOW = (
        7  # maximum number of days used to average a backfill correction
    )
    MIN_CUM_VISITS = 500  # need to observe at least 500 counts before averaging
