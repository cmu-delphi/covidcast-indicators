"""
This file contains configuration variables used to generate the EMR-Hosp signal.

Author: Maria Jahja
Created: 2020-06-01
"""

from datetime import datetime, timedelta


class Config:
    """Static configuration variables.
    """

    # dates
    DAY_SHIFT = timedelta(days=1)  # shift dates forward for labeling purposes
    # Feb 1 is when we start producing sensors
    FIRST_SENSOR_DATE = datetime(2020, 2, 1) - DAY_SHIFT
    # add burn-in sensor dates to calculate direction
    BURN_IN_DATE = datetime(2020, 1, 29) - DAY_SHIFT

    # data columns
    EMR_COUNT_COLS = ["Total_Count", "IP_COVID_Total_Count"]
    EMR_DATE_COL = "Admit_Date"
    EMR_RENAME_COLS = {"HRR ID": "hrr", "Admit_Date": "date", "Age_Band": "age_group"}
    EMR_DTYPES = {"Admit_Date": str,
                  "fips": str,
                  "Total_Count": float,  # pandas float handles NAs, but int does not
                  "IP_COVID_Total_Count": float,
                  "HRR ID": str,
                  "Age_Band": str}

    CLAIMS_COUNT_COLS = ["Denominator", "Covid_like"]
    CLAIMS_DATE_COL = "ServiceDate"
    CLAIMS_RENAME_COLS = {"Pat HRR ID": "hrr", "ServiceDate": "date",
                          "PatCountyFIPS": "fips", "PatAgeGroup": "age_group"}

    CLAIMS_DTYPES = {
        "ServiceDate": str,
        "PatCountyFIPS": str,
        "Denominator": float,
        "Covid_like": float,
        "PatAgeGroup": str,
        "Pat HRR ID": str,
    }

    FIPS_COL = "fips"
    DATE_COL = "date"
    AGE_COL = "age_group"
    HRR_COL = "hrr"

    SMOOTHER_BANDWIDTH = 100  # bandwidth for the linear left Gaussian filter
    MIN_DEN = 100  # number of total visits needed to produce a sensor
    MAX_BACKFILL_WINDOW = (
        7  # maximum number of days used to average a backfill correction
    )
    MIN_CUM_VISITS = 500  # need to observe at least 500 counts before averaging
