"""
This file contains configuration variables used to generate the COVID-NET sensor.

Author: Eu Jing Chua
Created: 2020-06-12
"""

class Config:
    """
    Static configuration variables
    """
    # API Parameters
    API_INIT_URL = "https://gis.cdc.gov/grasp/covid19_3_api/GetPhase03InitApp"
    API_MMWR_COLS = ["year", "weeknumber", "weekstart", "weekend"]
    API_AGE_COLS = ["label", "ageid", "parentid"]

    API_HOSP_URL = "https://gis.cdc.gov/grasp/covid19_3_api/PostPhase03DownloadData"
    API_HOSP_DTYPES = {
        "year": "int64",
        "mmwr-year": "int64",
        "mmwr-week": "int64"
    }
    API_SEASONS = [59]

    HOSP_RENAME_COLS = {
        "weekend": "date",
        "catchment": "geo_id",
        "cumulative-rate": "val"
    }

    DATE_COL = "date"
    STATE_COL = "geo_id"
    RATE_COL = "rate"
