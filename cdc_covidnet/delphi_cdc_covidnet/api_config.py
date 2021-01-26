"""
This file contains configuration variables used to interact with the COVID-NET API.

Author: Eu Jing Chua
Created: 2020-06-12
"""

class APIConfig:
    """Static configuration variables."""

    # API Parameters
    INIT_URL = "https://gis.cdc.gov/grasp/covid19_3_api/GetPhase03InitApp"
    MMWR_COLS = ["year", "weeknumber", "weekstart", "weekend"]
    AGE_COLS = ["label", "ageid", "parentid"]

    HOSP_URL = "https://gis.cdc.gov/grasp/covid19_3_api/PostPhase03DownloadData"
    HOSP_DTYPES = {
        "year": "int64",
        "mmwr-year": "int64",
        "mmwr-week": "int64"
    }
    SEASONS = [59]

    HOSP_RENAME_COLS = {
        "weekend": "date",
        "mmwr-week": "epiweek",
        "catchment": "geo_id",
        "cumulative-rate": "val"
    }

    STATE_COL = "geo_id"
