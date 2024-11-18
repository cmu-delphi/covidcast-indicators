"""Registry for signal names."""

GEOS = [
    "state",
    "nation"
]

# column name from socrata
TOTAL_ADMISSION_COVID_API = "totalconfc19newadm"
TOTAL_ADMISSION_FLU_API = "totalconfflunewadm"

SIGNALS_MAP = {
    "confirmed_admissions_covid": TOTAL_ADMISSION_COVID_API,
    "confirmed_admissions_flu": TOTAL_ADMISSION_FLU_API,
}

TYPE_DICT = {
        "timestamp": "datetime64[ns]",
        "geo_id": str,
}

TYPE_DICT.update({signal: float for signal in SIGNALS_MAP.keys()})
