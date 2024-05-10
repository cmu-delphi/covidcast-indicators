"""Registry for variations."""

GEOS = [
    "hrr",
    "msa",
    "nation",
    "state",
    "county",
]

SIGNALS_MAP = {
    "percent_visits_covid": "pct_visits_covid",
    "percent_visits_influenza": "pct_visits_influenza",
    "percent_visits_rsv": "pct_visits_rsv",
    "percent_visits_combined": "pct_visits_combined",
    "percent_visits_smoothed_covid": "smoothed_pct_visits_covid",
    "percent_visits_smoothed_1": "smoothed_pct_visits_influenza",
    "percent_visits_smoothed_rsv": "smoothed_pct_visits_rsv",
    "percent_visits_smoothed": "smoothed_pct_visits_combined",
}

SIGNALS = [val for (key, val) in SIGNALS_MAP.items()]
NEWLINE = "\n"

CSV_COLS = [
            "geo_id",
            "val",
            "se",
            "sample_size",
            "missing_val",
            "missing_se",
            "missing_sample_size"
        ]

TYPE_DICT = {key: float for key in SIGNALS}
TYPE_DICT.update({"timestamp": "datetime64[ns]",
                   "geography": str,
                   "county": str,
                   "fips": int})
