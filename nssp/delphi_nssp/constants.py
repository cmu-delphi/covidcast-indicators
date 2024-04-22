"""Registry for variations."""

GEOS = [
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

SIGNALS = ["pct_visits_covid", "pct_visits_influenza", "pct_visits_rsv", "pct_visits_combined",
            "smoothed_pct_visits_covid", "smoothed_pct_visits_influenza",
            "smoothed_pct_visits_rsv", "smoothed_pct_visits_combined"]

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
