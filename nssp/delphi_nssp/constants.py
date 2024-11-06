"""Registry for variations."""

GEOS = [
    "hrr",
    "msa",
    "nation",
    "state",
    "county",
    "hhs",
]

SIGNALS_MAP = {
    "percent_visits_covid": "pct_ed_visits_covid",
    "percent_visits_influenza": "pct_ed_visits_influenza",
    "percent_visits_rsv": "pct_ed_visits_rsv",
    "percent_visits_combined": "pct_ed_visits_combined",
    "percent_visits_smoothed_covid": "smoothed_pct_ed_visits_covid",
    "percent_visits_smoothed_1": "smoothed_pct_ed_visits_influenza",
    "percent_visits_smoothed_rsv": "smoothed_pct_ed_visits_rsv",
    "percent_visits_smoothed": "smoothed_pct_ed_visits_combined",
}

SIGNALS = [val for (key, val) in SIGNALS_MAP.items()]
NEWLINE = "\n"

AUXILIARY_COLS = [
    "se",
    "sample_size",
    "missing_val",
    "missing_se",
    "missing_sample_size",
]
CSV_COLS = ["geo_id", "val"] + AUXILIARY_COLS

TYPE_DICT = {key: float for key in SIGNALS}
TYPE_DICT.update(
    {
        "timestamp": "datetime64[ns]",
        "geography": str,
        "county": str,
        "fips": str,
    }
)

SECONDARY_COLS_MAP = {
    "week_end": "timestamp",
    "geography": "geo_value",
    "percent_visits": "val",
    "pathogen": "signal",
}

SECONDARY_SIGNALS_MAP = {
    "COVID-19": "pct_ed_visits_covid_secondary",
    "INFLUENZA": "pct_ed_visits_influenza_secondary",
    "RSV": "pct_ed_visits_rsv_secondary",
    "Combined": "pct_ed_visits_combined_secondary",
}

SECONDARY_SIGNALS = [val for (key, val) in SECONDARY_SIGNALS_MAP.items()]
SECONDARY_GEOS = ["state","nation","hhs"]

SECONDARY_TYPE_DICT = {
    "timestamp": "datetime64[ns]",
    "geo_value": str,
    "val": float,
    "geo_type": str,
    "signal": str,
}
SECONDARY_KEEP_COLS = [key for (key, val) in SECONDARY_TYPE_DICT.items()]