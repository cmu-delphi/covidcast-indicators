"""Registry for variations."""

GEOS = [
    "nation",
    "state",
    "county",
]

METRICS = ['percent_visits_covid','percent_visits_influenza',
           'percent_visits_rsv','percent_visits_combined',
           'percent_visits_smoothed_covid','percent_visits_smoothed_influenza',
           'percent_visits_smoothed_rsv','percent_visits_smoothed_combined']

SENSORS = ['percent_visits_covid','percent_visits_influenza',
           'percent_visits_rsv','percent_visits_combined',
           'smoothed_percent_visits_covid','smoothed_percent_visits_influenza',
           'smoothed_percent_visits_rsv','smoothed_percent_visits_combined']

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
