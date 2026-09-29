# -*- coding: utf-8 -*-
"""Registry for variations.

Data source
-----------
CDC NSSP Emergency Department Respiratory Daily
(https://data.cdc.gov/Public-Health-Surveillance/NSSP-Emergency-Department-Respiratory-Daily/vjzj-u7u8),
the public daily feed from CDC's National Syndromic Surveillance Program
(NSSP). Each record is the percentage of emergency department visits for a
given pathogen (ARI, COVID-19, influenza, RSV) out of all ED visits, for one
U.S. state (or the nation) on one day. The feed is refreshed weekly; each
refresh contains one row per day.

This indicator is the unauthenticated, public-data sibling of the existing
``nssp`` indicator, which reads the restricted NSSP trajectories feed
(``rdmq-nq56``) behind a Socrata app token. The two indicators share signal
names on purpose: in COVIDcast, ``source`` + ``signal`` is the unique key, so
``nssp-ed`` signals sit naturally alongside the ``nssp`` ones.
"""

DATASET_ID = "vjzj-u7u8"
DATASET_DOMAIN = "data.cdc.gov"
DATASET_URL = (
    "https://data.cdc.gov/Public-Health-Surveillance/"
    "NSSP-Emergency-Department-Respiratory-Daily/vjzj-u7u8"
)

# Columns we require from the Socrata feed. If any are missing the feed schema
# has changed and the pull fails loudly instead of emitting wrong data.
EXPECTED_COLUMNS = {"date", "pathogen", "geography", "percent_visits"}

# Raw pathogen labels used by the feed. Every label we know about must appear
# here; an unknown label raises instead of being silently dropped.
PATHOGENS = ["ARI", "COVID", "Influenza", "RSV"]

# Feed pathogen label -> covidcast signal base name. Reuses the existing
# ``nssp`` signal names (the ARI composite is new to this indicator).
SIGNALS_MAP = {
    "ARI": "pct_ed_visits_ari",
    "COVID": "pct_ed_visits_covid",
    "Influenza": "pct_ed_visits_influenza",
    "RSV": "pct_ed_visits_rsv",
}

# Suffix for the 7-day trailing-average variants, per the signal naming
# conventions in _template_python/INDICATOR_DEV_GUIDE.md ("7dav" tag).
SMOOTHED_SUFFIX = "_7dav"

SIGNALS_BASE = [SIGNALS_MAP[pathogen] for pathogen in PATHOGENS]
SIGNALS_SMOOTHED = [signal + SMOOTHED_SUFFIX for signal in SIGNALS_BASE]
SIGNALS = SIGNALS_BASE + SIGNALS_SMOOTHED

GEOS = [
    "nation",
    "state",
    "hhs",
]

# The label the feed uses for the national aggregate row.
NATIONAL_LABEL = "United States"

# A percentage of ED visits must lie in [0, 100]. Rows outside this range are
# implausible (bad source data) and are dropped with an error log.
MIN_PLAUSIBLE_PCT = 0.0
MAX_PLAUSIBLE_PCT = 100.0

# Trailing window (in days) for the smoothed signals.
SMOOTHING_WINDOW_DAYS = 7

AUXILIARY_COLS = [
    "se",
    "sample_size",
    "missing_val",
    "missing_se",
    "missing_sample_size",
]
CSV_COLS = ["geo_id", "val"] + AUXILIARY_COLS

TYPE_DICT = {"timestamp": "datetime64[ns]", "geography": str, "pathogen": str}

NEWLINE = "\n"
