"""Registry for variations."""

GEOS = [
    "nation",
    "state",
    # "hrr",
    # "msa",
    # "hhs",
    # "county",
    # "wwss",  # wastewater sample site, name will probably need to change
]

SIGNALS = ["pcr_conc_smoothed"]
METRIC_SIGNALS = ["detect_prop_15d", "percentile", "ptc_15d"]
PROVIDER_NORMS = {
    "provider": ["CDC_VERILY", "CDC_VERILY", "NWSS", "NWSS", "WWS"],
    "normalization": [
        "flow-population",
        "microbial",
        "flow-population",
        "microbial",
        "microbial",
    ],
}
METRIC_DATES = ["date_start", "date_end"]
SAMPLE_SITE_NAMES = {
    "wwtp_jurisdiction": "category",
    "wwtp_id": int,
    "reporting_jurisdiction": "category",
    "sample_location": "category",
    "county_names": "category",
    "county_fips": "category",
    "population_served": float,
    "sampling_prior": bool,
    "sample_location_specify": float,
}
SIG_DIGITS = 4

NEWLINE = "\n"
