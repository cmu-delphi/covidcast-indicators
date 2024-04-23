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
SIG_DIGITS = 4

TYPE_DICT = {key: float for key in SIGNALS}
TYPE_DICT.update({"timestamp": "datetime64[ns]"})
TYPE_DICT_METRIC = {key: float for key in METRIC_SIGNALS}
TYPE_DICT_METRIC.update({key: "datetime64[ns]" for key in ["date_start", "date_end"]})
# Sample site names
TYPE_DICT_METRIC.update(
    {
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
)
