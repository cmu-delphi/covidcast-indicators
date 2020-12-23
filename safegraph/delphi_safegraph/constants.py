"""Constants for constructing Safegraph indicator."""

HOME_DWELL = 'median_home_dwell_time'
COMPLETELY_HOME = 'completely_home_prop'
FULL_TIME_WORK = 'full_time_work_prop'
PART_TIME_WORK = 'part_time_work_prop'

SIGNALS = [
    HOME_DWELL,
    COMPLETELY_HOME,
    FULL_TIME_WORK,
    PART_TIME_WORK
]

GEO_RESOLUTIONS = [
    'county',
    'state',
    'msa',
    'hrr',
    'hhs',
    'nation'
]
