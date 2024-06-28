"""
Generate doctor-visits sensors.

Author: Maria Jahja
Created: 2020-04-18
Modified:
 - 2020-04-30: Aaron Rumack (add megacounty code)
 - 2020-05-06: Aaron and Maria (weekday effects/other adjustments)
"""

# standard packages
from datetime import timedelta, datetime
from multiprocessing import Pool, cpu_count

# third party
import numpy as np
import pandas as pd


# first party
from delphi_utils import Weekday
from .config import Config
from .geo_maps import GeoMaps
from .process_data import csv_to_df
from .sensor import DoctorVisitsSensor


def update_sensor(
        filepath:str, startdate:datetime, enddate:datetime, dropdate:datetime, geo:str, parallel: bool,
        weekday:bool, se:bool, logger
):
    """Generate sensor values.

    Args:
      filepath: path to the aggregated doctor-visits data
      startdate: first sensor date (YYYY-mm-dd)
      enddate: last sensor date (YYYY-mm-dd)
      dropdate: data drop date (YYYY-mm-dd)
      geo: geographic resolution, one of ["county", "state", "msa", "hrr", "nation", "hhs"]
      parallel: boolean to run the sensor update in parallel
      weekday: boolean to adjust for weekday effects
      se: boolean to write out standard errors, if true, use an obfuscated name
      logger: the structured logger
    """
    data = csv_to_df(filepath, startdate, enddate, dropdate, logger)

    # aggregate age groups (so data is unique by service date and FIPS)
    data = data.groupby([Config.DATE_COL, Config.GEO_COL]).sum(numeric_only=True).reset_index()
    assert np.sum(data.duplicated()) == 0, "Duplicates after age group aggregation"
    assert (data[Config.COUNT_COLS] >= 0).all().all(), "Counts must be nonnegative"

    drange = lambda s, e: np.array([s + timedelta(days=x) for x in range((e - s).days)])
    fit_dates = drange(Config.FIRST_DATA_DATE, dropdate)
    burnindate = startdate - Config.DAY_SHIFT
    burn_in_dates = drange(burnindate, dropdate)
    sensor_dates = drange(startdate, enddate)
    # The ordering of sensor dates corresponds to the order of burn-in dates
    final_sensor_idxs = np.where(
        (burn_in_dates >= startdate) & (burn_in_dates <= enddate))[0][:len(sensor_dates)]

    # handle if we need to adjust by weekday
    params = Weekday.get_params(
        data,
        "Denominator",
        Config.CLI_COLS + Config.FLU1_COL,
        Config.DATE_COL,
        [1, 1e5, 1e10, 1e15],
        logger,
    ) if weekday else None
    if weekday and np.any(np.all(params == 0,axis=1)):
        # Weekday correction failed for at least one count type
        return None

    # handle explicitly if we need to use Jeffreys estimate for binomial proportions
    jeffreys = bool(se)

    # get right geography
    geo_map = GeoMaps()
    mapping_func = geo_map.geo_func[geo.lower()]
    data_groups, _ = mapping_func(data)
    unique_geo_ids = list(data_groups.groups.keys())

    # run sensor fitting code (maybe in parallel)
    out = []
    if not parallel:
        for geo_id in unique_geo_ids:
            sub_data = data_groups.get_group(geo_id).copy()
            if weekday:
                sub_data = Weekday.calc_adjustment(params,
                                                   sub_data,
                                                   Config.CLI_COLS + Config.FLU1_COL,
                                                   Config.DATE_COL)

            res = DoctorVisitsSensor.fit(
                sub_data,
                fit_dates,
                burn_in_dates,
                final_sensor_idxs,
                geo_id,
                Config.MIN_RECENT_VISITS,
                Config.MIN_RECENT_OBS,
                jeffreys,
                logger
            )
            out.append(res)

    else:
        n_cpu = min(10, cpu_count())
        logger.debug(f"starting pool with {n_cpu} workers")

        with Pool(n_cpu) as pool:
            pool_results = []
            for geo_id in unique_geo_ids:
                sub_data = data_groups.get_group(geo_id).copy()
                if weekday:
                    sub_data = Weekday.calc_adjustment(params,
                                                       sub_data,
                                                       Config.CLI_COLS + Config.FLU1_COL,
                                                       Config.DATE_COL)

                pool_results.append(
                    pool.apply_async(
                        DoctorVisitsSensor.fit,
                        args=(
                            sub_data,
                            fit_dates,
                            burn_in_dates,
                            final_sensor_idxs,
                            geo_id,
                            Config.MIN_RECENT_VISITS,
                            Config.MIN_RECENT_OBS,
                            jeffreys,
                            logger
                        ),
                    )
                )
            out = [proc.get() for proc in pool_results]

    return pd.concat(out)
