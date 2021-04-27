"""
Generate doctor-visits sensors.

Author: Maria Jahja
Created: 2020-04-18
Modified:
 - 2020-04-30: Aaron Rumack (add megacounty code)
 - 2020-05-06: Aaron and Maria (weekday effects/other adjustments)
"""

# standard packages
import logging
from datetime import timedelta
from multiprocessing import Pool, cpu_count

# third party
import numpy as np
import pandas as pd

# first party
from delphi_utils import Nans
from .config import Config
from .geo_maps import GeoMaps
from .sensor import DoctorVisitsSensor
from .weekday import Weekday


def write_to_csv(output_df: pd.DataFrame, geo_level, se, out_name, output_path="."):
    """Write sensor values to csv.

    Args:
      output_dict: dictionary containing sensor rates, se, unique dates, and unique geo_id
      se: boolean to write out standard errors, if true, use an obfuscated name
      out_name: name of the output file
      output_path: outfile path to write the csv (default is current directory)
    """
    if se:
        logging.info(f"========= WARNING: WRITING SEs TO {out_name} =========")

    out_n = 0
    for d in set(output_df["date"]):
        filename = "%s/%s_%s_%s.csv" % (output_path,
                                        (d + Config.DAY_SHIFT).strftime("%Y%m%d"),
                                        geo_level,
                                        out_name)
        single_date_df = output_df[output_df["date"] == d]
        with open(filename, "w") as outfile:
            outfile.write(
                "geo_id,val,se,direction,sample_size,"
                "missing_val,missing_se,missing_sample_size\n"
            )

            for line in single_date_df.itertuples():
                geo_id = line.geo_id
                sensor = 100 * line.val # report percentages
                se_val = 100 * line.se
                assert not np.isnan(sensor), "sensor value is nan, check pipeline"
                assert sensor < 90, f"strangely high percentage {geo_id, sensor}"
                if not np.isnan(se_val):
                    assert se_val < 5, f"standard error suspiciously high! investigate {geo_id}"

                if se:
                    assert sensor > 0 and se_val > 0, "p=0, std_err=0 invalid"
                    outfile.write(
                        "%s,%f,%s,%s,%s,%d,%d,%d\n" %
                        (
                            geo_id, sensor, se, "NA", "NA",
                            Nans.NOT_MISSING.value,
                            Nans.NOT_MISSING.value,
                            Nans.NOT_APPLICABLE.value
                        )
                    )
                else:
                    # for privacy reasons we will not report the standard error
                    outfile.write(
                        "%s,%f,%s,%s,%s,%d,%d,%d\n" %
                        (
                            geo_id, sensor, "NA", "NA", "NA",
                            Nans.NOT_MISSING.value,
                            Nans.PRIVACY.value,
                            Nans.NOT_APPLICABLE.value
                        )
                    )

                out_n += 1
    logging.debug(f"wrote {out_n} rows for {geo_level}")


def update_sensor(
        filepath, startdate, enddate, dropdate, geo, parallel,
        weekday, se
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
    """
    # as of 2020-05-11, input file expected to have 10 columns
    # id cols: ServiceDate, PatCountyFIPS, PatAgeGroup, Pat HRR ID/Pat HRR Name
    # value cols: Denominator, Covid_like, Flu_like, Flu1, Mixed
    data = pd.read_csv(
        filepath,
        usecols=Config.FILT_COLS,
        dtype=Config.DTYPES,
        parse_dates=[Config.DATE_COL],
    )
    assert (
            np.sum(data.duplicated(subset=Config.ID_COLS)) == 0
    ), "Duplicated data! Check the input file"

    # drop HRR columns - unused for now since we assign HRRs by FIPS
    data.drop(columns=Config.HRR_COLS, inplace=True)
    data.dropna(inplace=True)  # drop rows with any missing entries

    # aggregate age groups (so data is unique by service date and FIPS)
    data = data.groupby([Config.DATE_COL, Config.GEO_COL]).sum().reset_index()
    assert np.sum(data.duplicated()) == 0, "Duplicates after age group aggregation"
    assert (data[Config.COUNT_COLS] >= 0).all().all(), "Counts must be nonnegative"

    ## collect dates
    # restrict to training start and end date
    drange = lambda s, e: np.array([s + timedelta(days=x) for x in range((e - s).days)])
    startdate = pd.to_datetime(startdate) - Config.DAY_SHIFT
    burnindate = startdate - Config.DAY_SHIFT
    enddate = pd.to_datetime(enddate)
    dropdate = pd.to_datetime(dropdate)
    assert startdate > Config.FIRST_DATA_DATE, "Start date <= first day of data"
    assert startdate < enddate, "Start date >= end date"
    assert enddate <= dropdate, "End date > drop date"
    data = data[(data[Config.DATE_COL] >= Config.FIRST_DATA_DATE) & \
                (data[Config.DATE_COL] < dropdate)]
    fit_dates = drange(Config.FIRST_DATA_DATE, dropdate)
    burn_in_dates = drange(burnindate, dropdate)
    sensor_dates = drange(startdate, enddate)
    # The ordering of sensor dates corresponds to the order of burn-in dates
    final_sensor_idxs = np.where(
        (burn_in_dates >= startdate) & (burn_in_dates <= enddate))[0][:len(sensor_dates)]

    # handle if we need to adjust by weekday
    params = Weekday.get_params(data) if weekday else None

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
                sub_data = Weekday.calc_adjustment(params, sub_data)

            res = DoctorVisitsSensor.fit(
                sub_data,
                fit_dates,
                burn_in_dates,
                final_sensor_idxs,
                geo_id,
                Config.MIN_RECENT_VISITS,
                Config.MIN_RECENT_OBS,
                jeffreys
            )
            out.append(res)

    else:
        n_cpu = min(10, cpu_count())
        logging.debug(f"starting pool with {n_cpu} workers")

        with Pool(n_cpu) as pool:
            pool_results = []
            for geo_id in unique_geo_ids:
                sub_data = data_groups.get_group(geo_id).copy()
                if weekday:
                    sub_data = Weekday.calc_adjustment(params, sub_data)

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
                        ),
                    )
                )
            out = [proc.get() for proc in pool_results]

    return pd.concat(out)
