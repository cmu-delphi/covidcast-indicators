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
from .config import Config
from .geo_maps import GeoMaps
from .sensor import DoctorVisitsSensor
from .weekday import Weekday


def write_to_csv(output_dict, out_name, output_path="."):
    """Write sensor values to csv.

    Args:
        output_dict: dictionary containing sensor rates, se, unique dates, and unique geo_id
        output_path: outfile path to write the csv (default is current directory)
    """

    geo_level = output_dict["geo_level"]
    dates = output_dict["dates"]
    geo_ids = output_dict["geo_ids"]
    all_rates = output_dict["rates"]
    all_se = output_dict["se"]
    all_include = output_dict["include"]

    out_n = 0
    for i, d in enumerate(dates):
        filename = "%s/%s_%s_%s.csv" % (
            output_path,
            (d + Config.DAY_SHIFT).strftime("%Y%m%d"),
            geo_level,
            out_name,
        )

        with open(filename, "w") as outfile:
            outfile.write("geo_id,val,se,direction,sample_size\n")

            for geo_id in geo_ids:
                sensor = all_rates[geo_id][i] * 100  # report percentage
                se = all_se[geo_id][i] * 100

                if all_include[geo_id][i]:
                    assert not np.isnan(sensor), "sensor value is nan, check pipeline"
                    if not np.isnan(se):
                        assert (
                            se < 5
                        ), f"standard error suspiciously high! investigate {geo_id}"

                    # for privacy reasons we will not report the standard error
                    outfile.write(
                        "%s,%f,%s,%s,%s\n" % (geo_id, sensor, "NA", "NA", "NA")
                    )
                    out_n += 1
    logging.debug(f"wrote {out_n} rows for {len(geo_ids)} {geo_level}")


def update_sensor(
    filepath, outpath, staticpath, startdate, dropdate, geo, parallel, weekday
):
    """Generate sensor values, and write to csv format.

    Args:
        filepath: path to the aggregated doctor-visits data
        outpath: output path for the csv results
        staticpath: path for the static geographic files
        startdate: starting date to consider data (YYYY-mm-dd)
        dropdate: data drop date (YYYY-mm-dd)
        geo: geographic resolution, one of ["county", "state", "msa", "hrr"]
        parallel: boolean to run the sensor update in parallel
        weekday: boolean to adjust for weekday effects
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

    # restrict to start and end date
    startdate = pd.to_datetime(startdate)
    dropdate = pd.to_datetime(dropdate)
    data = data[
        (data[Config.DATE_COL] >= startdate) & (data[Config.DATE_COL] < dropdate)
    ]
    assert startdate <= Config.BURN_IN_DATE, "Start date is after sensor burn-in date"
    assert (data[Config.COUNT_COLS] >= 0).all().all(), "Counts must be nonnegative"

    # handle if we need to adjust by weekday
    params = Weekday.get_params(data) if weekday else None

    # collect dates
    drange = lambda s, e: np.array([s + timedelta(days=x) for x in range((e - s).days)])
    fit_dates = drange(startdate, dropdate)
    burn_in_dates = drange(Config.BURN_IN_DATE, dropdate)
    sensor_dates = drange(Config.FIRST_SENSOR_DATE, dropdate)
    sensor_idxs = np.where(burn_in_dates >= Config.FIRST_SENSOR_DATE)

    # get right geography
    geo_map = GeoMaps(staticpath)
    if geo.lower() == "county":
        data_groups, _ = geo_map.county_to_megacounty(
            data, Config.MIN_RECENT_VISITS, Config.RECENT_LENGTH
        )
    elif geo.lower() == "state":
        data_groups, _ = geo_map.county_to_state(data)
    elif geo.lower() == "msa":
        data_groups, _ = geo_map.county_to_msa(data)
    elif geo.lower() == "hrr":
        data_groups, _ = geo_map.county_to_hrr(data)
    else:
        logging.error(f"{geo} is invalid, pick one of 'county', 'state', 'msa', 'hrr'")
        return False
    unique_geo_ids = list(data_groups.groups.keys())

    # run sensor fitting code (maybe in parallel)
    sensor_rates = {}
    sensor_se = {}
    sensor_include = {}
    if not parallel:
        for geo_id in unique_geo_ids:
            sub_data = data_groups.get_group(geo_id).copy()
            if weekday:
                sub_data = Weekday.calc_adjustment(params, sub_data)

            res = DoctorVisitsSensor.fit(
                sub_data,
                fit_dates,
                burn_in_dates,
                geo_id,
                Config.MIN_RECENT_VISITS,
                Config.MIN_RECENT_OBS,
            )
            sensor_rates[geo_id] = res["rate"][sensor_idxs]
            sensor_se[geo_id] = res["se"][sensor_idxs]
            sensor_include[geo_id] = res["incl"][sensor_idxs]

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
                            geo_id,
                            Config.MIN_RECENT_VISITS,
                            Config.MIN_RECENT_OBS,
                        ),
                    )
                )
            pool_results = [proc.get() for proc in pool_results]

            for res in pool_results:
                geo_id = res["geo_id"]
                sensor_rates[geo_id] = res["rate"][sensor_idxs]
                sensor_se[geo_id] = res["se"][sensor_idxs]
                sensor_include[geo_id] = res["incl"][sensor_idxs]

    unique_geo_ids = list(sensor_rates.keys())
    output_dict = {
        "rates": sensor_rates,
        "se": sensor_se,
        "dates": sensor_dates,
        "geo_ids": unique_geo_ids,
        "geo_level": geo,
        "include": sensor_include,
    }

    # write out results
    out_name = "smoothed_adj_cli" if weekday else "smoothed_cli"
    write_to_csv(output_dict, out_name, outpath)
    logging.debug(f"wrote files to {outpath}")
    return True
