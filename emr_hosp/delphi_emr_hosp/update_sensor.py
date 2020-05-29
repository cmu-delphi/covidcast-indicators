"""
Generate EMR-hosp sensors.

Author: Maria Jahja
Created: 2020-06-01
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
from .sensor import EMRHospSensor
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
                sensor = all_rates[geo_id][i]
                se = all_se[geo_id][i]

                if all_include[geo_id][i]:
                    assert not np.isnan(sensor), "value for included sensor is nan"
                    assert not np.isnan(se), "se for included sensor is nan"
                    assert sensor < 90, f"value suspiciously high, {geo_id}: {sensor}"
                    assert se < 5, f"se suspiciously high, {geo_id}: {se}"

                    # for privacy reasons we will not report the standard error
                    outfile.write(
                        "%s,%f,%s,%s,%s\n" % (geo_id, sensor, "NA", "NA", "NA")
                    )
                    out_n += 1
    logging.debug(f"wrote {out_n} rows for {len(geo_ids)} {geo_level}")


def update_sensor(
    emr_filepath, claims_filepath, outpath, staticpath, startdate, dropdate, geo,
    parallel, weekday
):
    """Generate sensor values, and write to csv format.

    Args:
        emr_filepath: path to the aggregated EMR data
        claims_filepath: path to the aggregated claims data
        outpath: output path for the csv results
        staticpath: path for the static geographic files
        startdate: starting date to consider data (YYYY-mm-dd)
        dropdate: data drop date (YYYY-mm-dd)
        geo: geographic resolution, one of ["county", "state", "msa", "hrr"]
        parallel: boolean to run the sensor update in parallel
        weekday: boolean to adjust for weekday effects
    """

    emr_data = pd.read_csv(
        emr_filepath,
        usecols=Config.EMR_DTYPES.keys(),
        dtype=Config.EMR_DTYPES,
        parse_dates=[Config.EMR_DATE_COL]
    )

    claims_data = pd.read_csv(
        claims_filepath,
        usecols=Config.CLAIMS_DTYPES.keys(),
        dtype=Config.CLAIMS_DTYPES,
        parse_dates=[Config.CLAIMS_DATE_COL],
    )

    # standardize naming
    emr_data.rename(columns=Config.EMR_RENAME_COLS, inplace=True)
    claims_data.rename(columns=Config.CLAIMS_RENAME_COLS, inplace=True)

    # restrict to start and end date
    startdate = pd.to_datetime(startdate)
    dropdate = pd.to_datetime(dropdate)
    emr_data = emr_data[
        (emr_data[Config.DATE_COL] >= startdate) & (
            emr_data[Config.DATE_COL] < dropdate)
        ]
    claims_data = claims_data[
        (claims_data[Config.DATE_COL] >= startdate) & (
            claims_data[Config.DATE_COL] < dropdate)
        ]
    assert startdate <= Config.BURN_IN_DATE, "Start date is after sensor burn-in date"
    assert (
        (emr_data[Config.EMR_COUNT_COLS] >= 0).all().all()
    ), "EMR counts must be nonnegative"
    assert (
        (claims_data[Config.CLAIMS_COUNT_COLS] >= 0).all().all()
    ), "Claims counts must be nonnegative"

    # aggregate age groups (so data is unique by date and geo_col)
    geo_col = "hrr" if geo == "hrr" else "fips"
    emr_data = emr_data.groupby([geo_col, "date"]).sum()
    claims_data = claims_data.groupby([geo_col, "date"]).sum()
    emr_data.dropna(inplace=True)  # drop rows with any missing entries
    claims_data.dropna(inplace=True)  # drop rows with any missing entries

    # merge data
    data = emr_data.merge(claims_data, how="outer", left_index=True, right_index=True)
    assert data.isna().all(axis=1).sum() == 0, "entire row is NA after merge"

    # calculate combined numerator and denominator
    data.fillna(0, inplace=True)
    data["num"] = data["IP_COVID_Total_Count"] + data["Covid_like"]
    data["den"] = data["Total_Count"] + data["Denominator"]
    data = data[['num', 'den']]

    # get right geography
    geo_map = GeoMaps(staticpath)
    if geo.lower() == "county":
        data_frame = geo_map.county_to_megacounty(data)
    elif geo.lower() == "state":
        data_frame = geo_map.county_to_state(data)
    elif geo.lower() == "msa":
        data_frame = geo_map.county_to_msa(data)
    elif geo.lower() == "hrr":
        data_frame = data  # data is already adjusted in aggregation step above
    else:
        logging.error(f"{geo} is invalid, pick one of 'county', 'state', 'msa', 'hrr'")
        return False
    unique_geo_ids = list(sorted(np.unique(data_frame.index.get_level_values(0))))

    # collect dates
    drange = lambda s, e: np.array([s + timedelta(days=x) for x in range((e - s).days)])
    fit_dates = drange(startdate, dropdate)
    burn_in_dates = drange(Config.BURN_IN_DATE, dropdate)
    sensor_dates = drange(Config.FIRST_SENSOR_DATE, dropdate)
    sensor_idxs = np.where(burn_in_dates >= Config.FIRST_SENSOR_DATE)

    # for each location, fill in all missing dates with 0 values
    multiindex = pd.MultiIndex.from_product((unique_geo_ids, fit_dates),
                                            names=[geo, "date"])
    assert (
        len(multiindex) < ((3141 + 52) * len(fit_dates))
    ), "more loc-date pairs than maximum number of counties x number of dates"

    # fill dataframe with missing dates using 0
    data_frame = data_frame.reindex(multiindex, fill_value=0)
    data_frame.fillna(0, inplace=True)

    # handle if we need to adjust by weekday
    params = Weekday.get_params(data_frame) if weekday else None

    # run sensor fitting code (maybe in parallel)
    sensor_rates = {}
    sensor_se = {}
    sensor_include = {}
    if not parallel:
        for geo_id in unique_geo_ids:
            sub_data = data_frame.loc[geo_id].copy()
            if weekday:
                sub_data = Weekday.calc_adjustment(params, sub_data)

            res = EMRHospSensor.fit(sub_data, burn_in_dates, geo_id)
            sensor_rates[geo_id] = res["rate"][sensor_idxs]
            sensor_se[geo_id] = res["se"][sensor_idxs]
            sensor_include[geo_id] = res["incl"][sensor_idxs]

    else:
        n_cpu = min(10, cpu_count())
        logging.debug(f"starting pool with {n_cpu} workers")

        with Pool(n_cpu) as pool:
            pool_results = []
            for geo_id in unique_geo_ids:
                sub_data = data_frame.loc[geo_id].copy()
                if weekday:
                    sub_data = Weekday.calc_adjustment(params, sub_data)

                pool_results.append(
                    pool.apply_async(
                        EMRHospSensor.fit, args=(sub_data, burn_in_dates, geo_id,),
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
    wip_string = "wip_XXXXX_"
    out_name = "smoothed_adj_cli" if weekday else "smoothed_cli"
    write_to_csv(output_dict, wip_string + out_name, outpath)
    logging.debug(f"wrote files to {outpath}")
    return True
