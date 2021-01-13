"""
Generate doctor-visits sensors.

Author: Maria Jahja
Created: 2020-04-18
Modified:
 - 2020-04-30: Aaron Rumack (add megacounty code)
 - 2020-05-06: Aaron and Maria (weekday effects/other adjustments)
 - 2020-11-12: Aaron Rumack (add sensorization)
"""

# standard packages
import logging
from datetime import timedelta
from multiprocessing import Pool, cpu_count

# third party
import numpy as np
import pandas as pd

# third party
import covidcast

# first party
from .config import Config
from .geo_maps import GeoMaps
from .sensor import DoctorVisitsSensor
from .sensorize import Sensorizer
from .weekday import Weekday
from delphi_utils import GeoMapper


def write_to_csv(output_dict, se, out_name, output_path="."):
    """Write sensor values to csv.

    Args:
      output_dict: dictionary containing sensor rates, se, unique dates, and unique geo_id
      se: boolean to write out standard errors, if true, use an obfuscated name
      out_name: name of the output file
      output_path: outfile path to write the csv (default is current directory)
    """

    if se:
        logging.info(f"========= WARNING: WRITING SEs TO {out_name} =========")

    geo_level = output_dict["geo_level"]
    dates = output_dict["dates"]
    geo_ids = output_dict["geo_ids"]
    all_rates = output_dict["rates"]
    all_se = output_dict["se"]
    all_include = output_dict["include"]

    out_n = 0
    for i, d in enumerate(dates):
        filename = "%s/%s_%s_%s.csv" % (output_path,
                                        (d + Config.DAY_SHIFT).strftime("%Y%m%d"),
                                        geo_level,
                                        out_name)

        with open(filename, "w") as outfile:
            outfile.write("geo_id,val,se,direction,sample_size\n")

            for geo_id in geo_ids:
                sensor = all_rates[geo_id][i] * 100  # report percentage
                se_val = all_se[geo_id][i] * 100

                if all_include[geo_id][i]:
                    assert not np.isnan(sensor), "sensor value is nan, check pipeline"
                    assert sensor < 90, f"strangely high percentage {geo_id, sensor}"
                    if not np.isnan(se_val):
                        assert se_val < 5, f"standard error suspiciously high! investigate {geo_id}"

                    if se:
                        assert sensor > 0 and se_val > 0, "p=0, std_err=0 invalid"
                        outfile.write(
                            "%s,%f,%s,%s,%s\n" % (geo_id, sensor, se_val, "NA", "NA"))
                    else:
                        # for privacy reasons we will not report the standard error
                        outfile.write(
                            "%s,%f,%s,%s,%s\n" % (geo_id, sensor, "NA", "NA", "NA"))
                    out_n += 1
    logging.debug(f"wrote {out_n} rows for {len(geo_ids)} {geo_level}")

def write_coefs_to_csv(output_dict, out_name, output_path="."):
    """Write sensorization coefficients to csv.
    Only supported for static sensorization, since this can save memory.

    Args:
      output_dict: dictionary containing sensorization coefficients and unique geo_id
      out_name: name of the output file
      output_path: outfile path to write the csv (default is current directory)
    """

    geo_level = output_dict["geo_level"]
    geo_ids = output_dict["geo_ids"]
    all_coefs = output_dict["coefs"]

    out_n = 0
    filename = "%s/%s_%s.csv" % (output_path,
                                    geo_level,
                                    out_name)

    with open(filename, "w") as outfile:
        outfile.write("geo_id,b1,b0\n")

        for geo_id in geo_ids:
            b1 = all_coefs[geo_id][1]
            b0 = all_coefs[geo_id][0] * 100  # report percentage
            assert not np.isnan(b1), "sensorization coef b1 is nan, check pipeline"
            assert not np.isnan(b0), "sensorization coef b0 is nan, check pipeline"

            outfile.write("%s,%f,%f\n" % (geo_id, b1, b0))
            out_n += 1
    logging.debug(f"wrote {out_n} rows for {len(geo_ids)} {geo_level}")


def update_sensor( # pylint: disable=too-many-branches
        filepath, outpath, staticpath, startdate, enddate, dropdate, geo, parallel,
        weekday, se, sensorize, global_sensor_fit="intercept", prefix=None
):
    """Generate sensor values, and write to csv format.

    Args:
      filepath: path to the aggregated doctor-visits data
      outpath: output path for the csv results
      staticpath: path for the static geographic files
      startdate: first sensor date (YYYY-mm-dd)
      enddate: last sensor date (YYYY-mm-dd)
      dropdate: data drop date (YYYY-mm-dd)
      geo: geographic resolution, one of ["county", "state", "msa", "hrr"]
      parallel: boolean to run the sensor update in parallel
      weekday: boolean to adjust for weekday effects
      se: boolean to write out standard errors, if true, use an obfuscated name
      sensorize: boolean to sensorize signal
      global_sensor_fit: method to fit/rescale sensorized signal
      prefix: string to prefix to output files (used for obfuscation in producing SEs)
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
    final_sensor_idxs = np.where(
        (burn_in_dates >= startdate) & (burn_in_dates <= enddate))

    # handle if we need to adjust by weekday
    params = Weekday.get_params(data) if weekday else None

    # get right geography
    geo_map = GeoMaps()
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
                se # use Jeffreys estimate
            )
            sensor_rates[geo_id] = res["rate"][final_sensor_idxs]
            sensor_se[geo_id] = res["se"][final_sensor_idxs]
            sensor_include[geo_id] = res["incl"][final_sensor_idxs]

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
                            se, # use Jeffreys estimate
                        ),
                    )
                )
            pool_results = [proc.get() for proc in pool_results]

            for res in pool_results:
                geo_id = res["geo_id"]
                sensor_rates[geo_id] = res["rate"][final_sensor_idxs]
                sensor_se[geo_id] = res["se"][final_sensor_idxs]
                sensor_include[geo_id] = res["incl"][final_sensor_idxs]

    unique_geo_ids = list(sensor_rates.keys())
    output_dict = {
        "rates": sensor_rates,
        "se": sensor_se,
        "dates": sensor_dates,
        "geo_ids": unique_geo_ids,
        "geo_level": geo,
        "include": sensor_include,
    }

    if sensorize:

        # Convert data in dict of dicts format to single pd.DataFrame
        signal_dfs = []
        for geo_id in unique_geo_ids:
            incl = output_dict["include"][geo_id]
            geo_df = pd.DataFrame(
                data={"signal":output_dict["rates"][geo_id][incl],
                      "time": burn_in_dates[final_sensor_idxs][incl]})
            geo_df["geo"] = geo_id
            signal_dfs.append(geo_df)
        signal_df = pd.concat(signal_dfs)

        # Load target (7-day averaged case incidence proportion)
        target_df = covidcast.signal("jhu-csse",
                                     "confirmed_7dav_incidence_prop",
                                     geo_type = geo.lower(),
                                     start_day = pd.to_datetime(sensor_dates[0]),
                                     end_day = pd.to_datetime(sensor_dates[-1]))
        target_df = target_df[["geo_value","time_value","value"]]

        geomapper = GeoMapper()
        fips_pop = pd.DataFrame({"fips":sorted(list(geomapper.get_geo_values("fips")))})
        fips_pop = fips_pop[fips_pop.fips.str.slice(start=2) != "000"]
        fips_pop = geomapper.add_population_column(fips_pop,"fips",geocode_col="fips")
        if geo.lower() == "state":
            geo_weights = geomapper.replace_geocode(
                fips_pop,"fips","state_id",from_col="fips",new_col="geo",date_col=None
            )
        elif geo.lower() == "hrr":
            geo_weights = geomapper.replace_geocode(
                fips_pop,"fips","hrr",from_col="fips",new_col="geo",date_col=None
            )
        elif geo.lower() == "msa":
            geo_weights = geomapper.replace_geocode(
                fips_pop,"fips","msa",from_col="fips",new_col="geo",date_col=None
            )
        elif geo.lower() == "county":
            geo_weights = fips_pop.rename(columns={"fips":"geo"})
        geo_weights = geo_weights.rename(columns={"population":"weight"})

        # Sensorize!
        sensorized_df, coef_df = Sensorizer.sensorize(
                                    signal_df,
                                    target_df,
                                    "geo","time","signal",
                                    "geo_value","time_value","value",
                                    global_weights=geo_weights,
                                    global_sensor_fit=global_sensor_fit)

        # Use sensorized_df to fill in sensorized rates
        for geo_id in unique_geo_ids:
            incl = output_dict["include"][geo_id]
            output_dict["rates"][geo_id][incl] = sensorized_df[
                sensorized_df["geo"] == geo_id
            ]["signal"]

        # Eventually need to update SEs also

        # Coef dict
        coef_dict = {
            "coefs": {},
            "geo_ids": unique_geo_ids,
            "geo_level": geo,
        }
        for geo_id in unique_geo_ids:
            idx = np.where(coef_df["geo"] == geo_id)[0][0]
            coef_dict["coefs"][geo_id] = [coef_df["b0"].values[idx], coef_df["b1"].values[idx]]

    # write out results
    out_name = ["smoothed"]
    if weekday:
        out_name.append("adj")
    if sensorize:
        out_name.append("scaled")
    out_name.append("cli")
    out_name = "_".join(out_name)
    if se:
        assert prefix is not None, "template has no obfuscated prefix"
        out_name = prefix + "_" + out_name

    write_to_csv(output_dict, se, out_name, outpath)

    if sensorize:
        out_name = "coefficients"
        write_coefs_to_csv(coef_dict, out_name, outpath)

    logging.debug(f"wrote files to {outpath}")
    return True
