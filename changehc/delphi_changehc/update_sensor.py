"""
Generate CHC sensors.

Author: Aaron Rumack
Created: 2020-10-14
"""
# standard packages
import logging
from multiprocessing import Pool, cpu_count

# third party
import numpy as np
import pandas as pd
from delphi_utils import GeoMapper, read_params, add_prefix

# first party
from .config import Config, Constants
from .constants import SMOOTHED, SMOOTHED_ADJ, SMOOTHED_CLI, SMOOTHED_ADJ_CLI, NA
from .sensor import CHCSensor
from .weekday import Weekday


def write_to_csv(output_dict, write_se, out_name, output_path="."):
    """Write sensor values to csv.

    Args:
        output_dict: dictionary containing sensor rates, se, unique dates, and unique geo_id
        write_se: boolean to write out standard errors, if true, use an obfuscated name
        out_name: name of the output file
        output_path: outfile path to write the csv (default is current directory)
    """
    if write_se:
        logging.info("========= WARNING: WRITING SEs TO {0} =========".format(out_name))
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
                    if sensor > 90:
                        logging.warning("value suspiciously high, {0}: {1}".format(
                            geo_id, sensor
                        ))
                    assert se < 5, f"se suspiciously high, {geo_id}: {se}"
                    if write_se:
                        assert sensor > 0 and se > 0, "p=0, std_err=0 invalid"
                        outfile.write(
                            "%s,%f,%s,%s,%s\n" % (geo_id, sensor, se, NA, NA))
                    else:
                        # for privacy reasons we will not report the standard error
                        outfile.write(
                            "%s,%f,%s,%s,%s\n" % (geo_id, sensor, NA, NA, NA)
                        )
                    out_n += 1
    logging.debug("wrote {0} rows for {1} {2}".format(
        out_n, len(geo_ids), geo_level
    ))


class CHCSensorUpdator:  # pylint: disable=too-many-instance-attributes
    """Contains methods to update sensor and write results to csv."""

    def __init__(self,
                 startdate,
                 enddate,
                 dropdate,
                 geo,
                 parallel,
                 weekday,
                 numtype,
                 se):
        """Init Sensor Updator.

        Args:
            startdate: first sensor date (YYYY-mm-dd)
            enddate: last sensor date (YYYY-mm-dd)
            dropdate: data drop date (YYYY-mm-dd)
            geo: geographic resolution, one of ["county", "state", "msa", "hrr", "hhs", "nation"]
            parallel: boolean to run the sensor update in parallel
            weekday: boolean to adjust for weekday effects
            numtype: type of count data used, one of ["covid", "cli"]
            se: boolean to write out standard errors, if true, use an obfuscated name
        """
        self.startdate, self.enddate, self.dropdate = [
            pd.to_datetime(t) for t in (startdate, enddate, dropdate)]
        # handle dates
        assert (self.startdate > (Config.FIRST_DATA_DATE + Config.BURN_IN_PERIOD)
                ), f"not enough data to produce estimates starting {self.startdate}"
        assert self.startdate < self.enddate, "start date >= end date"
        assert self.enddate <= self.dropdate, "end date > drop date"
        self.geo, self.parallel, self.weekday, self.numtype, self.se = geo.lower(), parallel, \
                                                                       weekday, numtype, se

        # output file naming
        if self.numtype == "covid":
            signals = [SMOOTHED_ADJ if self.weekday else SMOOTHED]
        elif self.numtype == "cli":
            signals = [SMOOTHED_ADJ_CLI if self.weekday else SMOOTHED_CLI]
        signal_names = add_prefix(
            signals,
            wip_signal=read_params()["wip_signal"])
        self.updated_signal_names = signal_names

        # initialize members set in shift_dates().
        self.burnindate = None
        self.fit_dates = None
        self.burn_in_dates = None
        self.sensor_dates = None

    def shift_dates(self):
        """Shift estimates forward to account for time lag, compute burnindates, sensordates."""
        drange = lambda s, e: pd.date_range(start=s,periods=(e-s).days,freq='D')
        self.startdate = self.startdate - Config.DAY_SHIFT
        self.burnindate = self.startdate - Config.BURN_IN_PERIOD
        self.fit_dates = drange(Config.FIRST_DATA_DATE, self.dropdate)
        self.burn_in_dates = drange(self.burnindate, self.dropdate)
        self.sensor_dates = drange(self.startdate, self.enddate)
        return True

    def geo_reindex(self, data):
        """Reindex based on geography, include all date, geo pairs.

        Args:
            data: dataframe, the output of loadcombineddata
        Returns:
            dataframe
        """
        # get right geography
        geo = self.geo
        gmpr = GeoMapper()
        if geo not in {"county", "state", "msa", "hrr", "nation", "hhs"}:
            logging.error("{0} is invalid, pick one of 'county', "
                          "'state', 'msa', 'hrr', 'hss','nation'".format(geo))
            return False
        if geo == "county":
            data_frame = gmpr.fips_to_megacounty(data,
                                                 Config.MIN_DEN,
                                                 Config.MAX_BACKFILL_WINDOW,
                                                 thr_col="den",
                                                 mega_col=geo)
        elif geo == "state":
            data_frame = gmpr.replace_geocode(data, "fips", "state_id", new_col="state")
        else:
            data_frame = gmpr.replace_geocode(data, "fips", geo)

        unique_geo_ids = pd.unique(data_frame[geo])
        data_frame.set_index([geo, Config.DATE_COL],inplace=True)
        # for each location, fill in all missing dates with 0 values
        multiindex = pd.MultiIndex.from_product((unique_geo_ids, self.fit_dates),
                                                names=[geo, Config.DATE_COL])
        assert (len(multiindex) <= (Constants.MAX_GEO[geo] * len(self.fit_dates))
                ), "more loc-date pairs than maximum number of geographies x number of dates"
        # fill dataframe with missing dates using 0
        data_frame = data_frame.reindex(multiindex, fill_value=0)
        data_frame.fillna(0, inplace=True)
        return data_frame


    def update_sensor(self,
            data,
            outpath):
        """Generate sensor values, and write to csv format.

        Args:
            data: pd.DataFrame with columns num and den
            outpath: output path for the csv results
        """
        self.shift_dates()
        final_sensor_idxs = (self.burn_in_dates >= self.startdate) &\
            (self.burn_in_dates <= self.enddate)

        # load data
        data.reset_index(inplace=True)
        data_frame = self.geo_reindex(data)
        # handle if we need to adjust by weekday
        wd_params = Weekday.get_params(data_frame) if self.weekday else None
        # run sensor fitting code (maybe in parallel)
        sensor_rates = {}
        sensor_se = {}
        sensor_include = {}
        if not self.parallel:
            for geo_id, sub_data in data_frame.groupby(level=0):
                sub_data.reset_index(level=0,inplace=True)
                if self.weekday:
                    sub_data = Weekday.calc_adjustment(wd_params, sub_data)
                res = CHCSensor.fit(sub_data, self.burnindate, geo_id)
                res = pd.DataFrame(res)
                sensor_rates[geo_id] = np.array(res.loc[final_sensor_idxs,"rate"])
                sensor_se[geo_id] = np.array(res.loc[final_sensor_idxs,"se"])
                sensor_include[geo_id] = np.array(res.loc[final_sensor_idxs,"incl"])
        else:
            n_cpu = min(10, cpu_count())
            logging.debug("starting pool with {0} workers".format(n_cpu))
            with Pool(n_cpu) as pool:
                pool_results = []
                for geo_id, sub_data in data_frame.groupby(level=0,as_index=False):
                    sub_data.reset_index(level=0, inplace=True)
                    if self.weekday:
                        sub_data = Weekday.calc_adjustment(wd_params, sub_data)
                    pool_results.append(
                        pool.apply_async(
                            CHCSensor.fit, args=(sub_data, self.burnindate, geo_id,),
                        )
                    )
                pool_results = [proc.get() for proc in pool_results]
                for res in pool_results:
                    geo_id = res["geo_id"]
                    res = pd.DataFrame(res)
                    sensor_rates[geo_id] = np.array(res.loc[final_sensor_idxs, "rate"])
                    sensor_se[geo_id] = np.array(res.loc[final_sensor_idxs, "se"])
                    sensor_include[geo_id] = np.array(res.loc[final_sensor_idxs, "incl"])
        unique_geo_ids = list(sensor_rates.keys())
        output_dict = {
            "rates": sensor_rates,
            "se": sensor_se,
            "dates": self.sensor_dates,
            "geo_ids": unique_geo_ids,
            "geo_level": self.geo,
            "include": sensor_include,
        }

        # write out results
        for signal in self.updated_signal_names:
            write_to_csv(output_dict, self.se, signal, outpath)
        logging.debug("wrote files to {0}".format(outpath))
