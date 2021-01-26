"""
Update claims-based hospitalization indicator.

Author: Maria Jahja
Created: 2020-09-27

"""

# standard packages
import logging
from multiprocessing import Pool, cpu_count

# third party
import numpy as np
import pandas as pd
from delphi_utils import GeoMapper

# first party
from .config import Config, GeoConstants
from .load_data import load_data
from .indicator import ClaimsHospIndicator
from .weekday import Weekday


class ClaimsHospIndicatorUpdater:
    """Updater class for claims-based hospitalization indicator."""

    # pylint: disable=too-many-instance-attributes, too-many-arguments
    # all variables are used

    def __init__(self, startdate, enddate, dropdate, geo, parallel, weekday,
                 write_se, signal_name):
        """
        Initialize updater for the claims-based hospitalization indicator.

        Args:
            startdate: first indicator date (YYYY-mm-dd)
            enddate: last indicator date (YYYY-mm-dd)
            dropdate: data drop date (YYYY-mm-dd)
            geo: geographic resolution, one of ["county", "state", "msa", "hrr", "hhs", "nation"]
            parallel: boolean to run the indicator update in parallel
            weekday: boolean to adjust for weekday effects
            write_se: boolean to write out standard errors, if true, use an obfuscated name
            signal_name: string signal name

        """
        self.startdate, self.enddate, self.dropdate = [pd.to_datetime(t) for t in
                                                       (startdate, enddate, dropdate)]

        self.geo, self.parallel, self.weekday, self.write_se, self.signal_name = \
            geo.lower(), parallel, weekday, write_se, signal_name

        # init in shift_dates, declared here for pylint
        self.burnindate, self.fit_dates, self.burn_in_dates, self.output_dates = \
            [None] * 4

        assert (
                self.startdate > (Config.FIRST_DATA_DATE + Config.BURN_IN_PERIOD)
        ), f"not enough data to produce estimates starting {self.startdate}"
        assert self.startdate < self.enddate, "start date >= end date"
        assert self.enddate <= self.dropdate, "end date > drop date"
        assert (
                geo in ['county', 'state', 'msa', 'hrr', 'hhs', 'nation']
        ), f"{geo} is invalid, pick one of 'county', 'state', 'msa', 'hrr', 'hhs', 'nation'"

    def shift_dates(self):
        """
        Shift estimates forward to account for time lag.

        Explanation:
        We will shift estimates one day forward to account for a 1 day lag. For example,
        we want to produce estimates for the time range May 2 to May 20, inclusive.
        Given a drop on May 20, we have data up until May 19. We then train on data from
        Jan 1 until May 19, storing only the values on May 1 to May 19. we then shift
        the dates forward by 1, giving us values on May 2 to May 20. We shift the
        startdate back by one day in order to get the proper estimate at May 1.

        """
        drange = lambda s, e: pd.date_range(start=s, periods=(e - s).days, freq='D')
        self.startdate = self.startdate - Config.DAY_SHIFT
        self.burnindate = self.startdate - Config.BURN_IN_PERIOD
        self.fit_dates = drange(Config.FIRST_DATA_DATE, self.dropdate)
        self.burn_in_dates = drange(self.burnindate, self.dropdate)
        self.output_dates = drange(self.startdate, self.enddate)

    def geo_reindex(self, data):
        """
        Reindex dataframe based on desired output geography.

        Args:
            data: dataframe, the output of load_data::load_data()

        Returns:
            reindexed dataframe

        """
        geo_map = GeoMapper()
        if self.geo == "county":
            data_frame = geo_map.fips_to_megacounty(data,
                                                    Config.MIN_DEN,
                                                    Config.MAX_BACKWARDS_PAD_LENGTH,
                                                    thr_col="den",
                                                    mega_col=self.geo)
        elif self.geo == "state":
            data_frame = geo_map.replace_geocode(data,
                                                 from_code="fips",
                                                 new_col=self.geo,
                                                 new_code="state_id")
            data_frame[self.geo] = data_frame[self.geo]
        elif self.geo in ["msa", "hhs", "nation"]:
            data_frame = geo_map.replace_geocode(data,
                                                 from_code="fips",
                                                 new_code=self.geo)
        elif self.geo == "hrr":
            data_frame = data  # data is already adjusted in aggregation step above
        else:
            logging.error(
                "%s is invalid, pick one of 'county', 'state', 'msa', 'hrr', 'hhs', nation'",
                self.geo)
            return False

        unique_geo_ids = pd.unique(data_frame[self.geo])
        data_frame.set_index([self.geo, 'date'], inplace=True)

        # for each location, fill in all missing dates with 0 values
        multiindex = pd.MultiIndex.from_product((unique_geo_ids, self.fit_dates),
                                                names=[self.geo, "date"])
        assert (
                len(multiindex) <= (GeoConstants.MAX_GEO[self.geo] * len(self.fit_dates))
        ), "more loc-date pairs than maximum number of geographies x number of dates"
        # fill dataframe with missing dates using 0
        data_frame = data_frame.reindex(multiindex, fill_value=0)
        data_frame.fillna(0, inplace=True)
        return data_frame

    def update_indicator(self, input_filepath, outpath):
        """
        Generate and output indicator values.

        Args:
            input_filepath: path to the aggregated claims data
            outpath: output path for the csv results

        """
        self.shift_dates()
        final_output_inds = \
            (self.burn_in_dates >= self.startdate) & (self.burn_in_dates <= self.enddate)

        # load data
        base_geo = Config.HRR_COL if self.geo == Config.HRR_COL else Config.FIPS_COL
        data = load_data(input_filepath, self.dropdate, base_geo)
        data_frame = self.geo_reindex(data)

        # handle if we need to adjust by weekday
        wd_params = Weekday.get_params(data_frame) if self.weekday else None

        # run fitting code (maybe in parallel)
        rates = {}
        std_errs = {}
        valid_inds = {}
        if not self.parallel:
            for geo_id, sub_data in data_frame.groupby(level=0):
                sub_data.reset_index(level=0, inplace=True)
                if self.weekday:
                    sub_data = Weekday.calc_adjustment(wd_params, sub_data)
                res = ClaimsHospIndicator.fit(sub_data, self.burnindate, geo_id)
                res = pd.DataFrame(res)
                rates[geo_id] = np.array(res.loc[final_output_inds, "rate"])
                std_errs[geo_id] = np.array(res.loc[final_output_inds, "se"])
                valid_inds[geo_id] = np.array(res.loc[final_output_inds, "incl"])
        else:
            n_cpu = min(Config.MAX_CPU_POOL, cpu_count())
            logging.debug("starting pool with %d workers", n_cpu)
            with Pool(n_cpu) as pool:
                pool_results = []
                for geo_id, sub_data in data_frame.groupby(level=0, as_index=False):
                    sub_data.reset_index(level=0, inplace=True)
                    if self.weekday:
                        sub_data = Weekday.calc_adjustment(wd_params, sub_data)
                    pool_results.append(
                        pool.apply_async(
                            ClaimsHospIndicator.fit,
                            args=(sub_data, self.burnindate, geo_id,),
                        )
                    )
                pool_results = [proc.get() for proc in pool_results]
                for res in pool_results:
                    geo_id = res["geo_id"]
                    res = pd.DataFrame(res)
                    rates[geo_id] = np.array(res.loc[final_output_inds, "rate"])
                    std_errs[geo_id] = np.array(res.loc[final_output_inds, "se"])
                    valid_inds[geo_id] = np.array(res.loc[final_output_inds, "incl"])

        # write out results
        unique_geo_ids = list(rates.keys())
        output_dict = {
            "rates": rates,
            "se": std_errs,
            "dates": self.output_dates,
            "geo_ids": unique_geo_ids,
            "geo_level": self.geo,
            "include": valid_inds,
        }

        self.write_to_csv(output_dict, outpath)
        logging.debug("wrote files to %s", outpath)

    def write_to_csv(self, output_dict, output_path="./receiving"):
        """
        Write values to csv.

        Args:
            output_dict: dictionary containing values, se, unique dates, and unique geo_id
            output_path: outfile path to write the csv

        """
        if self.write_se:
            logging.info("========= WARNING: WRITING SEs TO %s =========",
                         self.signal_name)

        geo_level = output_dict["geo_level"]
        dates = output_dict["dates"]
        geo_ids = output_dict["geo_ids"]
        all_rates = output_dict["rates"]
        all_se = output_dict["se"]
        all_include = output_dict["include"]
        out_n = 0
        for i, date in enumerate(dates):
            filename = "%s/%s_%s_%s.csv" % (
                output_path,
                (date + Config.DAY_SHIFT).strftime("%Y%m%d"),
                geo_level,
                self.signal_name,
            )
            with open(filename, "w") as outfile:
                outfile.write("geo_id,val,se,direction,sample_size\n")
                for geo_id in geo_ids:
                    val = all_rates[geo_id][i]
                    se = all_se[geo_id][i]
                    if all_include[geo_id][i]:
                        assert not np.isnan(val), "value for included value is nan"
                        assert not np.isnan(se), "se for included rate is nan"
                        if val > 90:
                            logging.warning("value suspicious, %s: %d", geo_id, val)
                        assert se < 5, f"se suspicious, {geo_id}: {se}"
                        if self.write_se:
                            assert val > 0 and se > 0, "p=0, std_err=0 invalid"
                            outfile.write(
                                "%s,%f,%s,%s,%s\n" % (geo_id, val, se, "NA", "NA"))
                        else:
                            # for privacy reasons we will not report the standard error
                            outfile.write(
                                "%s,%f,%s,%s,%s\n" % (geo_id, val, "NA", "NA", "NA"))
                        out_n += 1

        logging.debug("wrote %d rows for %d %s", out_n, len(geo_ids), geo_level)
