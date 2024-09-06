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

# first party
from delphi_utils import GeoMapper, Weekday

from .config import Config, GeoConstants
from .indicator import ClaimsHospIndicator
from .load_data import load_data


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
        data_frame.set_index([self.geo, "timestamp"], inplace=True)

        # for each location, fill in all missing dates with 0 values
        multiindex = pd.MultiIndex.from_product((unique_geo_ids, self.fit_dates),
                                                names=[self.geo, Config.DATE_COL])
        assert (
                len(multiindex) <= (GeoConstants.MAX_GEO[self.geo] * len(self.fit_dates))
        ), "more loc-date pairs than maximum number of geographies x number of dates"
        # fill dataframe with missing dates using 0
        data_frame = data_frame.reindex(multiindex, fill_value=0)
        data_frame.fillna(0, inplace=True)
        return data_frame

    def update_indicator(self, input_filepath, logger):
        """
        Generate and output indicator values.

        Args:
            input_filepath: path to the aggregated claims data
        """
        self.shift_dates()
        final_output_inds = (self.burn_in_dates >= self.startdate) & (self.burn_in_dates <= self.enddate)

        # load data
        base_geo = Config.HRR_COL if self.geo == Config.HRR_COL else Config.FIPS_COL
        data = load_data(input_filepath, self.dropdate, base_geo)
        data_frame = self.geo_reindex(data)

        # handle if we need to adjust by weekday
        wd_params = (
            Weekday.get_params_legacy(
                data_frame,
                "den",
                ["num"],
                Config.DATE_COL,
                [1, 1e5],
                logger,
            )
            if self.weekday
            else None
        )
        df_lst = []
        output_df = pd.DataFrame()
        if not self.parallel:
            for geo_id, sub_data in data_frame.groupby(level=0):
                sub_data.reset_index(inplace=True)
                if self.weekday:
                    sub_data = Weekday.calc_adjustment(wd_params, sub_data, ["num"], Config.DATE_COL)
                sub_data.set_index(Config.DATE_COL, inplace=True)
                res = ClaimsHospIndicator.fit(sub_data, self.burnindate, geo_id)
                temp_df = pd.DataFrame(res)
                temp_df = temp_df.loc[final_output_inds]
                df_lst.append(pd.DataFrame(temp_df))
            output_df = pd.concat(df_lst)
        else:
            n_cpu = min(Config.MAX_CPU_POOL, cpu_count())
            logging.debug("starting pool with %d workers", n_cpu)
            with Pool(n_cpu) as pool:
                pool_results = []
                for geo_id, sub_data in data_frame.groupby(level=0, as_index=False):
                    sub_data.reset_index(inplace=True)
                    if self.weekday:
                        sub_data = Weekday.calc_adjustment(wd_params, sub_data, ["num"], Config.DATE_COL)
                    sub_data.set_index(Config.DATE_COL, inplace=True)
                    pool_results.append(
                        pool.apply_async(
                            ClaimsHospIndicator.fit,
                            args=(
                                sub_data,
                                self.burnindate,
                                geo_id,
                            ),
                        )
                    )
                df_lst = [pd.DataFrame(proc.get()).loc([final_output_inds]) for proc in pool_results]
                output_df = pd.concat(df_lst)

        return output_df

    def preprocess_output(self, df) -> pd.DataFrame:
        """
        Check for any anomlies and formats the output for exports.

        Parameters
        ----------
        df

        Returns
        -------
        df
        """
        filtered_df = df[df["incl"]]
        filtered_df = filtered_df.reset_index()
        filtered_df.rename(columns={"rate": "val"}, inplace=True)
        filtered_df["timestamp"] = filtered_df["timestamp"].astype(str)
        df_list = []
        for geo_id, group in filtered_df.groupby("geo_id"):
            assert not group.val.isnull().any()
            assert not group.se.isnull().any()
            assert np.all(group.se < 5), f"se suspicious, {geo_id}: {np.where(group.se >= 5)[0]}"
            if np.any(group.val > 90):
                for sus_val in np.where(group.val > 90):
                    logging.warning("value suspicious, %s: %d", geo_id, sus_val)
            if self.write_se:
                assert np.all(group.val > 0) and np.all(group.se > 0), "p=0, std_err=0 invalid"
            else:
                group["se"] = np.NaN
            group["direction"] = np.NaN
            df_list.append(group)

        output_df = pd.concat(df_list)
        return output_df
