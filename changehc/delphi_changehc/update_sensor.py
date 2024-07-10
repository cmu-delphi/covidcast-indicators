"""
Generate CHC sensors.

Author: Aaron Rumack
Created: 2020-10-14
"""
# standard packages
from multiprocessing import Pool, cpu_count

# third party
import numpy as np
import pandas as pd
from delphi_utils import GeoMapper, Weekday, add_prefix, create_export_csv

# first party
from .config import Config
from .constants import SMOOTHED, SMOOTHED_ADJ, SMOOTHED_ADJ_CLI, SMOOTHED_ADJ_FLU, SMOOTHED_CLI, SMOOTHED_FLU
from .sensor import CHCSensor


def write_to_csv(df, geo_level, write_se, day_shift, out_name, logger, output_path=".", start_date=None, end_date=None):
    """Write sensor values to csv.

    Args:
        df: dataframe containing unique timestamp, unqiue geo_id, val, se, sample_size
        geo_level: the geographic level being written e.g. county, state
        write_se: boolean to write out standard errors, if true, use an obfuscated name
        day_shift: a timedelta specifying the time shift to apply to the dates
        out_name: name of the output file
        output_path: outfile path to write the csv (default is current directory)
        start_date: the first date of the dates to be written
        end_date: the last date of the dates to be written
    """
    df = df.copy()

    # shift dates forward for labeling
    df["timestamp"] += day_shift

    # suspicious value warnings
    suspicious_se_mask = df["se"].gt(5)
    assert df[suspicious_se_mask].empty, " se contains suspiciously large values"
    assert not df["se"].isna().any(), " se contains nan values"
    if write_se:
        logger.info("========= WARNING: WRITING SEs TO {0} =========".format(out_name))
    else:
        df["se"] = np.nan

    assert not df["val"].isna().any(), " val contains nan values"
    suspicious_val_mask = df["val"].gt(90)
    if not df[suspicious_val_mask].empty:
        for geo in df.loc[suspicious_val_mask, "geo_id"]:
            logger.warning("value suspiciously high, {0}: {1}".format(
                geo, out_name
            ))

    dates = create_export_csv(
        df,
        export_dir=output_path,
        geo_res=geo_level,
        start_date=start_date,
        end_date=end_date,
        sensor=out_name,
        write_empty_days=True
    )
    logger.debug("wrote {0} rows for {1} {2}".format(
        df.size, df["geo_id"].unique().size, geo_level
    ))
    logger.debug("wrote files to {0}".format(output_path))
    return dates


class CHCSensorUpdater:  # pylint: disable=too-many-instance-attributes
    """Contains methods to update sensor and write results to csv."""

    def __init__(self,
                 startdate,
                 enddate,
                 dropdate,
                 geo,
                 parallel,
                 weekday,
                 numtype,
                 se,
                 wip_signal,
                 logger):
        """Init Sensor Updater.

        Args:
            startdate: first sensor date (YYYY-mm-dd)
            enddate: last sensor date (YYYY-mm-dd)
            dropdate: data drop date (YYYY-mm-dd)
            geo: geographic resolution, one of ["county", "state", "msa", "hrr", "hhs", "nation"]
            parallel: boolean to run the sensor update in parallel
            weekday: boolean to adjust for weekday effects
            numtype: type of count data used, one of ["covid", "cli"]
            se: boolean to write out standard errors, if true, use an obfuscated name
            wip_signal: Prefix for WIP signals
            logger: the structured logger
        """
        self.logger = logger
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
            signal_name = SMOOTHED_ADJ if self.weekday else SMOOTHED
        elif self.numtype == "cli":
            signal_name = SMOOTHED_ADJ_CLI if self.weekday else SMOOTHED_CLI
        elif self.numtype == "flu":
            signal_name = SMOOTHED_ADJ_FLU if self.weekday else SMOOTHED_FLU
        else:
            raise ValueError(f'Unsupported numtype received "{numtype}",'
                             f' must be one of ["covid", "cli", "flu"]')
        self.signal_name = add_prefix([signal_name], wip_signal=wip_signal)[0]

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
            self.logger.error("{0} is invalid, pick one of 'county', "
                          "'state', 'msa', 'hrr', 'hss','nation'".format(geo))
            return False
        if geo == "county":
            data_frame = gmpr.fips_to_megacounty(data,
                                                 Config.MIN_DEN,
                                                 Config.MAX_BACKFILL_WINDOW,
                                                 thr_col="den",
                                                 mega_col=geo,
                                                 date_col=Config.DATE_COL)
            # this line should be removed once the fix is implemented for megacounties
            data_frame = data_frame[~((data_frame['county'].str.len() > 5) | (data_frame['county'].str.contains('_')))]
            # handle rogue \N:
            data_frame = data_frame[data_frame['county'] != r'\N']
        elif geo == "state":
            data_frame = gmpr.replace_geocode(data, "fips", "state_id", new_col="state",
                                              date_col=Config.DATE_COL)
        else:
            data_frame = gmpr.replace_geocode(data, "fips", geo, date_col=Config.DATE_COL)

        unique_geo_ids = pd.unique(data_frame[geo])
        data_frame.set_index([geo, Config.DATE_COL],inplace=True)
        # for each location, fill in all missing dates with 0 values
        multiindex = pd.MultiIndex.from_product((unique_geo_ids, self.fit_dates), names=[geo, Config.DATE_COL])
        assert len(multiindex) <= (len(gmpr.get_geo_values(gmpr.as_mapper_name(geo))) * len(self.fit_dates)), (
            "more loc-date pairs than maximum number of geographies x number of dates, "
            f"length of multiindex is {len(multiindex)}, geo level is {geo}"
        )

        # fill dataframe with missing dates using 0
        data_frame = data_frame.reindex(multiindex, fill_value=0)
        data_frame.fillna(0, inplace=True)
        return data_frame


    def update_sensor(self,
        data,
        output_path):
        """Generate sensor values, and write to csv format.

        Args:
            data: pd.DataFrame with columns num and den
            output_path: output path for the csv results
        """
        self.shift_dates()
        final_sensor_idxs = (self.burn_in_dates >= self.startdate) &\
            (self.burn_in_dates <= self.enddate)

        # load data
        data.reset_index(inplace=True)
        data_frame = self.geo_reindex(data)
        # handle if we need to adjust by weekday
        if self.weekday:
            wd_params = Weekday.get_params_legacy(
                data_frame,
                "den",
                ["num"],
                Config.DATE_COL,
                [1, 1e5],
                self.logger,
           )
        else:
            wd_params = None
        # run sensor fitting code (maybe in parallel)
        if not self.parallel:
            dfs = []
            for geo_id, sub_data in data_frame.groupby(level=0):
                sub_data.reset_index(inplace=True)
                if self.weekday:
                    sub_data = Weekday.calc_adjustment(wd_params, sub_data, ["num"], Config.DATE_COL)
                sub_data.set_index(Config.DATE_COL, inplace=True)
                res = CHCSensor.fit(sub_data, self.burnindate, geo_id, self.logger)
                res = pd.DataFrame(res).loc[final_sensor_idxs]
                dfs.append(res)
        else:
            n_cpu = min(10, cpu_count())
            self.logger.debug("starting pool with {0} workers".format(n_cpu))
            with Pool(n_cpu) as pool:
                pool_results = []
                for geo_id, sub_data in data_frame.groupby(level=0,as_index=False):
                    sub_data.reset_index(inplace=True)
                    if self.weekday:
                        sub_data = Weekday.calc_adjustment(wd_params, sub_data, ["num"], Config.DATE_COL)
                    sub_data.set_index(Config.DATE_COL, inplace=True)
                    pool_results.append(
                        pool.apply_async(
                            CHCSensor.fit, args=(sub_data, self.burnindate, geo_id, self.logger),
                        )
                    )
                pool_results = [proc.get() for proc in pool_results]
                dfs = []
                for res in pool_results:
                    res = pd.DataFrame(res).loc[final_sensor_idxs]
                    dfs.append(res)

        # Form the output dataframe
        df = pd.concat(dfs)
        # sample size is never shared
        df["sample_size"] = np.nan
        # conform to naming expected by create_export_csv()
        df = df.reset_index().rename(columns={"rate": "val"})
        # df.loc[~df['incl'], ["val", "se"]] = np.nan  # update to this line after nancodes get merged in
        df = df[df["incl"]]

        # write out results
        dates = write_to_csv(
            df,
            geo_level=self.geo,
            start_date=min(self.sensor_dates),
            end_date=max(self.sensor_dates),
            write_se=self.se,
            day_shift=Config.DAY_SHIFT,
            out_name=self.signal_name,
            output_path=output_path,
            logger=self.logger
        )
        stats = []
        if len(dates) > 0:
            stats = [(max(dates), len(dates))]
        return stats
