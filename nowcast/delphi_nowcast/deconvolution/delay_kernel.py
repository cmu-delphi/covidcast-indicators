from datetime import datetime, date
from typing import List, Tuple

import pandas as pd
import scipy.stats as stats


def get_national_delay_distribution(
        update: bool = False,
        start_date: date = datetime(2020, 10, 1)) -> Tuple[List[float], List[float]]:
    """
    Retrieve distribution of delays from public CDC linelist.

    The samples are clipped to [1, 45), units in days, and fit to a gamma distribution.
    The discretized values are returned.

    Parameters
    ----------
    update
        Boolean whether to pull the latest data and recalculate the distribution
    start_date
        starting date to clip the delay samples from the linelist data. this is ignored
        if update=False. the default is October 1, 2020.

    Returns
    -------
        tuple of the distribution and gamma parameters
    """

    if update:
        linelist = pd.read_csv("https://data.cdc.gov/api/views/vbim-akqf/rows.csv",
                               usecols=["cdc_report_dt", "onset_dt"],
                               parse_dates=["cdc_report_dt", "onset_dt"])

        linelist = linelist[~linelist.onset_dt.isna()]
        delay_df = linelist[linelist.onset_dt > start_date]
        delay_df["report_delay"] = (delay_df.cdc_report_dt - delay_df.onset_dt).dt.days
        delay_df = delay_df[delay_df.report_delay.gt(0) & delay_df.report_delay.lt(45)]

        coefs = stats.gamma.fit(delay_df.report_delay, floc=0)
    else:
        coefs = (1.5867418033937597, 0, 5.191785168093063)

    # discretized distribution
    delay_gam = stats.gamma(*coefs)
    delay_dist = delay_gam.pdf(range(1, 45))
    delay_dist /= delay_dist.sum()
    return list(delay_dist), list(coefs)


def get_florida_delay_distribution(
        update: bool = False,
        start_date: date = datetime(2020, 5, 1)) -> Tuple[List[float], List[float]]:
    """
    Retrieve distribution of delays from symptom onset to case report. Data is taken from
    the publicly available Florida linelist.

    The samples are clipped to [1, 45), units in days, and fit to a gamma distribution.
    The discretized values are returned.

    Parameters
    ----------
    update
        Boolean whether to pull the latest data and recalculate the distribution
    start_date
        starting date to clip the delay samples from the linelist data. this is ignored
        if update=False. the default is May 1, 2020.

    Returns
    -------
        tuple of the distribution and gamma parameters
    """
    if update:
        florida_linelist = pd.read_csv(
            "https://www.arcgis.com/sharing/rest/content/items/4cc62b3a510949c7a8167f6baa3e069d/data",
            parse_dates=["Case_", "EventDate", "ChartDate"])

        delay_df = florida_linelist[
            florida_linelist.EventDate > start_date]
        delay_df["delay"] = (delay_df.ChartDate - delay_df.EventDate).dt.days
        delay_df = delay_df[delay_df.delay.gt(0) & delay_df.delay.lt(45)]
        coefs = stats.gamma.fit(delay_df.delay, floc=0)
    else:
        coefs = (1.4948103204081697, 0, 4.282169385049879)

    # discretized distribution
    delay_gam = stats.gamma(*coefs)
    delay_dist = delay_gam.pdf(range(1, 45))
    delay_dist /= delay_dist.sum()
    return list(delay_dist), list(coefs)
