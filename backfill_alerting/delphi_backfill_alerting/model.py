"""
Model training, testing and result evaluation.

"""
from datetime import timedelta

# third party
import numpy as np
import pandas as pd
import statsmodels.api as sm
from delphi_utils import GeoMapper

# first party
from .config import Config

gmpr = GeoMapper()

param_list = ["value_raw", "log_value_7dav"] + [f"issueD{i}" for i in range(6)] \
        + [f"refD{i}" for i in range(6)] + [f"issueW{i}" for i in range(3)]

def model_traning_and_testing(backfill_df, dropdate, bv_type, ref_lag):
    """Training and testing for backfill variables.

    For backfill fraction, use data with issue date from dropdate - 180 days
    to dropdate - anchor_lag days to be the training data. The data for
    the most recent anchor_lag days is used as the testing data.

    For change rate, use data with issue date from dropdate - 180 days to the
    day before dropdate to be the training data. The data for dropdate is used
    as the testing data.

    The data is trained separately for each location. Besides, data is trained
    in groups of different lags.

    Args:
       backfill_df: dataframe for backfill information
       dropdate : the most recent issue date considered
       bv_type: the type of the backfill variable
       ref_lag: k for change rate or anchor_lag for backfill fraction

    Returns:
        backfill dataframes with prediction result
    """
    if bv_type == Config.CHANGE_RATE:
        test_start = dropdate
    else:
        test_start = dropdate - timedelta(days=ref_lag)

    train_pdList = []
    test_pdList = []
    for loc in backfill_df[Config.GEO_COL].unique():
        traindf = backfill_df.loc[(backfill_df[Config.GEO_COL] == loc)
                                  & (backfill_df["issue_date"] < test_start)].dropna()
        traindf["predicted"] = None
        testdf = backfill_df.loc[(backfill_df[Config.GEO_COL] == loc)
                                  & (backfill_df["issue_date"] >= test_start)].dropna()
        testdf["predicted"] = None
        if traindf.shape[0] < 200 or testdf.shape[0] == 0:
            continue

        for i in range(1, len(Config.LAG_SPLITS)):
            # Train separately for lags
            train_indx = (traindf["lag"] <= Config.LAG_SPLITS[i] + 2)\
                         &(traindf["lag"] > Config.LAG_SPLITS[i-1]-3)
            test_indx = (testdf["lag"] <= Config.LAG_SPLITS[i])\
                         &(testdf["lag"] > Config.LAG_SPLITS[i-1])

            if sum(test_indx) == 0:
                continue

            try:
                res = sm.GLM(traindf.loc[train_indx, "value"] + 1,
                             traindf.loc[train_indx, param_list],
                             family=sm.families.Gamma(link=sm.families.links.log())
                             ).fit()

                traindf.loc[train_indx, "predicted"] = res.predict(
                    traindf.loc[train_indx, param_list]) - 1
                testdf.loc[test_indx, "predicted"] = res.predict(
                    testdf.loc[test_indx, param_list]) - 1
            except ValueError:
                pass
        train_pdList.append(traindf)
        test_pdList.append(testdf)
    return (pd.concat(train_pdList).dropna(),
            pd.concat(test_pdList).dropna())

def evaluation(results, dropdate, cache_dir, test=False):
    """
    Get the generalized evaluation of the prediction result.

    Args:
       results: list of two dataframes:in-sample and out-sample prediction
       dropdate : the most recent issue date considered
       cache_dir: directory to the cache folder

    Returns:
        list of special dates or location-date pairs
    """
    traindf, testdf = results
    if testdf.shape[0] == 0:
        return [], [], [], []
    traindf["test"] = False
    testdf["test"] = True
    evl_df = traindf.append(testdf)
    evl_df["residue"] = evl_df["value"] - evl_df["predicted"]
    evl_df["p"] = evl_df.groupby(["geo_value", "lag"])["residue"].rank(pct=True)
    evl_df["log_p"] = evl_df["p"].apply(lambda x:np.log(x+1e-15))
    evl_df["weighted_logp"] = evl_df["log_p"] / (evl_df["lag"] + 1)
    if not test:
        evl_df.to_csv(cache_dir+f"/evl_result_{dropdate.date()}.csv", index=False)

    # Averaged over all locations and all lags
    # Generalized evaluation of each test date
    result_per_date = evl_df.groupby(["issue_date", "test"]).mean().reset_index()
    result_per_date["date_rank"] = result_per_date["weighted_logp"].rank(pct=True)
    result_per_date["alert"] = None
    result_per_date.loc[result_per_date["date_rank"] > 0.95, "alert"] = "L"
    result_per_date.loc[result_per_date["date_rank"] < 0.05, "alert"] = "S"
    l_dates = list(result_per_date.loc[
        (result_per_date["test"]) & (result_per_date["alert"] == "L"),
        "issue_date"].dt.strftime('%Y-%m-%d').unique())
    s_dates = list(result_per_date.loc[
        (result_per_date["test"]) & (result_per_date["alert"] == "S"),
        "issue_date"].dt.strftime('%Y-%m-%d').unique())

    # Averaged over all lags
    # Generalized evaluation of each test date for each location
    result_per_date_loc = evl_df.groupby(["issue_date", "test",
                                          "geo_value"]).mean().reset_index()
    result_per_date_loc["alert"] = None
    pdList = []
    for loc in result_per_date_loc["geo_value"].unique():
        subdf = result_per_date_loc[result_per_date_loc["geo_value"] == loc]
        subdf.loc[:, "date_rank"] = subdf["weighted_logp"].rank(pct=True)
        subdf.loc[subdf["date_rank"] > 0.95, "alert"] = "L"
        subdf.loc[subdf["date_rank"] < 0.05, "alert"] = "S"
        pdList.append(subdf.loc[subdf["test"]].dropna())
    result_per_date_loc = pd.concat(pdList)
    result_per_date_loc["pair"] = result_per_date_loc["geo_value"] + ","\
        + result_per_date_loc["issue_date"].dt.strftime('%Y-%m-%d')
    l_pairs = list(result_per_date_loc.loc[result_per_date_loc["alert"] == "L",
                                      "pair"].unique())
    s_pairs = (result_per_date_loc.loc[result_per_date_loc["alert"] == "S",
                                      "pair"].unique())
    return l_dates, s_dates, l_pairs, s_pairs
 