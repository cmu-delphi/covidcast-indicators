from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
from delphi_utils import covidcast_wrapper
import covidcast
from freezegun import freeze_time
from pandas.testing import assert_frame_equal

TEST_DIR = Path(__file__).parent
class TestCovidcastWrapper:
    def test_metadata(self):
        expected_df = covidcast.metadata()
        df = covidcast_wrapper.metadata()
        assert_frame_equal(expected_df, df)

    @freeze_time("2024-07-29")
    def test_signal(self):
        meta_df = covidcast_wrapper.metadata()
        data_filter = ((meta_df["max_time"] >= datetime(year=2024, month=6, day=1)) & (meta_df["time_type"] == "day"))
        signal_df = meta_df[data_filter].groupby("data_source")["signal"].agg(['unique'])
        enddate = datetime.today()
        startdate = enddate - timedelta(days=15)
        for data_source, row in signal_df.iterrows():
            signals = list(row[0])
            for signal in signals:
                # expected_df = covidcast.signal(data_source, signal, start_day=startdate, end_day=enddate, geo_type="state")
                expected_df = pd.read_pickle(f"{TEST_DIR}/test_data/{data_source}_{signal}.pkl")
                if expected_df is None:
                    print("%s %s %s %s not existing", data_source, signal, startdate, enddate)
                    continue
                df = covidcast_wrapper.signal(data_source, signal, start_day=startdate, end_day=enddate, geo_type="state")

                check = df.merge(expected_df, indicator=True)
                assert (check["_merge"] == "both").all()

