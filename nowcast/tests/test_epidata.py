import os
import tempfile
from datetime import date
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from delphi_nowcast.data_containers import LocationSeries, SensorConfig
from delphi_nowcast.epidata import export_to_csv, get_indicator_data, get_historical_sensor_data, EPIDATA_START_DATE


class TestGetIndicatorData:

    @patch("delphi_epidata.Epidata.async_epidata")
    def test_results(self, mock_epidata):
        mock_epidata.return_value = [
            ({"result": 1, "epidata": [{"time_value": 20200101, "value": 1},
                                       {"time_value": 20200102, "value": np.nan}]},
             {"data_source": "src1", "signals": "sig1", "geo_type": "state", "geo_value": "ca"}),
            ({"result": 1, "epidata": [{"time_value": 20200101, "value": 2.5}]},
             {"data_source": "src1", "signals": "sig1", "geo_type": "county", "geo_value": "01001"}),
            ({"result": -2},
             {"data_source": "src2", "signals": "sig2", "geo_type": "state", "geo_value": "ca"}),
            ({"result": -2},
             {"data_source": "src2", "signals": "sig2", "geo_type": "county", "geo_value": "01001"}),
        ]
        test_output = get_indicator_data(
            [SensorConfig("src1", "sig1", None, None), SensorConfig("src2", "sig2", None, None)],
            [LocationSeries("ca", "state"), LocationSeries("01001", "county")],
            date(2020, 1, 1)
        )
        assert test_output == {
            ("src1", "sig1", "state", "ca"): LocationSeries("ca", "state", {date(2020, 1, 1): 1}),
            ("src1", "sig1", "county", "01001"): LocationSeries("01001", "county", {date(2020, 1, 1): 2.5})
        }
        mock_epidata.assert_called_once_with([
            {"source": "covidcast",
             "data_source": "src1",
             "signals": "sig1",
             "time_type": "day",
             "geo_type": "state",
             "geo_value": "ca",
             "time_values": f"{EPIDATA_START_DATE}-20200101",
             "as_of": "20200101"},
            {"source": "covidcast",
             "data_source": "src1",
             "signals": "sig1",
             "time_type": "day",
             "geo_type": "county",
             "geo_value": "01001",
             "time_values": f"{EPIDATA_START_DATE}-20200101",
             "as_of": "20200101"},
            {"source": "covidcast",
             "data_source": "src2",
             "signals": "sig2",
             "time_type": "day",
             "geo_type": "state",
             "geo_value": "ca",
             "time_values": f"{EPIDATA_START_DATE}-20200101",
             "as_of": "20200101"},
            {"source": "covidcast",
             "data_source": "src2",
             "signals": "sig2",
             "time_type": "day",
             "geo_type": "county",
             "geo_value": "01001",
             "time_values": f"{EPIDATA_START_DATE}-20200101",
             "as_of": "20200101"}
        ])

    @patch("delphi_epidata.Epidata.async_epidata")
    def test_no_results(self, mock_epidata):
        mock_epidata.return_value = [
            ({"result": -2},
             {"data_source": "src1", "signals": "sig1", "geo_type": "state", "geo_value": "ca"}),
            ({"result": -2},
             {"data_source": "src1", "signals": "sig1", "geo_type": "county", "geo_value": "01001"}),
        ]
        test_output = get_indicator_data(
            [SensorConfig("src1", "sig1", None, None), SensorConfig("src2", "sig2", None, None)],
            [LocationSeries("ca", "state")],
            date(2020, 1, 1)
        )
        assert test_output == {}
        mock_epidata.assert_called_once_with([
            {"source": "covidcast",
             "data_source": "src1",
             "signals": "sig1",
             "time_type": "day",
             "geo_type": "state",
             "geo_value": "ca",
             "time_values": f"{EPIDATA_START_DATE}-20200101",
             "as_of": "20200101"},
            {"source": "covidcast",
             "data_source": "src2",
             "signals": "sig2",
             "time_type": "day",
             "geo_type": "state",
             "geo_value": "ca",
             "time_values": f"{EPIDATA_START_DATE}-20200101",
             "as_of": "20200101"},
        ])

    @patch("delphi_epidata.Epidata.async_epidata")
    def test_error(self, mock_epidata):
        mock_epidata.return_value = [({"result": -3, "message": "test failure"}, {})]
        with pytest.raises(Exception, match="Bad result from Epidata: test failure"):
            get_indicator_data([SensorConfig(None, None, None, None)],
                               [LocationSeries(None, None)],
                               date(2020, 1, 1))
        mock_epidata.assert_called_once_with([
            {"source": "covidcast",
             "data_source": None,
             "signals": None,
             "time_type": "day",
             "geo_type": None,
             "geo_value": None,
             "time_values": f"{EPIDATA_START_DATE}-20200101",
             "as_of": "20200101"}
        ])


class TestGetHistoricalSensorData:

    @patch("delphi_epidata.Epidata.covidcast_nowcast")
    def test_results(self, mock_epidata):
        mock_epidata.return_value = {
            "result": 1,
            "epidata": [{"time_value": 20200101, "value": 1},
                        {"time_value": 20200102, "value": np.nan}]
        }
        test_output = get_historical_sensor_data(SensorConfig(None, None, None, None),
                                                 LocationSeries(None, None),
                                                 date(2020, 1, 1),
                                                 date(2020, 1, 4))

        assert test_output == (LocationSeries(None, None, {date(2020, 1, 1): 1}),
                               [date(2020, 1, 2),
                                date(2020, 1, 3),
                                date(2020, 1, 4)])

    @patch("delphi_epidata.Epidata.covidcast_nowcast")
    def test_no_results(self, mock_epidata):
        mock_epidata.return_value = {"result": -2}
        test_output = get_historical_sensor_data(SensorConfig(None, None, None, None),
                                                 LocationSeries(None, None),
                                                 date(2020, 1, 1),
                                                 date(2020, 1, 4))

        assert test_output == (LocationSeries(None, None), [date(2020, 1, 1), date(2020, 1, 2),
                                                            date(2020, 1, 3), date(2020, 1, 4)])

    @patch("delphi_epidata.Epidata.covidcast_nowcast")
    def test_error(self, mock_epidata):
        mock_epidata.return_value = {"result": -3, "message": "test failure"}
        with pytest.raises(Exception, match="Bad result from Epidata: test failure"):
            get_historical_sensor_data(SensorConfig(None, None, None, None),
                                       LocationSeries(None, None),
                                       date(2020, 1, 1),
                                       date(2020, 1, 4))


class TestExportToCSV:

    def test_export_to_csv(self):
        """Test export creates the right file and right contents."""
        test_sensor = SensorConfig(source="src",
                                   signal="sig",
                                   name="test",
                                   lag=4)
        test_value = LocationSeries("ca", "state", {date(2020, 1, 1): 1.5})
        with tempfile.TemporaryDirectory() as tmpdir:
            out_files = export_to_csv(test_value, test_sensor, date(2020, 1, 5), receiving_dir=tmpdir)
            assert len(out_files) == 1
            out_file = out_files[0]
            assert os.path.isfile(out_file)
            assert out_file.endswith("issue_20200105/src/20200101_state_sig.csv")
            out_file_df = pd.read_csv(out_file)
            pd.testing.assert_frame_equal(out_file_df,
                                          pd.DataFrame({"sensor_name": ["test"],
                                                        "geo_value": ["ca"],
                                                        "value": [1.5]}))
