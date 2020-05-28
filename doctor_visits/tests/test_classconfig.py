import pytest

from delphi_doctor_visits.config import Config


class TestConfigValues:
    def test_values(self):

        conf = Config()

        assert conf.CLI_COLS == ["Covid_like", "Flu_like", "Mixed"]
        assert conf.MIN_OBS == 2500
