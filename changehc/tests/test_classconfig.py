# standard
import pytest

# first party
from delphi_changehc.config import Config


class TestConfigValues:
    def test_values(self):
        conf = Config()

        assert conf.MIN_DEN == 100
        assert conf.MAX_BACKFILL_WINDOW == 7
