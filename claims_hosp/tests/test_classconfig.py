# first party
from delphi_claims_hosp.config import Config


class TestConfigValues:
    def test_values(self):
        conf = Config()

        assert conf.MIN_DEN == 100
        assert conf.MAX_BACKWARDS_PAD_LENGTH == 7
