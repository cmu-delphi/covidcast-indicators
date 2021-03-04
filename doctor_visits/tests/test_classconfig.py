from delphi_doctor_visits.config import Config


class TestConfigValues:
    def test_values(self):

        conf = Config()

        assert conf.CLI_COLS == ["Covid_like", "Flu_like", "Mixed"]
        assert conf.MIN_RECENT_VISITS == 100
        assert conf.MIN_RECENT_OBS == 3
