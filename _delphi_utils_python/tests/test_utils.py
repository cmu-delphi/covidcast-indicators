import pytest

from delphi_utils import read_params


class TestReadParams:
    def test_return_params(self):
        params = read_params()

        assert params["test"] == "yes"
