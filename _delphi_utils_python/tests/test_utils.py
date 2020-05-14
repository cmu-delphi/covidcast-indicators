import pytest

import os

from delphi_utils import read_params


class TestReadParams:
    def test_return_params(self):
        params = read_params()
        assert params["test"] == "yes"

    def test_copy_template(self):
        os.remove("params.json")
        params = read_params()
        assert params["test"] == "yes"
