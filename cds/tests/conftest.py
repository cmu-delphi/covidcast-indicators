# -*- coding: utf-8 -*-

import pytest

from os import listdir, remove
from os.path import join

from delphi_cds.run import run_module


@pytest.fixture(scope="session")
def run_as_module():
    # Clean receiving directory
    for fname in listdir("receiving"):
        if ".csv" in fname:
            remove(join("receiving", fname))

    run_module()
