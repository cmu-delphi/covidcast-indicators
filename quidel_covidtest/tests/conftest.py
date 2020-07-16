# -*- coding: utf-8 -*-

import pytest

import os
from os.path import join

from delphi_quidel_covidtest.run import run_module


@pytest.fixture(scope="session")
def run_as_module():
    # Clean receiving directory
    for fname in os.listdir("receiving"):
        os.remove(join("receiving", fname))

    run_module()
