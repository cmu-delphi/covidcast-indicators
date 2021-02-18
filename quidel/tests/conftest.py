# -*- coding: utf-8 -*-

import os
from os.path import join
import pytest


@pytest.fixture(scope="session")
def clean_receiving_dir():
    # Clean receiving directory
    for fname in os.listdir("receiving"):
        if ".csv" in fname:
            os.remove(join("receiving", fname))
    for fname in os.listdir("cache"):
        if ".csv" in fname:
            os.remove(join("cache", fname))
