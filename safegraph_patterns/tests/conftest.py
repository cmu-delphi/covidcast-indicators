# -*- coding: utf-8 -*-

from os.path import join
import os
import pytest


@pytest.fixture(scope="session")
def clean_receiving_dir():
    # Clean receiving directory
    for fname in os.listdir("receiving"):
        if ".csv" in fname:
            os.remove(join("receiving", fname))
