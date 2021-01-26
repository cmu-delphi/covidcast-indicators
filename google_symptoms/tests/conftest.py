# -*- coding: utf-8 -*-

import pytest

from os import listdir, remove, makedirs
from os.path import join, exists

from delphi_google_symptoms.run import run_module


@pytest.fixture(scope="session")
def run_as_module():
    if exists("receiving"):
        # Clean receiving directory
        for fname in listdir("receiving"):
            remove(join("receiving", fname))
    else:
        makedirs("receiving")

    run_module()
