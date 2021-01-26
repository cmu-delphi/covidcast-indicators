# -*- coding: utf-8 -*-

import pytest
import mock

from os import listdir, remove, makedirs
from os.path import join, exists

import delphi_google_symptoms


@pytest.fixture(scope="session")
def run_as_module():
    if exists("receiving"):
        # Clean receiving directory
        for fname in listdir("receiving"):
            remove(join("receiving", fname))
    else:
        makedirs("receiving")

    with mock.patch("delphi_google_symptoms.pull.initialize_credentials", return_value=None) as mock_credentials:
        delphi_google_symptoms.run.run_module()
