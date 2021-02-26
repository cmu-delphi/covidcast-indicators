# -*- coding: utf-8 -*-

from boto3 import Session
from moto import mock_s3
import pytest

from os import listdir, remove
from os.path import join

from delphi_utils import read_params
from delphi_google_health.run import run_module


@pytest.fixture(scope="session")
def clean_receiving_dir():
    # Clean receiving directory
    for fname in listdir("receiving"):
        if fname != ".gitignore":
            remove(join("receiving", fname))
