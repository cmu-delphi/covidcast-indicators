# -*- coding: utf-8 -*-

import pytest

from os import listdir, remove
from os.path import join

from delphi_usafacts.run import run_module


@pytest.fixture(scope="session")
def run_as_module():
    # Clean receiving directory
    for fname in listdir("../receiving"):
        if fname[0] == ".":
            continue
        remove(join("../receiving", fname))

    run_module()
