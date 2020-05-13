# -*- coding: utf-8 -*-
import json


def read_params():
    """Reads a file named 'params.json' in the current working directory.
    """
    with open("params.json", "r") as json_file:
        return json.load(json_file)
