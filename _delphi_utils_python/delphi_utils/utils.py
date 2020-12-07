"""Read parameter files containing configuration information."""
# -*- coding: utf-8 -*-
from json import load
from os.path import exists
from shutil import copyfile

def read_params():
    """Read a file named 'params.json' in the current working directory.

    If the file does not exist, it copies the file 'params.json.template' to
    'param.json' and then reads the file.
    """
    if not exists("params.json"):
        copyfile("params.json.template", "params.json")

    with open("params.json", "r") as json_file:
        return load(json_file)
