# -*- coding: utf-8 -*-
"""Functions to call when running the tool.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_utils.validator`.
"""
import argparse as ap
from .. import get_structured_logger
from .validate import Validator
import os
from shutil import copyfile
from json import load
import time
import sys

# Read Params for each file given name and temp
def read_params2(indicator):
    """Read a file named 'params.json' in the current working directory.
    If the file does not exist, it copies the file 'params.json.template' to
    'params.json' and then reads the file.
    """
    # Looks at indicator, searches in params files

    # If params files doesn't exist, should return false
    template = indicator + "/params.json.template"
    if os.path.exists(template):
        copyfile(template, "params.json")
    else:
        return False

    with open("params.json", "r") as json_file:
        return load(json_file)


#   Cycles through all the files in receiving, finds the correct params,
#   runs run_module_helper,
#   Saves output in csv format?
def run_module():

    directory = "/home/kerx/receiving"
    nowtime = time.time()
    for filename in sorted(os.listdir(directory)):
        newdir = "/home/kerx/params/" + filename
        print(filename)
        if not read_params2(newdir):
            print(filename + "empty")
        else:
            run_module_helper(read_params2(newdir), filename)
            time2 = time.time()
            print("Time for " + filename + ": " + str(time2 - nowtime))
            nowtime = time2
    sys.exit(0)
    return 42

def run_module_helper(params, indicator):
    """Run the validator as a module."""
    csvdir = os.getcwd() + "/receiving/" + indicator
    if(len(os.listdir(csvdir)) == 0): return
    parser = ap.ArgumentParser()
    parser.add_argument("--dry_run", action="store_true", help="When provided, return zero exit"
                        " status irrespective of the number of failures")
    args = parser.parse_args()

    # Check for params, and return / exit
    validator = Validator(params)


    # Need to change logger here
    validator.validate().print_and(
        get_structured_logger(__name__,
                              params["common"].get("log_filename", None)),
        not args.dry_run)


def validator_from_params(params):
    """Construct a validator from `params`.

    Arguments
    ---------
    params: Dict[str, Any]
        Dictionary of parameters
    """
    if "validation" in params:
        return Validator(params)
    return None
