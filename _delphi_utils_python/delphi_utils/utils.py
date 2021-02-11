"""Read parameter files containing configuration information."""
# -*- coding: utf-8 -*-
from json import load,dump
from os.path import exists
from shutil import copyfile
import sys

def read_params():
    """Read a file named 'params.json' in the current working directory.

    If the file does not exist, it copies the file 'params.json.template' to
    'params.json' and then reads the file.
    """
    if not exists("params.json"):
        copyfile("params.json.template", "params.json")

    with open("params.json", "r") as json_file:
        return load(json_file)

def params_get(path, params):
    """Read a key/path from loaded params.

    Parameters
    ----------
    path : str
      The .-delimited path to the value to return. Currently supports dict keys
      but not numeric array indices.

    params : dict
      Params as read using read_params().
    """
    r = params
    for p in path.split("."):
        r = r[p]
    return r

def params_set(path, value, params):
    """Set a key/path in loaded params.

    Parameters
    ----------
    path : str
      The .-delimited path to the value to set. Currently supports dict keys
      but not numeric array indices.

    value : str
      The value to assign at the specified location. Supports several types:
        "true" "false" - assign boolean
        ,-delimited value, or
        existing value is list - assign list
        "/dev/fd/..." - assign string by reading path as file
        anything else - assign string

    params : dict
      Params as read using read_params().
    """
    path = path.split(".")
    for p in path[:-1]:
        params = params[p] # stick the 'params' handle on the innermost dict/list
    if value.startswith("/dev/fd/"): # explicitly handle process substitutions
        with open(value) as f:
            value = f.read().strip()
    if value in {"true","false"}:
        value = value == "true"
    elif isinstance(params[path[-1]],list) or value.find(",")>0:
        value = value.split(",")
    params[path[-1]] = value

def params_run():
    """Get or set parameter value from command line arguments."""
    if len(sys.argv)<3:
        print("""
Usage:
        python -m delphi_utils set key1 value1 key2 value2a,value2b,value2c [...]
        python -m delphi_utils get key1.key2.key3[...]
""")
        sys.exit()

    params = read_params()
    if sys.argv[1]=="get":
        print(params_get(sys.argv[2], params))
    if sys.argv[1]=="set":
        n=0
        for k,v in zip(sys.argv[2::2], sys.argv[3::2]):
            params_set(k, v, params)
            n += 1
        with open("params.json", "w") as f:
            dump(params, f, sort_keys=True, indent=2)
        print(f"Updated {n} items")
