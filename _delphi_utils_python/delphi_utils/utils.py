"""Read parameter files containing configuration information."""
# -*- coding: utf-8 -*-
from json import load,dump
from shutil import copyfile, move
import os
import sys

def read_params():
    """Read a file named 'params.json' in the current working directory.

    If the file does not exist, it copies the file 'params.json.template' to
    'params.json' and then reads the file.
    """
    if not os.path.exists("params.json"):
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

def transfer_files():
    """Transfer files to prepare for acquisition."""
    params = read_params()
    export_dir = params["common"].get("export_dir", None)
    delivery_dir = params["delivery"].get("delivery_dir", None)
    files_to_export = os.listdir(export_dir)
    for file_name in files_to_export:
        if file_name.lower().endswith(".csv"):
            move(os.path.join(export_dir, file_name), os.path.join(delivery_dir, file_name))

def delete_move_files():
    """Delete or move output files depending on dir settings provided in params.

    1. Delete files in export-dir if delivery-dir is specified and is different
       from export_dir (aka only delete files produced by the most recent run)
    2. If validation-failures-dir is specified, move failed files there instead
    If dry-run tag is True, then this function should not (and currently does not) get called
    """
    params = read_params()
    export_dir = params["common"].get("export_dir", None)
    delivery_dir = params["delivery"].get("delivery_dir", None)
    validation_failure_dir = params["validation"]["common"].get("validation_failure_dir", None)
    # Create validation_failure_dir if it doesn't exist
    if (validation_failure_dir is not None) and (not os.path.exists(validation_failure_dir)):
        os.mkdir(validation_failure_dir)
    # Double-checking that export-dir is not delivery-dir
    # Throw assertion error if delivery_dir or export_dir is unspecified
    assert(delivery_dir is not None and export_dir is not None)
    assert export_dir != delivery_dir
    files_to_delete = os.listdir(export_dir)
    for file_name in files_to_delete:
        if file_name.endswith(".csv") or file_name.endswith(".CSV"):
            if validation_failure_dir is not None:
                move(os.path.join(export_dir, file_name),
                    os.path.join(validation_failure_dir, file_name))
            else:
                os.remove(os.path.join(export_dir, file_name))
