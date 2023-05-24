"""Indicator running utilities."""
import argparse as ap
import importlib
import os
from typing import Any, Callable, Dict, Optional
import multiprocessing
import time
from .archive import ArchiveDiffer, archiver_from_params
from .logger import get_structured_logger
from .utils import read_params, transfer_files, delete_move_files
from .validator.validate import Validator
from .validator.run import validator_from_params


Params = Dict[str, Any]

# Trivial function to use as default value for validator and archive functions.
NULL_FN = lambda x: None

def run_indicator_pipeline(indicator_fn:  Callable[[Params], None],
                            flash_fn: Callable[[Params], None] = NULL_FN,
                           validator_fn:  Callable[[Params], Optional[Validator]] = NULL_FN,
                           archiver_fn:  Callable[[Params], Optional[ArchiveDiffer]] = NULL_FN,
                             timer=1):
    """Run an indicator with its optional validation and archiving.

    Each argument to this function should itself be a function that will be passed a common set of
    parameters (see details below).  This parameter dictionary should have five subdictionaries
    keyed as "indicator", "validation", "archive", "flash", and "common" corresponding to parameters
    to be used in `indicator_fn`, `validator_fn`, `archiver_fn`, `flash_fn` and shared across
    functions, respectively.The timer function stops the flash process after a certain time.

    Arguments
    ---------
    indicator_fn: Callable[[Params], None]
        function that takes a dictionary of parameters and produces indicator output
    flash_fn: Callable[[Params], None]
        function that takes a dictionary of parameters and writes points of interest to the log.
    validator_fn: Callable[[Params], Optional[Validator]]
        function that takes a dictionary of parameters and produces the associated Validator or
        None if no validation should be performed.
    archiver_fn: Callable[[Params], Optional[ArchiveDiffer]]
        function that takes a dictionary of parameters and produces the associated ArchiveDiffer or
        None if no archiving should be performed.
    """
    params = read_params()
    logger = get_structured_logger(
        name=indicator_fn.__module__,
        filename=params["common"].get("log_filename", None),
        log_exceptions=params["common"].get("log_exceptions", True))

    #Get version and indicator name for startup
    ind_name = indicator_fn.__module__.replace(".run", "")
    #Check for version.cfg in indicator directory
    if os.path.exists("version.cfg"):
        with open("version.cfg") as ver_file:
            current_version = "not found"
            for line in ver_file:
                if "current_version" in line:
                    current_version = str.strip(line)
                    current_version = current_version.replace("current_version = ", "")
    #Logging - Starting Indicator
        logger.info(f"Started {ind_name} with covidcast-indicators version {current_version}")
    else: logger.info(f"Started {ind_name} without version.cfg")

    indicator_fn(params)
    validator = validator_fn(params)
    archiver = archiver_fn(params)

    t1 = multiprocessing.Process(target=flash_fn, args=[params])
    t1.start()
    start = time.time()
    while time.time()-start < timer:
        if not t1.is_alive():
            break
        time.sleep(10)
    else:
        t1.terminate()
        t1.join()
    if validator:
        validation_report = validator.validate()
        validation_report.log(logger)
        # Validators on dry-run always return success
        if not validation_report.success():
            delete_move_files()
    if (not validator or validation_report.success()):
        if archiver:
            archiver.run(logger)
        if "delivery" in params:
            transfer_files()


if __name__ == "__main__":
    parser = ap.ArgumentParser()
    parser.add_argument("indicator_name",
                        type=str,
                        help="Name of the Python package containing the indicator.  This package "
                             "must export a `run.run_module(params)` function.")
    args = parser.parse_args()
    indicator_module = importlib.import_module(args.indicator_name)
    flash_module = importlib.import_module('delphi_utils.flash_eval.run')
    run_indicator_pipeline(indicator_module.run.run_module,
                           flash_module.run_module,
                           validator_from_params,
                           archiver_from_params,
                           timer=600
                           )
