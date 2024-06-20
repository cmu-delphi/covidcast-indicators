import json
import subprocess
from os import makedirs
from delphi_utils import read_params, get_structured_logger
from delphi_doctor_visits.run import run_module
from datetime import datetime, timedelta

if __name__ == "__main__":
    params = read_params()
    logger = get_structured_logger(__name__, filename=params["common"]["log_filename"])

    start_issue = datetime.strptime(params["patch"]["start_issue"], "%Y-%m-%d")
    end_issue = datetime.strptime(params["patch"]["end_issue"], "%Y-%m-%d") 

    logger.info(f"""Start patching {params["patch"]["patch_dir"]}""")
    logger.info(f"""Start issue: {start_issue.strftime("%Y-%m-%d")}""")
    logger.info(f"""End issue: {end_issue.strftime("%Y-%m-%d")}""")

    makedirs(params["patch"]["patch_dir"], exist_ok=True)

    current_issue = start_issue

    while current_issue <= end_issue:
        logger.info(f"""Running issue {current_issue.strftime("%Y-%m-%d")}""")

        params["patch"]["current_issue"] = current_issue.strftime("%Y-%m-%d")

        current_issue_dir = f"""{params["patch"]["patch_dir"]}/issue_{params['patch']['current_issue'].replace("-", "")}/nssp"""
        params["common"]["export_dir"] = f"""{current_issue_dir}"""
        makedirs(f"{current_issue_dir}", exist_ok=True)

        run_module(params)
        current_issue += timedelta(days=1)
