"""Change the json template to run the package."""

import argparse
import json
from pathlib import Path


def get_hosp(edi_file, geo_dir, out_dir, receiving_dir, se):
    """Output the json.params file needed to run the claims_hosp package.
    
    Args:
        edi_file: Path to EDI file (claims)
        geo_dir: Dir containing the geo_map files
        out_dir: Output dir to put the json file
        receiving_dir: Output dir to put the hosp estimates
        se: Boolean to output SEs or not
    """

    if se:
        weekday = [True]
    else:
        weekday = [True, False]

    data = {
        "common": {
            "export_dir": receiving_dir,
            "log_exceptions": False
        },
        "indicator": {
            "input_file": edi_file,
            "start_date": None,
            "end_date": None,
            "drop_date": None,
            "n_backfill_days": 70,
            "n_waiting_days": 3,
            "write_se": se,
            "obfuscated_prefix": "wip_henear",
            "parallel": True,
            "geos": ["state", "msa", "hrr", "county", "hhs", "nation"],
            "weekday": weekday
        }
    }
    print(f"Using {edi_file}")

    with open(Path(out_dir) / 'params.json', 'w') as outfile:
        json.dump(data, outfile, indent=4)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('edi_file')
    parser.add_argument('geo_dir')
    parser.add_argument('out_dir')
    parser.add_argument('receiving_dir')
    parser.add_argument('--se', action="store_true")

    args = parser.parse_args()
    get_hosp(args.edi_file, args.geo_dir,
             args.out_dir, args.receiving_dir, args.se)
