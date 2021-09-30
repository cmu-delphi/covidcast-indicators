import requests
from datetime import date
import pandas as pd
import json
import time
from functools import partial
import multiprocessing as mp

URL = "https://placesapi.safegraph.com/v1/graphql/bulk"
QUERY = """
query AcademicBulkWeeklyPatterns(
  $date: DateTime!
  $iso_country_code: String
  $last_seen_placekey: String
) {
  bulk_weekly_patterns(
    date: $date
    iso_country_code: $iso_country_code
    last_seen_placekey: $last_seen_placekey
  ) {
    placekey
    safegraph_brand_ids
    visits_by_day
    date_range_start
    date_range_end
    postal_code
  }
}
"""
BATCH = 500

def pull_one(query, headers, brand_ids):
    while True:
        req = requests.post(
            URL,
            headers=headers,
            data=json.dumps(query))
        try:
            response = req.json()
            df = pd.read_json(
                json.dumps(response["data"]["bulk_weekly_patterns"])
            )
            first_placekey = df.placekey[0]
            n = response["extensions"]["row_count"] 
            last_placekey = df.placekey[n-1]
            df = df[df["safegraph_brand_ids"].isin(brand_ids)]
            break
        except json.decoder.JSONDecodeError as e:
            req.raise_for_status()
            print(f"Expected JSON; got:\n'''{req.text}'''")
            time.sleep(1)
        except (KeyError,AttributeError) as e:
            print(df)
            print(req.text)
            raise
    print(f"{query['variables']['last_seen_placekey']}: [{len(df)} of {n}] {first_placekey} -- {last_placekey}")
    return response, df, (last_placekey if n == BATCH else None)

def make_request(params, day):
    query = {
        "query": QUERY,
        "variables": {
            "date": day.strftime("%Y-%m-%d"),
            "iso_country_code": "US",
            "last_seen_placekey": ""
        }
    }
    headers = {
        "apikey":params['indicator']['apikey'],
        "content-type":"application/json"
    }
    return query, headers

PLACEKEY_SIGNPOSTS = ["", None]#["", "222-226", "225-223", "22s-223", "zzw-223", None]
def pull(params, day, brand_ids):
    query, headers = make_request(params, day)
    job_pull = partial(pull_until, query=query, headers=headers, brand_ids=brand_ids)
    job_args = list(zip(
        PLACEKEY_SIGNPOSTS[:-1],
        PLACEKEY_SIGNPOSTS[1:]
    ))
    print(f"{len(job_args)}: {job_args}")
    with mp.Pool(len(job_args)) as pool:
        all_out = pool.map(job_pull, job_args)
    out_dict = dict(all_out)
    # the last df of the first signpost may overlap with the first df of the
    # second signpost, and so on
    dfs = out_dict[job_args[0]]
    for i in range(1, len(job_args)):
        dfs[-1] = pd.concat([dfs[-1], out_dict[job_args[i]][0]]).drop_duplicates()
        dfs.extend(out_dict[job_args[i]][1:])
    return pd.concat(dfs)
    


def pull_until(placekey_bookends, query, headers, brand_ids):
    start_placekey, end_placekey = placekey_bookends
    query = json.loads(json.dumps(query)) # make a copy before editing
    query["variables"]["last_seen_placekey"] = start_placekey
    dfs = []
    while True:
        response, df, last_placekey = pull_one(query, headers, brand_ids)
        dfs.append(df)
        if last_placekey is None:
            print(f"{placekey_bookends}: last_placekey {last_placekey}")
            break
        if (end_placekey is not None) and (last_placekey > end_placekey):
            print(f"{placekey_bookends}: last_placekey {last_placekey}")
            break
        if query["variables"]["last_seen_placekey"] is last_placekey:
            print(f"Bad repeat: {last_placekey}")
            break
        query["variables"]["last_seen_placekey"] = last_placekey
        time.sleep(0.1)
    return placekey_bookends, dfs


