"""Pulls data from places API."""
import json
import time
from functools import partial
import multiprocessing as mp
from datetime import timedelta
import requests
import pandas as pd

URL = "https://api.safegraph.com/v2/graphql"
QUERY = """
query search($last_cursor: String, $bar_res: Int, $start_date: DateTime!, $end_date: DateTime!){
  search(filter: {naics_code: $bar_res, address: {iso_country_code: "US"} }) {
    weekly_patterns(start_date: $start_date end_date: $end_date) {
      results (first: 500 after: $last_cursor){
        edges {
          cursor
          node {
          placekey
          visits_by_day{visits}
          location_name
          brands{brand_id}
          date_range_start
          date_range_end
          postal_code
          }
        }
      }
    }
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
            "start_date": (day - timedelta(days=1)).strftime("%Y-%m-%d"),
            "end_date": day.strftime("%Y-%m-%d"),
            "last_cursor": "",
            "bar_res": naics_code
        }
    }
    headers = {
        "apikey":params['indicator']['apikey'],
        "content-type":"application/json"
    }
    return query, headers

CURSOR_SIGNPOSTS = {
    722410: ["",
             "V2Vla2x5UGF0dGVybnM6MjIzLTIyMkA4Z2cteHlrLWZzNSw=",
             "V2Vla2x5UGF0dGVybnM6MjI3LTIyMkA2M3YtZDI2LThuNSw=",
             "V2Vla2x5UGF0dGVybnM6MjJuLTIyMkA1cHctYzU5LXl5OSw=",
             "V2Vla2x5UGF0dGVybnM6enp3LTIyM0A1cGotbmRzLXl2eiw=",
             None],
    722511: ["",
             "V2Vla2x5UGF0dGVybnM6MjIzLTIyMkA1emItdm5mLXZwdiw=",
             "V2Vla2x5UGF0dGVybnM6MjI2LTIyMkA2Mjctd2RtLXgzcSw=",
             "V2Vla2x5UGF0dGVybnM6MjJqLTIyM0A2MjctdGtiLXZqOSw=",
             "V2Vla2x5UGF0dGVybnM6enp3LTIyM0A1cHctNmZrLXdwOSw=",
             None]
}

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


