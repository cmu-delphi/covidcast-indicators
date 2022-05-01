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

def pull_one(query, headers):
    """Perform single query attemp, with light cleaning."""
    for _ in range(5):
        success = True
        req = requests.post(
            URL,
            headers=headers,
            data=json.dumps(query))
        try:
            response = req.json()
            dump = json.dumps(response["data"]["search"]["weekly_patterns"]["results"]["edges"])
            dump = json.loads(dump)
            nodes = [i['node'] for i in dump]
            cursor = dump[-1]['cursor']
            df = pd.DataFrame(nodes)
            first_placekey = df.placekey[0]
            n = response["extensions"]["row_count"]
            last_placekey = df.placekey[n-1]
            break
        except json.decoder.JSONDecodeError as e:
            print(f"Expected JSON; got:\n'''{req.text}'''")
            time.sleep(1)
            success = not success
        except (KeyError,AttributeError) as e:
            print(f"KeyError or AttributeError; got: {e}\n'''{req.text}'''")
            time.sleep(1)
            success = not success
        except BaseException as e: # pylint: disable=W0703
            print(f"Unexpected {e}, {type(e)}")
            time.sleep(1)
            success = not success
    if not success:
        raise RuntimeError
    print(f"{n} records: {first_placekey} -- {last_placekey}")
    return response, df, (cursor if n == BATCH else None), last_placekey

def make_request(params, day, naics_code):
    """Make a request in Places API."""
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

def pull(params, day, naics_code):
    """Make queries in Places API, using multiprocessing."""
    query, headers = make_request(params, day, naics_code)
    job_pull = partial(pull_until, query=query, headers=headers)
    job_args = list(zip(
        CURSOR_SIGNPOSTS[naics_code][:-1],
        CURSOR_SIGNPOSTS[naics_code][1:]
    ))
    with mp.Pool(len(job_args)) as pool:
        all_out = pool.map(job_pull, job_args)
    out_dict = dict(all_out)
    # the last df of the first signpost may overlap with the first df of the
    # second signpost, and so on
    dfs = out_dict[job_args[0]]
    for i in range(1, len(job_args)):
        dfs[-1] = pd.concat([dfs[-1], out_dict[job_args[i]][0]])\
            .drop_duplicates(subset = ['placekey'])
        dfs.extend(out_dict[job_args[i]][1:])
    return pd.concat(dfs)

def pull_until(cursor_bookends, query, headers):
    """Make queries in Places API in sets of 500, until finish."""
    curtime = time.time()
    start_cursor, end_cursor = cursor_bookends
    query = json.loads(json.dumps(query)) # make a copy before editing
    query["variables"]["last_cursor"] = start_cursor
    dfs = []
    while True:
        _, df, last_cursor, _ = pull_one(query, headers)
        dfs.append(df)
        if last_cursor is None:
            print(f"{cursor_bookends}: last_cursor {last_cursor}")
            break
        if (end_cursor is not None) and (num_first_comp(end_cursor, last_cursor)):
            print(f"{cursor_bookends}: last_cursor {last_cursor}")
            break
        if query["variables"]["last_cursor"] is last_cursor:
            print(f"Bad repeat: {last_cursor}")
            break
        query["variables"]["last_cursor"] = last_cursor
        time.sleep(0.1)
    print(f"Finished multiprocessing job for {cursor_bookends},\
        duration {(time.time() - curtime)} seconds")
    return cursor_bookends, dfs

def num_first_comp(x, y):
    """Return true if Y >= X, Numbers are larger than alphabets."""
    comp_len = min(len(x), len(y))
    for i in range(comp_len):
        xi = x[i]
        yi = y[i]
        if xi == yi:
            continue
        # Compare Digits first
        if xi.isdigit() and yi.isdigit():
            return int(yi) > int(xi)
        if xi.isdigit() and not yi.isdigit():
            return False
        if not xi.isdigit() and yi.isdigit():
            return True
        return yi >= xi
    return False
