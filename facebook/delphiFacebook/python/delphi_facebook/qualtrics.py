#!/bin/python3
import requests
import json
from time import sleep
import zipfile
import io
from datetime import *
from dateutil import tz
import sys
import os

from .dryrun import _make_fake_fetchers

BASE_URL="https://ca1.qualtrics.com/API"
TIMEZONE = "America/Los_Angeles"
TZ = tz.gettz(TIMEZONE)

def progress(t):
    n=2*(t+1)
    return '.'*min(10,n),pow(2,max(0,t-4))

def dicta(*dcts):
    ret = dict()
    for d in dcts: ret.update(d)
    return ret

def _make_url(endpoint):
    return f'{BASE_URL}/v3/{endpoint}'

def make_fetchers(params):
    header = {'X-API-TOKEN': params['token']}
    if not params['token']:
        return _make_fake_fetchers(params,header,_make_url)
    def g(endpoint,**kw):
        url = _make_url(endpoint)
        r = requests.get(url,params=kw,headers=header)
        return r
    def p(endpoint,data):
        url = _make_url(endpoint)
        r = requests.post(url,json=data,headers=header)
        return r
    return g,p

def get(fetch,post,params):
    resp=fetch("whoami")
    if not resp.ok: return resp
    resp=fetch("surveys")
    if not resp.ok: return resp
    results=[]
    for surv in resp.json()['result']['elements']:
        if not surv['isActive']: continue
        if not surv['name'] in params['surveys']['active']: continue
        print(json.dumps(surv,sort_keys=True,indent=3))
        base  = f"surveys/{surv['id']}/export-responses/"
        # Fully cumulative:
        #start = datetime.combine(date(2020,4,6),time(00,00,00),tzinfo=TZ)

        # Transitional period:
        # place starter files for DEPLOY and US EXPANSION from 4-6 to 4-19 in qualtrics folder
        # subsequent downloads will start at 19 morning
        # revisit when we shift to fully incremental mode
        #start = datetime.combine(date(2020,4,19),time(00,00,00),tzinfo=TZ)

        # Fully incremental: 7 days to capture backfill
        start = datetime.combine(date.today()-timedelta(days=7),time(00,00,00),tzinfo=TZ)

        # Account for StartDate->RecordedDate lag:
        end   = datetime.combine(date.today(),time(4,00,00),tzinfo=TZ)
        r = post(base,{
            "format":"csv",
            "timeZone":TIMEZONE,
            "startDate":start.isoformat(),
            "endDate":end.isoformat(),
                        "breakoutSets":"false",
        })
        if not r.ok: return r
        progressId = r.json()['result']['progressId']
        print(r.text)
        progressStatus = "inProgress"
        t=0
        wait,waitt = progress(t)
        while progressStatus != "complete" and progressStatus != "failed":
            t+=1
            r = fetch(f"{base}{progressId}")
            if not r.ok: return r
            progressStatus = r.json()['result']['status']
            pct = r.json()['result']['percentComplete']
            print(f"{progressStatus}: {pct}")
            if pct<100:
                for i in wait:
                    sleep(waitt)
                    print(i,end="",flush=True)
                sleep(waitt)
                print()
            wait,waitt = progress(t)
        if progressStatus=="failed":
            raise Exception(f"ERROR: could not download \"{surv['name']}\"\n{json.dumps(r.json(),sort_keys=True,indent=2)}")
        fileId = r.json()['result']['fileId']
        r = fetch(f"{base}{fileId}/file")
        if not r.ok: return r
        outfilename=f"{date.today()}.{start.date()}.{end.date()}.{surv['name'].replace(' ','_')}.csv"
        if r.ok == "dry-run":
            print(f"SAVE {outfilename}")
        else:
            z = zipfile.ZipFile(io.BytesIO(r.content))
            for n in z.namelist():
                with open(os.path.join(params['qualtrics_dir'],
                                       outfilename),'wb') as out:
                    out.write(z.read(n))
                break
        results.append(r)
    return results
