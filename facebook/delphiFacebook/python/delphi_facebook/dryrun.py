# -*- coding: utf-8 -*-
"""Functions to call when running without a valid Qualtrics token
"""

NEWLINE="\n"

def _make_fake_fetchers(params,header,_make_url):
    from unittest.mock import MagicMock
    def g(endpoint,**kw):
        url = _make_url(endpoint)
        print(f'GET {url}\n{NEWLINE.join("   %s:%s" % (x,y) for (x,y) in header.items())}')
        r = MagicMock(ok=True)
        if endpoint == "surveys":
            r.json=MagicMock(return_value={
                'result':{'elements':[
                    {'isActive':True,'name':x,'id':'some-id'} for x in params['surveys']['active']
                ]}})
        if endpoint.endswith("some-progress-id"):
            r.json=MagicMock(return_value={
                'result':{'status':'complete', 'percentComplete':100, 'fileId':'some-file-id'}})
        if endpoint.endswith("file"):
            r.ok="dry-run"
        return r
    def p(endpoint,data):
        url = _make_url(endpoint)
        print(f'POST {url}\n{NEWLINE.join("   %s:%s" % (x,y) for (x,y) in header.items())}\n   {data}')
        r = MagicMock(ok=True)
        if endpoint.endswith("export-responses/"):
            r.json=MagicMock(return_value={
                'result':{'progressId':'some-progress-id'}})
            r.text="{'result':...,'meta':...}"
        return r
    return g,p
