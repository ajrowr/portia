
import datetime, StringIO, csv, sys, os, urllib
import requests.packages.urllib3
import json
requests.packages.urllib3.disable_warnings()


class Keychain(object):
    """
    Since it's a common use case to work with multiple extractors on multiple accounts, 
    keychain makes this simpler by keeping track of account keys. Since an extractor's basic info
    can be used to discover which account owns it, and API keys allow inference of the account GUID, 
    an extractor can tell us which key it needs.
    """
    
    _keys = {}
    
    def add_api_key(self, apikey):
        k = apikey[:32]
        self._keys[k] = apikey
    
    def add_account(self, account):
        print "Adding account {}".format(account)
        self.add_api_key(account.apikey)
    
    def get_user_key(self, guid):
        myguid = guid.replace('-', '')
        return self._keys[myguid]


keychain = Keychain()
try:
    apikey_default = os.environ['IMPORT_IO_API_KEY']
    keychain.add_api_key(apikey_default)
except:
    print "WARNING: No default API key found in environment. Set IMPORT_IO_API_KEY to your API key to enable keychain features"
    apikey_default = None

    

_run_fmt = "{state:12} {success:24} {start:16} {finish:16} {rowCount:>10} {ident}"

def _pytime(ts):
    return ts and datetime.datetime.fromtimestamp(ts/1000) or None

_timefmt = lambda dt: dt.strftime('%b %e %H:%M')
        
def _format_run_info(raw):
    r = raw['fields']
    c = dict([(k, r.get(k+'UrlCount', 0)) for k in ('total', 'success', 'failed')])
    
    started_at = _pytime(r['startedAt'])
    stopped_at = _pytime(r.get('stoppedAt'))
    aug = [
        ('success', "{success:>6} of {total:>6}".format(**c)),
        ('start', _timefmt(started_at)),
        ('finish', stopped_at and _timefmt(stopped_at) or '...'),
        ('ident', r['extractorId']),
    ]
    return dict(aug + r.items())


class ImportioArtifact(object):
    _apikey = None
    account = None
    keychain = None
    _info = None
    
    @property
    def apikey(self):
        if not self._apikey:
            if self.account:
                self._apikey = self.account.apikey
            elif apikey_default:
                kc = self.keychain or keychain
                self._apikey = apikey_default ## seed with environment default apikey
                try:
                    self._apikey = kc.get_user_key(self.info['_meta']['ownerGuid'])
                except:
                    print "Key not found in keychain. Using generic key with limited access."
        
        return self._apikey
    
    @apikey.setter
    def apikey(self, value):
        print "Setting apikey"
        self._apikey = value


class ImportioAccount(object):
    apikey = None
    
    def __init__(self, apikey=None):
        self.apikey = apikey
    
    def runs_get_raw(self, page=1):
        u = ("https://store.import.io/store/crawlrun/_search"+\
            "?_sort=_meta.creationTimestamp&_page={page}&_perPage=30"+\
            "&_apikey={apikey}").format(apikey=self.apikey, page=page)
        # print u
        resp = requests.get(u)
        # print resp, resp.content
        rundat = resp.json()['hits']['hits']
        return rundat
    
    def runs_get(self, page=1):
        return [ImportioCrawlRun(ident=raw['_id'], account=self, info=raw['fields']) for raw in self.runs_get_raw(page=page)]
    
    def runs_show(self, active_only=True, page=1):
        rundat = self.runs_get_raw(page=page)
        print _run_fmt.format(state="Status",        success="Success", 
                        rowCount="Row count",   start="Started at",     
                        finish="Finished at", ident="Extractor")
        print '-' * 100
        print '\n'.join([_run_fmt.format(**_format_run_info(run)) for run in rundat])
    
    def extractors_get(self, page=None):
        ubase = ("http://store.import.io/store/extractor/_search"+\
            "?_sort=_meta.creationTimestamp&_mine=true&"+\
            "_size=50&_page={{page}}"+\
            "&_apikey={apikey}").format(apikey=self.apikey)
        
        rundat = []
        proceed = True
        def appendpage(p, rundat):
            u = ubase.format(page=page)
            resp = requests.get(u)
            respjs = resp.json()
            total = respjs['hits']['total']
            hits = respjs['hits']['hits']
            rundat += hits
            return len(hits) >= 20
        if page:
            appendpage(page, rundat)
        else:
            page = 1
            # print appendpage(page, rundat)
            while appendpage(page, rundat):
                sys.stdout.write('.')
                sys.stdout.flush()
                # print '..'
                page += 1
            print
            
        #
        # for p in range()
        # u = ubase.format(page=page)
        # resp = requests.get(u)
        # respjs = resp.json()
        # total = respjs['hits']['total']
        # rundat = respjs['hits']['hits']
        return rundat
    
    def extractors_show(self, page=None):
        extdat = self.extractors_get(page=page)
        _ext_fmt = "{id:40} {name:50}"
        print _ext_fmt.format(id="Ident", name="Name")
        print '-' * 100
        print '\n'.join([_ext_fmt.format(id=ext['_id'], **ext['fields']) for ext in extdat])
    
    def extractors_dict(self, page=None):
        _xf = lambda ext, f: ext['fields'][f]
        return dict([(_xf(ext, 'name'), ImportioExtractor(ext['_id'], account=self, info=ext)) for ext in self.extractors_get(page=page)])


me = ImportioAccount(apikey_default)


class ImportioCrawlRun(object):
    ident = None
    account = None
    apikey = None
    # extractor = None
    _info = None
    _extractor = None
    _log = None
    
    def __init__(self, ident=None, account=None, extractor=None, apikey=None, info=None):
        self.ident = ident
        self.account = account
        self.apikey = apikey or account and account.apikey
        
        # self.extractor = extractor
        self._info = info
    
    def __repr__(self):
        return "<{}.{} [{}] at {}>".format(self.__class__.__module__, self.__class__.__name__, self.ident, hex(id(self)))
    
    def _url(self, url_template, *args, **kwargs):
        return url_template.format(cr_id=self.ident, apikey=self.apikey, **kwargs)
    
    @property
    def info(self):
        if not self._info:
            u = self._url("https://store.import.io/store/crawlrun/{cr_id}?_apikey={apikey}")
            resp = requests.get(u)
            self._info = resp.json()
        return self._info
    
    @property
    def extractor(self):
        if not self._extractor:
            self._extractor = ImportioExtractor(self.info['extractorId'], account=self.account)
        return self._extractor
    
    @property
    def log(self):
        if not self._log:
            # u = self._url("https://store.import.io/store/crawlRun/{cr_id}/_attachment/log/{logident}", logident=self.info['log'])
            # self._log = 
            resp = self.attachment_get_response('log')
            self._log = csv.DictReader(StringIO.StringIO(resp.content))
        return self._log
            
    
    def attachment_get_response(self, type_name):
        urlbase = self._url("https://store.import.io/store/crawlrun/{cr_id}/_attachment/{{type_name}}/{{type_ident}}?_apikey={apikey}")
        ## crawlrun_type is one of json, csv, log
        ## crawlrun_type_ident is the ident of the specific type of crawlrun from the crawlrun struct
        type_ident = self.info[type_name]
        resp = requests.get(urlbase.format(type_name=type_name, type_ident=type_ident))
        return resp
    
    def attachment_get_csv_dictreader(self):
        resp = self.attachment_get_response('csv')
        body = StringIO.StringIO(resp.content)
        body.read(3) ## Throw away the BOM
        return csv.DictReader(body)
        
    @staticmethod
    def runs_search(apikey, raw=False, page=1, **kwargs):
        u = ("https://store.import.io/store/crawlrun/_search"+\
            "?_sort=_meta.creationTimestamp&_page={page}&_perPage=30"+\
            (kwargs and ('&' + urllib.urlencode(kwargs)) or '') +\
            "&_apikey={apikey}").format(apikey=apikey, page=page)
        print u
        resp = requests.get(u)
        if raw:
            return resp
        else:
            return resp.json()['hits']['hits']
        
    @classmethod
    def runs_get(klass, apikey, page=1, **kwargs):
        return [klass(hit['_id'], apikey=apikey, info=hit['fields']) for hit in klass.runs_search(apikey, page=page, **kwargs)]
        


class ImportioExtractor(ImportioArtifact):
    ident = None
    # apikey = None
    label = None
    account = None
    keychain = None
    _info = None
    _data = None
    def __init__(self, ident=None, account=None, keychain=keychain, label=None, apikey=None, info=None, *args, **kwargs):
        self.ident = ident
        self.account = account
        # self.apikey = apikey or account and account.apikey
        self.label = label
        self.keychain = keychain
        self._info = info
    
    def __repr__(self):
        # inf = "Extractor ID {}".format(self.ident)
        return "<{}.{} [{}] at {}>".format(self.__class__.__module__, self.__class__.__name__, self.ident, hex(id(self)))
    
    def _url(self, url_template, *args, **kwargs):
        return url_template.format(xid=self.ident, apikey=(self.apikey or apikey_default), **kwargs)
    
    def _patch(self, *args, **kwargs):
        u = self._url("https://store.import.io/store/extractor/{xid}?_apikey={apikey}")
        resp = requests.patch(u, headers={'Content-Type':'application/json'}, data=json.dumps(kwargs))
        return resp
    
    def get_csv(self):
        if not self._data:
            url_tmpl = "https://data.import.io/extractor/{xid}/csv/latest?_apikey={apikey}"
            req = requests.Request(method='GET', url=url_tmpl.format(apikey=self.apikey, xid=self.ident),
                    headers={'Accept-Encoding': 'gzip'})
            preq = req.prepare()
            ses = requests.Session()
            resp = ses.send(preq)
            if resp.status_code == 200:
                body = StringIO.StringIO(resp.content)
                body.read(3) ## Throw away the BOM
                self._data = csv.DictReader(body)
            else:
                print resp, resp.content[:200]
                raise Exception("It didn't work")
        return self._data
    
    def download_csv_as(self, filename):
        url_tmpl = "https://data.import.io/extractor/{xid}/csv/latest?_apikey={apikey}"
        resp = requests.get(self._url(url_tmpl), headers={'Accept-Encoding': 'gzip'})
        if resp.status_code == 200:
            body = StringIO.StringIO(resp.content)
            body.read(3)
        else:
            raise("Unexpected status code:".format(resp.status_code))
        with open(filename, 'w') as fout:
            fout.write(body.read())
        
    
    def reset(self):
        self._data = None
        return self
    
    def fields_get(self):
        return self.get_csv().fieldnames
    
    def data(self):
        return (r for r in self.get_csv())
        
    def data_fields(self, *fields):
        if not fields:
            fields = self.fields_get()
            
        if len(fields) == 1:
            return (r[fields[0]] for r in self.get_csv())
        elif len(fields) > 1:
            return (dict([(f, r[f]) for f in fields]) for r in self.get_csv())
        
    def urls_put(self, urls):
        utmpl = "https://store.import.io/store/extractor/{xid}/_attachment/urlList?_apikey={apikey}"
        r = requests.put(self._url(utmpl), data='\n'.join(urls), headers={'Content-Type': 'text/plain'})
        return r.json()
    
    def urls_get(self):
        inf = self.info
        u = self._url('https://store.import.io/store/extractor/{xid}/_attachment/'+\
                    'urlList/{url_list_id}?_apikey={apikey}', url_list_id=inf['urlList'])
        resp = requests.get(u, headers={'Accept-Encoding': 'gzip'})
        if resp.status_code == 200:
            return resp.content.split('\n')
        return resp
            
    def start(self):
        utmpl = "https://run.import.io/{xid}/start?_apikey={apikey}"
        resp = requests.post(self._url(utmpl))
        return resp.json()
    
    def runs_get_raw(self):
        u = self._url("https://store.import.io/store/crawlrun/_search"+\
                "?_sort=_meta.creationTimestamp&_page=1&_perPage=30"+\
                "&extractorId={xid}&_apikey={apikey}")
        resp = requests.get(u)
        return resp.json()['hits']['hits']
    
    def current_run_status(self):
        statuses = self.runs_get_raw()
        curr = filter(lambda r: r['fields']['state'] == 'STARTED', statuses) or [None]
        return curr[0]
        
    def show_status(self):
        c = self.current_run_status()
        if c:
            t, s, f = [float(c['fields'][k]) for k in ('totalUrlCount', 'successUrlCount', 'failedUrlCount')]
            print "Processed {pr} of {t} urls ({pc}% complete)".format(t=t, pr=s+f, pc=(s+f)/t)
    
    def status(self):
        
        print _run_fmt.format(state="Status",        success="Success", 
                        rowCount="Row count",   start="Started at",     
                        finish="Finished at")
        print '-' * 100
        print '\n'.join([_run_fmt.format(**_format_run_info(ext)) for ext in self.runs_get_raw()])
    
    @property    
    def info(self):
        if not self._info:
            u = self._url("https://store.import.io/store/extractor/{xid}?_apikey={apikey}")
            resp = requests.get(u)
            self._info = resp.json()
        if not self.apikey and self.keychain:
            self.apikey = self.keychain.get_user_key(self._info['_meta']['ownerGuid'])
            
        return self._info
        # return resp.json()



"""
resp = requests.get('https://store.import.io/store/extractor/9a558a89-4a9a-4caa-a04b-4a38013723ba/_attachment/training/3a829d88-135d-4da7-8d49-01dd6d51675e?_apikey='+proc2.xtrac.me.apikey)
"""

class ImportioExtractorTemplate(object):
    """
    Uses this process to construct an extractor from a template:
    - create extractor by POSTing synthesized extractor struct
    - create runtime config by POSTing synthesized rtc struct
    - PATCH extractor to use url list
    - add a URL list
    
    Fields is a list of dicts with format:
        name
        captureLink: bool
        type: 
        ranking: 
        xpath:
        ...
    Config:
        singleRecord: bool
        recordXPath: 
        noscript: bool
        
    """
    
    fields = []
    config = {}
    name = None

    def __init__(self, name=None, fields=[], config={}):
        self.config = config
        self.fields = fields
        self.name = name
    
    def _synth_extractor(self):
        mkfield = lambda fspec: dict(
            type = fspec.get('type', 'TEXT'),
            captureLink = fspec.get('captureLink', False),
            name = fspec.get('name', 'Unnamed')
        )
        fields = [mkfield(f) for f in self.fields]
        return dict(
            name = self.name,
            fields = fields
        )
    
    def _synth_runtime_config(self, extractorident):
        mkfield = lambda fspec: dict(
            type = fspec.get('type', 'TEXT'),
            captureLink = fspec.get('captureLink', False),
            name = fspec.get('name', 'Unnamed'),
            ranking = fspec.get('ranking', 0),
            xpath = fspec.get('xpath', '.')
        )
        fields = [mkfield(f) for f in self.fields]
        return dict(
            config = dict(
                singleRecord = self.config.get('singleRecord', False),
                recordXPath = self.config.get('recordXPath', '/html'),
                noscript = self.config.get('noscript', False),
                fields = fields
            ),
            extractorId = extractorident
        )
        
    
    def create(self, account):
        import json
        u = lambda fr: "https://store.import.io/store/{}?_apikey=".format(fr) + account.apikey
        hdr = {'Content-Type':'application/json'}
        # resp1 = requests.post('https://store.import.io/store/extractor?_apikey={}'.format(proc2.xtrac.me.apikey), headers={'Content-Type':'application/json'}, data=json.dumps(proc2.xyztemplate._synth_extractor()))
        # resp2 = requests.post("https://store.import.io/store/runtimeconfiguration?_apikey={}".format(proc2.xtrac.me.apikey), headers={'Content-Type':'application/json'}, data=json.dumps(proc2.xyztemplate._synth_runtime_config(resp1.json()['guid'])))
        # resp3 = requests.patch("https://store.import.io/store/extractor/{}?_apikey={}".format(xg, proc2.xtrac.me.apikey), headers={'Content-Type':'application/json'}, data=json.dumps(dict(latestConfigId=resp2.json()['guid'])))

        # print u('extractor')
        resp1 = requests.post(u('extractor'), headers=hdr, data=json.dumps(self._synth_extractor()))
        # print resp1
        x_guid = resp1.json()['guid']
        
        # print u('runtimeconfiguration')
        resp2 = requests.post(u('runtimeconfiguration'), headers=hdr, data=json.dumps(self._synth_runtime_config(x_guid)))
        # print resp2
        rtc_guid = resp2.json()['guid']
        
        # print u("extractor/{}".format(x_guid))
        resp3 = requests.patch(u("extractor/{}".format(x_guid)), headers=hdr, data=json.dumps({'latestConfigId':rtc_guid}))
        # print resp3    
        
        return ImportioExtractor(x_guid, account=account)
        
        
    