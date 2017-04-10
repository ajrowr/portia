
import logging, os, json, re

import tractor, honcho


env = lambda k, d=None: os.environ.get(k, d)

log_format = logging.Formatter('%(asctime)s|%(name)s:%(funcName)s:%(lineno)d [%(levelname)s] %(message)s')
logfile_path = env('LOGFILE_PATH')
class LogHandlers(object):
    import logging
    import logging.handlers
    
    mem = logging.handlers.MemoryHandler(100000)
    log = logfile_path and logging.FileHandler(logfile_path) or logging.NullHandler()
    stderr = logging.StreamHandler()

class JsonHandler(logging.Handler):
    """
    Logging handler for use as a target for MemoryHandler.flush().
    Instanciate this, then call 
        memhandler.setTarget(jsonhandler_instance)
        memhandler.flush()
    to get 
    """
    records = None
    log_entries = None
    
    def __init__(self):
        self.records = []
        self.log_entries = []
        logging.Handler.__init__(self)
    
    def emit(self, record):
        self.records.append(record)
        self.log_entries.append(self.formatter.format(record))

def log_setup(level_stderr='WARNING', level_logfile='INFO', level_json='INFO'):
    """We log in three ways.
    i). stderr (ie. apache logs) <- default logger
    ii). JSON debug info sent back to the client <- memoryhandler
    iii). logfile. <- filehandler
    """
    rootlogger = logging.getLogger()
    rootlogger.setLevel('DEBUG')
    
    LogHandlers.stderr.setLevel(level_stderr)
    LogHandlers.mem.setLevel(level_json)
    LogHandlers.log.setLevel(level_logfile)
    
    LogHandlers.stderr.setFormatter(log_format)
    LogHandlers.mem.setFormatter(log_format)
    LogHandlers.log.setFormatter(log_format)
    
    rootlogger.addHandler(LogHandlers.stderr)
    rootlogger.addHandler(LogHandlers.mem)
    rootlogger.addHandler(LogHandlers.log)

def get_json_log():
    jsonlog = JsonHandler()
    jsonlog.setFormatter(log_format)
    LogHandlers.mem.setTarget(jsonlog)
    LogHandlers.mem.flush()
    return jsonlog


class Overseer(object):
    ## State
    source_modules = {}
    active_source = None
    instance_ident = None
    source_handler = None
    query = ''
    server_address = ''
    script_path = ''
    site_tag = None
    site_handle = None
    data_path = None
    job_status = {}
    log = None
    
    def __init__(self, *source_module_names):
        self.source_modules = dict([(m, __import__(m)) for m in source_module_names])
        self.log = logging.getLogger('overseer')
        inf = self.mine_environ()
        for (o_k, e_k) in [('instance_ident', 'instance_ident'), ('query', 'query'), 
                        ('site_tag', 'sitetag'), ('site_handle', 'sitehandle'), ('data_path', 'datapath'),
                        ('server_address', 'server_addr'), ('script_path', 'script_path')]:
            try:            
                setattr(self, o_k, inf[e_k])
            except:
                self.log.warning("Could not set {} from environment".format(o_k))
            
    def pathify(self, path):
        return os.path.join(self.data_path, path)

    @staticmethod
    def mine_environ(environ=os.environ):
        """Extract and process relevant info from the environment."""
        global env
        # env = lambda k: environ.get(k, None)
    
        uriparts = environ['REQUEST_URI'].split('?')
        scrpath = uriparts[0]
        sitehandle = scrpath.split('/')[-1]
        sitetag = sitehandle.split('_')[0]

        datapath = "{}/data/{}/".format(env('DOCUMENT_ROOT'), sitetag)
    
        return dict(
            instance_ident = environ['INSTANCE_IDENT'],
            query = env('QUERY_STRING', ''),
            server_addr = env('SERVER_ADDR', ''),
            script_path = scrpath,
            sitehandle = sitehandle,        ## identifier of the source eg. 'gruhhub'
            sitetag = sitetag,              ## string used to invoke the source, eg 'grubhub_e'
            datapath = datapath
        )
        # pathify = lambda fnam: os.path.join(datapath, fnam)

        # sys.stderr.write(ddutils.outprefix + '\n')
        # ddutils.outprefix = pathify('temp/tempfile_')
        # honcho.outprefix = pathify('temp/tempfile_') #### TODO UGH THIS IS HORRIBLE
        
        ## TODO ^^ honcho.outprefix?
    
    def load_status(self):
        try:
            spath = self.pathify('status.json')
            # self.log.debug("Attempting to load status from {}".format(spath))
            self.job_status = json.load(open(spath))
            self.log.debug("Status file loaded from {}".format(spath))
        except:
            self.log.warning("Could not load status from {}, creating new")
            self.job_status = {'run_stages': []}
        return self.job_status
        
    def save_status(self):
        # global pathify
        with open(self.pathify('status.json'), 'w') as statusfile:
            json.dump(self.job_status, statusfile, indent=4)

    def do_update(self, proceed=True):
        # global jobstatus, pathify, handler
        newstages = self.source_handler.check_stages(self.job_status.get('run_stages', []), proceed=proceed)
        self.job_status['run_stages'] = newstages
        self.save_status()
        return self.job_status

    def reset(self, stageidx=0):
        """Truncates the job status structure after stage $stageidx, where $stageidx is a natural number (ie. 1-based) or
        truncate the entire structure.
        This has the effect of resetting the process to after the given stage.
        """
        # global jobstatus, pathify, handler
        if stageidx:
            self.job_status['run_stages'] = self.job_status['run_stages'][:stageidx]
        else:
            self.job_status['run_stages'] = []
        self.save_status()
    
        
    def handle_apache_request(self):
        """This is a macro for all the things that need to be done, it basically wraps the whole process in one method."""
        envdata = self.mine_environ()
        #### TODO UGH THIS IS HORRIBLE
        #### TODO UGH THIS IS HORRIBLE
        honcho.outprefix = self.pathify('temp/tempfile_') #### TODO UGH THIS IS HORRIBLE
        #### TODO UGH THIS IS HORRIBLE
        #### TODO UGH THIS IS HORRIBLE
        
        ## Determine active source and configure it
        valid_sources = filter(lambda s: s[1].permit(self), self.source_modules.items())
        if len(valid_sources) > 1:
            self.log.warning('Multiple valid sources were found.')
            
        my_source_key, my_source = valid_sources[0]
        self.log.info("Using source - {}".format(my_source))
        
        self.active_source = my_source
        my_source.configure(self)
        
        ## Run parse non-source args and config, set up extractors
        config = json.load(open(self.pathify('config.json')))['configurations'][self.instance_ident]
        self.load_status()
        extractors = dict([(k, tractor.ImportioExtractor(c['ident'])) for k, c in config['extractors'].items()])
        for (k, ex) in extractors.items():
            self.log.debug("Got extractor '{}' ({})".format(k, ex.ident))
        # self.log.debug("Got extractors: {}".format(''.join(["\n   {}: {}".format(k, ex.ident) for (k, ex) in extractors.items()])))
        self.source_handler = self.active_source.handlerclass(extractors)
        
        ## Handle web request, including continue & reset directives
        message = {}
        if self.server_address:
            self.log.debug("Using query string '{}'".format(self.query))
            if 'reset' in self.query:
                r = re.compile('from(\d+)')
                rg = r.match(self.query)
                stageidx = 0
                if rg:
                    try:
                        stageidx = int(rg.groups()[0])
                        do_reset = True
                    except:
                        log.warning("Could not parse stage reset index from query string '{}'".format(self.query))
                        do_reset = False
                else:
                    do_reset = True
                if do_reset:
                    self.log.info("Resetting from stage {}".format(stageidx))
                    self.reset(stageidx)
            
            try:
                if 'continue' in self.query:
                    self.do_update(proceed=True)
                    self.log.debug("Continuing process")
                else:
                    self.do_update(proceed=False)
                
            except Exception, exc:
                message = dict(
                    text = str(exc),
                    type = 'ERROR'
                )
                try:
                    self.active_source.log.exception(exc)
                except:
                    log.exception(exc)
                
        # self.save_status() ## redundant since reset and do_update save the status
        
        status = dict(
            status = self.job_status,
            message = message,
            debug = dict(
                log = get_json_log().log_entries,
                extractors = str(extractors),
                scrpath = self.script_path,
                # args=argsraw,
                sitetag = self.site_tag,
                # status=jobstatus,
                config = config,
                env = dict(
                    HTTP_HOST = env('HTTP_HOST'),
                    REMOTE_ADDR = env('REMOTE_ADDR'),
                    SCRIPT_NAME = env('SCRIPT_NAME'),
                    QUERY_STRING = env('QUERY_STRING'),
                    REQUEST_URI = env('REQUEST_URI'),
                    DOCUMENT_ROOT = env('DOCUMENT_ROOT'),
                    URLS_LIMIT = env('URLS_LIMIT'),
                    DEBUG_MODE = env('DEBUG_MODE'),
                    INSTANCE_IDENT = env('INSTANCE_IDENT'),
                    IMPORT_IO_API_KEY = env('IMPORT_IO_API_KEY'),
                )
                # env=os.environ
            )
        )

        print "Content-type: application/json\n"
        print json.dumps(status, indent=4)
                    
    


if __name__ == '__main__':
    o = piper.Overseer('grubhub', 'doordash', 'ubereats')
    o.configure_source()
    o.handle_apache_request()
    o.mine_environ()
    source_module.configure(environ) ## << lets a source module do its own config eg. regions and whatnot
    o.handle_apache_request


