
import sys, tempfile, datetime, time, csv, json
import tractor

class util(object):
    @staticmethod
    def with_temp_file(fn):
        outfname = ''
        with tempfile.NamedTemporaryFile(prefix=outprefix, delete=False) as outf:
            fn(outf)
            # extractor.download_csv_to(outf)
            outfname = outf.name
        return outfname

    @staticmethod
    def save_csv(extractor):
        def write_out(fout):
            extractor.download_csv_to(fout)
        return util.with_temp_file(write_out)    

    @staticmethod
    def timestamp_now():
        return 1000*int(time.mktime(datetime.datetime.now().timetuple()))
    
    @staticmethod
    def url_put_rider(u, dat):
        from urlparse import parse_qsl, urlparse, urlunparse
        from urllib import urlencode
        parsed = list(urlparse(u))
        qdict = dict(parse_qsl(parsed[4]) + [('_RIDER', json.dumps(dat))])
        parsed[4] = urlencode(qdict)
        return urlunparse(parsed)
    
    @staticmethod
    def url_get_rider(u):
        from urlparse import parse_qsl, urlparse, urlunparse
        try:
            return json.loads(dict(parse_qsl(urlparse(u).query)).get('_RIDER', '{}'))
        except:
            import sys
            sys.stderr.write(u + '\n')
            raise
    
    @staticmethod
    def log(message, level=0):
        sys.stderr.write(message + '\n')
    

class ProcessHandler(object):
    """
    Responsibilities:
    - recognize whether current status stage has ended
    - if it has, perform necessary intra-stage tasks and begin next stage
    - return either None or updated status info (updated list of stages)
    - each stage can be in various states. STARTED and FINISHED are standard, others are internal to the handler subclass
    """
    
    # stages_list = ['stage1_get_list', 'stage2_get_locationcookie', 'stage3_get_stores']
    # extractor_tags = ['extractor_list', 'extractor_locationcookie', 'extractor_stores']
    # stages_list = ['stage1_get_list', 'stage3_get_stores']
    
    # extractor_tags = [] ## These refer to extractors named in the config file / extractors dict
    stage_sequence = [] ## The names of inner ProcessStage subclasses representing process steps, in execution order
    
    ## Leave blank
    extractors = {}
    
    def __init__(self, extractors):
        self.extractors = extractors;
    
    def check_stages(self, stageinfo=[]):
        """
        Extractors is a dict of the extractors relevant to this Handler as prescribed in the config
        stageinfo is a list of the stages as obtained from the status file
        return value is the updated stageinfo
        [
        {status: 'STARTED', stage_ident: ..., extractor_tag: ..., when_started: ..., when_finished: ..., status_message: ..., progress_fraction: ...}
        ]
        
        FINISHED and COMPLETE as different statuses?
        STARTED for extractors and PROCESSING for other things
        """
        
        """
        Stage transitions:
        When a stage's work is done it returns a status of "FINISHED" from status()
        Then, the handler will call its .finish() method before invoking the next stage.
        So finish() is for AFTER the process has finished to do any transformations and post-checking. 
        If everything is OK then "status_ok": True must be set in its return val to indicate that
        we can proceed to next stage.
        
        Status() gives a status report of the current state of the ProcessStage but each of begin(), during() and finish() do also.
        All of these feed into the persistent state structure.
        You can return whatever you like from status(), the UI understands these:
        .....
        
        """
        
        ## Look at most recent stage, if status is not FINISHED then query the extractor
        stageinfo_dict = dict([(d['stage_class'], d) for d in stageinfo])
        if stageinfo:
            mystage = stageinfo[-1]
            stageidx = self.stage_sequence.index(mystage['stage_class'])

            # extractor_tag = mystage.get('extractor_tag', None)
            
            stage_classname = mystage.get('stage_class', None)
            if stage_classname:
                stage_prev_status = mystage['status']
                stage_class = getattr(self, stage_classname, None)
                # sys.stderr.write(str(type(stage_prev_status))+'\n')
                stageobj = stage_class(extractors=self.extractors, status_info=mystage, all_stages=stageinfo_dict)
                
                mystage.update(stageobj.status())
                new_status = mystage['status']
                if stage_prev_status != 'FINISHED' and new_status == 'FINISHED':
                    poststage_status = stageobj.finish(*[], **{})
                    if poststage_status['status_ok']:
                        stageidx += 1
                    mystage.update(poststage_status)
                        
                elif new_status == 'STARTED':
                    # sys.stderr.write("EXTRACTOR is running")
                    duringstage_status = stageobj.during(*[], **{})
                    mystage.update(duringstage_status)

                elif new_status == 'PROCESSING':
                    util.log("PROCESSOR is running")
                    duringstage_status = stageobj.during(*[], **{})
                    mystage.update(duringstage_status)
                
                elif stage_prev_status == 'FINISHED' and new_status == 'FINISHED':
                    stageidx += 1
                
                else:
                    pass
                    # sys.stderr.write("EXTRACTOR STAGE IS '{}'".format(new_status))
            

            # ## Not an extractor-based stage
            # else:
            #     if mystage['status'] == 'FINISHED':
            #         poststage_fn = getattr(self, "stage_post__{}".format(self.stages_list[stageidx]))
            #         poststage_status = poststage_fn(*[], **{})
            #         if poststage_status['status_ok']:
            #             mystage['status'] = 'FINISHED'
            #             stageidx += 1
            #         mystage.update(poststage_status)
            #     else:
            #         duringstage_fn = getattr(self, "stage_during__{}".format(self.stages_list[stageidx]), None)
            #         if duringstage_fn:
            #             duringstage_status = duringstage_fn(*[], **{})
            #             mystage.update(duringstage_status)
            #
                
        
        ## If stageinfo is empty, we're initializing
        else:
            stageidx = 0

        ## If stage finished or there's no stage data, run post-stage, taking into account any recent transforms
        
        too_many_stages = lambda: len(stageinfo) >= len(self.stage_sequence)
        if len(stageinfo) == 0 or mystage['status'] == 'FINISHED':
            if len(self.stage_sequence) > stageidx and not too_many_stages():
                newstage_class = getattr(self, self.stage_sequence[stageidx])
                prevstage = len(stageinfo) and stageinfo[-1] or None
                newstage_obj = newstage_class(extractors=self.extractors, all_stages=stageinfo_dict)
                stageinfo.append(newstage_obj.begin(previous_stage=prevstage, *[], **{}))
            else:
                pass
                # sys.stderr.write('PROCESS CONCLUDED\n')
                
        return stageinfo


class ProcessStage(object):
    """
    Represents a stage of a process
    """
    extractor_tag = None
    extractor_ident = None
    extractor = None
    message_begin = ''
    message_during = ''
    message_finish = ''
    status_info = {}
    all_stages = {}
    
    def __init__(self, extractors={}, status_info={}, all_stages={}):
        if self.extractor_tag:
            self.extractor = extractors.get(self.extractor_tag)
        elif self.extractor_ident:
            self.extractor = tractor.ImportioExtractor(self.extractor_ident)
        self.status_info = status_info
        self.all_stages = all_stages
    
    def runs_get_latest(self):
        runs = self.extractor.runs_get_raw()
        return runs[0]
    
    def status(self):
        if self.extractor:
            run = self.runs_get_latest()
            f = lambda k, d=None: run['fields'].get(k, d)
            d = dict(
                count_total = int(f('totalUrlCount', 0)),
                count_done = int(f('successUrlCount', 0)) + int(f('failedUrlCount', 0)),
                when_started = int(f('startedAt', 0)),
                when_stopped = int(f('stoppedAt', 0)),
                extractor_state = f('state'),
                status = f('state')
            )
            if d['count_total'] and d['count_done']:
                d['progress_fraction'] = float(d['count_done']) / float(d['count_total'])
            return d
        else:
            return dict(
                
            )
        
        # stageidx = self.stages_list.index(mystage['stage_ident'])
        # extractorstate = latestrun['fields']['state']
        # try:
        #     lrf = latestrun['fields']
        #     mystage.update(dict(
        #         count_total = int(lrf['totalUrlCount']),
        #         count_done = int(lrf['successUrlCount']) + int(lrf['failedUrlCount']),
        #         when_started = int(lrf.get('startedAt', 0)),
        #         when_stopped = int(lrf.get('stoppedAt', 0)),
        #     ))
        # except:
        #     pass
        
    
    def prep(self, previous_stage={}, *args, **kwargs):
        """Perform any necessary preparatory tasks so that the stage can run.
        This helps us keep begin() clean where possible."""
        return True
    
    def begin(self, previous_stage={}, *args, **kwargs):
        return dict(
            # stage_class = self.__class__.__name__,
            # extractor_tag = self.extractor_tag,
            # status_message = "XYZZY extraction running",
            # progress_fraction = 0.0,
            # status = 'STARTED'
        )
    
    def during(self, *args, **kwargs):
        return dict()
    
    def finish(self, *args, **kwargs):
        return dict(
            # status_ok = True,
            # status_message = "Store list extracted",
            # status = 'FINISHED',
            # output_written_to = <OUTFNAME>
        )
    

class ExtractorProcessStage(ProcessStage):
    """Runs the extractor identified by extractor_tag (or any extractor in self.extractor), then saves the CSV output to a tempfile
    """
    
    extractor_tag = None
    message_begin = "Extractor starting"
    message_during = "Extractor running"
    message_finish = "Extractor finished"
    
    def begin(self, previous_stage={}, *args, **kwargs):
        if not self.prep(previous_stage, *args, **kwargs):
            raise("Unable to prep extractor")
        if not self.extractor:
            raise("No extractor provided for extractor stage")
        # sys.stderr.write('STAGE I STARTING\n')
        self.extractor.start()
        return dict(
            #stage_ident = self.__class__.__name__,
            stage_class = self.__class__.__name__,
            extractor_tag = self.extractor_tag,
            status_message = self.message_begin,
            progress_fraction = 0.0,
            status = 'STARTED'
        )

    def during(self, *args, **kwargs):
        # sys.stderr.write('PROCESS RUNNING\n')
        return dict(
            status_message = self.message_during
        )

    def finish(self, *args, **kwargs):
        ## Get URLs from list extractor and inject them into store extractor
        ## (later we will incorporate the location cookie)
        outfname = util.save_csv(self.extractor)
        
        # sys.stderr.write('POST STAGE I EXECUTED\n')
        return dict(
            status_ok = True,
            status_message = self.message_finish,
            status = 'FINISHED',
            output_written_to = outfname
        )
            

class CSVGenerateStage(ProcessStage):
    input_stage_tag = None
    input_stage_filename_field = 'output_written_to'
    columns_out = []
    message_begin = "Processing data"
    message_during = "Processing data"
    message_finish = "Data processed"
    
    def map_row(self, row_in, rider_in={}):
        pass
    
    def begin(self, previous_stage={}, *args, **kwargs):
        input_stage = self.all_stages[self.input_stage_tag]
        in_fname = input_stage[self.input_stage_filename_field]
        prevdata = csv.DictReader(open(in_fname))
        
        timestamp_start = util.timestamp_now()
        
        def write_out(fout):
            dw = csv.DictWriter(fout, self.columns_out)
            dw.writeheader()
            for r in prevdata:
                rider = util.url_get_rider(r['url'])
                rout = self.map_row(r, rider)
                try:
                    dw.writerow(rout)
                except:
                    util.log(u"Failed for row: {}".format(rout))
                    # raise
    
        return dict(
            stage_class = self.__class__.__name__,
            status_ok = True,
            status_message = self.message_finish,
            status = 'FINISHED',
            output_written_to = util.with_temp_file(write_out), 
            when_started = timestamp_start,
            when_stopped = util.timestamp_now()
        )
    
    
