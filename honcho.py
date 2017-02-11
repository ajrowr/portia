
import sys, tempfile
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
    
    extractor_tags = [] ## These refer to extractors named in the config file / extractors dict
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
        ## Look at most recent stage, if status is not FINISHED then query the extractor
        if stageinfo:
            mystage = stageinfo[-1]
            stageidx = self.stage_sequence.index(mystage['stage_class'])

            # extractor_tag = mystage.get('extractor_tag', None)
            
            stage_classname = mystage.get('stage_class', None)
            if stage_classname:
                stage_prev_status = mystage['status']
                stage_class = getattr(self, stage_classname, None)
                stageobj = stage_class(extractors=self.extractors)
                
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
                    sys.stderr.write("PROCESSOR is running")
                    duringstage_status = stageobj.during(*[], **{})
                    mystage.update(duringstage_status)
                
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
                newstage_obj = newstage_class(extractors=self.extractors)
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
    extractor = None
    message_begin = ''
    message_during = ''
    message_finish = ''
    
    def __init__(self, extractors={}):
        if self.extractor_tag:
            self.extractor = extractors.get(self.extractor_tag)
    
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
            
