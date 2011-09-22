"""
    prickle.templates
    ~~~~~~~~~~~~~~~~~
    
    i load templates by template name, and hold the TemplateRunner
    
"""
import os.path
import imp
from twisted.internet.task import LoopingCall
from twisted.internet.threads import deferToThreadPool
from twisted.internet.defer import DeferredList
from twisted.python import log
from twisted.internet import reactor
import logging
import time
import operator


templates_dict = {}
templates = []

def _make_aliases(template):
    aliases = getattr(template, 'aliases', range(template.numGraphs))
    
    template.aliases = dict(enumerate(aliases))
    template.aliases_reversed = dict((v, k) for (k, v) in enumerate(aliases))
    

def load_template(name):
    """ Attempts to load a template by name """
    if name in templates_dict:
        return templates_dict[name]
        
    else:
        fp, pathname, description = imp.find_module(name, __path__)
        try:
            template = templates_dict[name] = imp.load_module(name, fp, pathname, description).template
            template.template = name
            templates.append(template)
            templates.sort(key=operator.attrgetter('template'))
            _make_aliases(template)
            
            return template
        
        finally:
            fp.close()
            
def template_exists(name):
    """ Checks if a template exists """
    try:
        load_template(name)
        return True
    except ImportError, AttributeError:
        raise
        return False
        

class TemplateRunner(object):
    """ The template runner, I manage all of the templates """
    
    def __init__(self, stats):
        self.stats = stats
        self.loopingCalls = []
        self.scheduledPeriods = set(stats.config['graph_draw_frequency'].keys()) # we know what we have scheduled, and we know what default needs.
        self.scheduledPeriods.discard('default')
    
    def create_databases(self, overwrite = False):
        for graph in self.stats.config['graphs']:
            cls = load_template(graph['template'])
            Cls = cls(self.make_filename(graph['id']), id = graph['id'], config = graph['config'], factory = self)
            Cls._create(overwrite)
        
    def make_filename(self, id):
        """ Convenience function that creates the filename / path for an rrd database """
        return os.path.join(self.stats.config['database_path'], '%s.rrd' % id)
        
    def run(self):
        """ Start each of the configured templates up. """
        for graph in self.stats.config['graphs']:
            cls = load_template(graph['template'])
            Cls = cls(self.make_filename(graph['id']), id = graph['id'], config = graph['config'], factory = self)
            Cls._defaultIntervalDraws = list(set(Cls.config['periods']) - self.scheduledPeriods)
            self.stats.active_graphs[graph['id']] = Cls
            Cls.run()
        
        reactor.callWhenRunning(self.start_graphing_loop)
        
    def start_graphing_loop(self):
        """Schedule all the looping calls for the graphs!"""
        from collections import defaultdict
        graph_draw_frequency = self.stats.config['graph_draw_frequency']
        
        
        # Instead of scheduling many LoopingCalls if the interval is the same, we group loopingcalls by intervals.
        i_group = defaultdict(list)
        
        for period, interval in graph_draw_frequency.iteritems():
            i_group[interval].append(period)
            
        for interval, periods in i_group.iteritems():
            lc = LoopingCall(self.render_graphs, periods)
            self.loopingCalls.append(lc)
            log.msg("Starting graphing_loop for periods=%r, interval=%i" % (periods, interval), logLevel = logging.INFO)
            lc.start(interval)
        
        log.msg("Started %i LoopingCalls to graph with." % len(self.loopingCalls), logLevel = logging.INFO)
            
    def stop(self):
        """ Stop the template runner """
        for lc in self.loopingCalls:
            if lc.running:
                lc.stop()
                
        self.loopingCalls = []
            
        for template in self.stats.active_graphs.itervalues():
            template.stop()
            
        self.active_graphs.clear()
        
    
    def _do_graph_render(self, callback, periods):
        """ Iternal method to render graphs """
        for period in periods:
            try:
                callback(period)
            except:
                log.msg("Error generating graphs, period=%s" % period, logLevel = logging.ERROR)
                log.err()
            
        
    
    
    def render_graphs(self, periods):
        """
            Render all the graphs that each template provides, work happens
            in multiple threads. Returns a deferred that fires when graphs have
            been successfully rendered.
        """
        
        threadPool = self.stats.rrd_threadpool
        # Determine which templates need to run.
        jobQueue = []
        for template in self.stats.active_graphs.itervalues():
            t_periodsToRun = []
            t_periods = template.config['periods']
            
            for period in periods:
                # If this is the default interval, tell it to run all unscheduled graphs
                if period == 'default':
                    t_periodsToRun += template._defaultIntervalDraws
                # If this period is a period that this template draws...
                elif period in t_periods:
                    t_periodsToRun.append(period)
            # Finally, if this template has any periods to run, we run it.
            if t_periodsToRun:
                jobQueue.append(
                    (template, t_periodsToRun)
                )
        
        # We have nothing to do...
        if not jobQueue:
            return
        log.msg('Generating graphs for periods=%r, jobQueueLength=%i' % (periods, len(jobQueue)), logLevel = logging.INFO)
        t = time.time()
        
        def workDone(res):
            log.msg('Generated graphs for periods=%r in  %.3f seconds.' % (periods, time.time() - t), logLevel = logging.INFO)
            return res
        
        d = DeferredList([
            deferToThreadPool(reactor, threadPool, self._do_graph_render, template._graph, t_periodsToRun) for template, t_periodsToRun in jobQueue
        ], consumeErrors = False)
        
        
        d.addCallback(workDone)
        d.addErrback(workDone)
        return d

            
    
    
__all__ = ['TemplateRunner', 'load_template', 'template_exists', 'templates', 'templates_dict']